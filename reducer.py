import os
import grpc
import sys
from concurrent import futures
import common_pb2
import common_pb2_grpc
from collections import defaultdict
from datetime import datetime


class Reducer(common_pb2_grpc.ReducerServicer):
    def __init__(self, reducer_id: int, num_mappers: int):
        self.reducer_id = reducer_id
        self.num_mappers = num_mappers
        self.num_centroids = 0
        self.dump_file = open(f"Data/Reducers/R{self.reducer_id}/dump.txt", "w")
        self.dump_file.close()

    def create_directories(self):
        if not os.path.exists("Data"):
            os.makedirs("Data")
        if not os.path.exists(f"Data/Reducers/R{self.reducer_id}"):
            os.makedirs(f"Data/Reducers/R{self.reducer_id}")

    def RunReduce(self, request: common_pb2.MasterReducerRequest, context):
        temp = self.reducer_id
        self.reducer_id = request.id

        self.create_directories()
        # self.dump_file = open(f"Data/Reducers/R{self.reducer_id}/dump.txt", "w")
        # self.dump_file.close()


        self.log(f"Reducer {self.reducer_id} started")
        self.data_points = defaultdict(list)
        self.num_centroids = request.num_centroids
        self.read_partitions()
        self.update_centroids()
        self.write_centroids()
        self.log(f"Reducer {self.reducer_id} finished")
        self.reducer_id = temp
        return common_pb2.MasterReducerResponse(status="SUCCESS")

    def read_partitions(self):
        failed = []
        for mapper_id in range(self.num_mappers):
            self.log(f"Requesting data from Mapper {mapper_id}")
            with grpc.insecure_channel(f"localhost:{8000 + mapper_id}") as channel:
                stub = common_pb2_grpc.MapperStub(channel)
                try:
                    response = stub.GetData(common_pb2.ReducerMapperRequest(reducer_id=self.reducer_id, mapper_id = mapper_id), timeout=3)
                    for data_point in response.data:
                        self.data_points[data_point.key].append(data_point.val)

                except grpc.RpcError:
                    self.log(f"Mapper {mapper_id} failed to respond")
                    failed.append(mapper_id)

        if(len(failed)==self.num_mappers):
            self.log("ERROR: All mappers down. Exiting code.")
            sys.exit()

        working = [i for i in range(self.num_mappers) if i not in failed]

        j = 0
        for mapper_id in failed:
            self.log(f"Requesting data of Mapper {mapper_id} from Mapper {working[j]}")
            with grpc.insecure_channel(f"localhost:{8000 + working[j]}") as channel:
                stub = common_pb2_grpc.MapperStub(channel)
                response = stub.GetData(common_pb2.ReducerMapperRequest(reducer_id=self.reducer_id, mapper_id = mapper_id))
                for data_point in response.data:
                    self.data_points[data_point.key].append(data_point.val)
                
            j += 1
            j = j % len(working)

    def update_centroids(self):
        self.new_centroids = {}
        for key, points in self.data_points.items():
            x_sum = sum(point.x for point in points)
            y_sum = sum(point.y for point in points)

            val = common_pb2.Centroid(x = x_sum/len(points), y = y_sum/len(points))
            self.new_centroids[key] = val

    def write_centroids(self):
        with open(f"Data/Reducers/R{self.reducer_id}.txt", "w") as f:
            for key,val in self.new_centroids.items():
                f.write(f"{key},{val.x},{val.y}\n")

    def GetCentroids(self, request: common_pb2.CentroidRequest, context):
        self.log(f"Reducer {request.id} GetCentroids request from master")
        data = []
        with open(f"Data/Reducers/R{request.id}.txt", "r") as f:
            for line in f.readlines():
                _, x, y = line.strip().split(",")
                data.append(common_pb2.Centroid(x=float(x), y=float(y)))

        return common_pb2.CentroidResponse(centroids=data)

    def log(self, message: str):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        print(message)
        with open(f"Data/Reducers/R{self.reducer_id}/dump.txt", "a") as f:
            f.write(f"{timestamp} {message}\n")

def serve(id, num_mappers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer = Reducer(id, num_mappers)
    common_pb2_grpc.add_ReducerServicer_to_server(reducer, server)
    server.add_insecure_port(f"localhost:{9000 + int(id)}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Insufficient arguments")
    else:
        serve(int(sys.argv[1]), int(sys.argv[2]))

# python reducer.py id num_mappers