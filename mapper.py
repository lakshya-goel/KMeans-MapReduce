import os
import grpc
import sys
from concurrent import futures
import common_pb2
import common_pb2_grpc
from typing import List, Tuple
from datetime import datetime


class Mapper(common_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id: int, num_centroids: int, num_reducers: int):
        self.mapper_id = mapper_id
        self.num_centroids = num_centroids
        self.num_reducers = num_reducers
        self.centroids = []
        self.dump_file = open(f"Data/Mappers/M{self.mapper_id}/dump.txt", "w")
        self.dump_file.close()

    def create_directories(self):
        if not os.path.exists("Data"):
            os.makedirs("Data")
        if not os.path.exists(f"Data/Mappers/M{self.mapper_id}"):
            os.makedirs(f"Data/Mappers/M{self.mapper_id}")

    def RunMap(self, request: common_pb2.MasterMapperRequest, context):
        temp = self.mapper_id
        self.mapper_id = request.id

        self.create_directories()
        # self.dump_file = open(f"Data/Mappers/M{self.mapper_id}/dump.txt", "w")
        # self.dump_file.close()

        self.log(f"Mapper {self.mapper_id} started")
        data_points = self.read_input_data(request.range)
        self.centroids = request.centroids
        self.write_partitions(data_points)
        self.log(f"Mapper {self.mapper_id} finished")
        self.mapper_id = temp
        return common_pb2.MasterMapperResponse(status="SUCCESS")

    def read_input_data(self, range: common_pb2.Range) -> List[common_pb2.Point]:
        data_points = []
        with open("Data/Input/points.txt", "r") as f:
            for i, line in enumerate(f):
                if range.start <= i < range.end:
                    x, y = map(float, line.strip().split(","))
                    data_points.append(common_pb2.Point(x=x, y=y))
        return data_points

    def write_partitions(self, data_points: List[common_pb2.Point]):
        self.partitions = [[] for _ in range(self.num_reducers)]
        for point in data_points:
            nearest_centroid_id = self.find_nearest_centroid(point)
            self.partitions[nearest_centroid_id % self.num_reducers].append(common_pb2.Int_Data(key=nearest_centroid_id, val=point))
        for partition_id, partition in enumerate(self.partitions):
            with open(f"Data/Mappers/M{self.mapper_id}/partition_{partition_id}.txt", "w") as f:
                for data_point in partition:
                    f.write(f"{data_point.key},{data_point.val.x},{data_point.val.y}\n")

    def find_nearest_centroid(self, point: common_pb2.Point) -> int:
        min_distance = float("inf")
        nearest_centroid_id = 0
        for i, centroid in enumerate(self.centroids):
            distance = self.euclidean_distance(point, centroid)
            if distance < min_distance:
                min_distance = distance
                nearest_centroid_id = i
        return nearest_centroid_id

    def euclidean_distance(self, p1: common_pb2.Point, p2: common_pb2.Centroid) -> float:
        return ((p1.x - p2.x) ** 2 + (p1.y - p2.y) ** 2) ** 0.5
    
    def GetData(self, request: common_pb2.ReducerMapperRequest, context):
        self.log(f"Mapper {request.mapper_id} GetData request from reducer {request.reducer_id}")
        data = []
        with open(f"Data/Mappers/M{request.mapper_id}/partition_{request.reducer_id}.txt", "r") as f:
            for line in f.readlines():
                key, x, y = line.strip().split(",")
                point = common_pb2.Point(x=float(x), y=float(y))
                data.append(common_pb2.Int_Data(key=int(key), val=point))
                    
        return common_pb2.ReducerMapperResponse(data=data)

    def log(self, message: str):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        print(message)
        with open(f"Data/Mappers/M{self.mapper_id}/dump.txt", "a") as f:
            f.write(f"{timestamp} {message}\n")

def serve(id, n_mappers, n_reducers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper = Mapper(id, n_mappers, n_reducers)
    common_pb2_grpc.add_MapperServicer_to_server(mapper, server)
    server.add_insecure_port(f"localhost:{8000 + int(id)}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Insufficient arguments")
    else:
        serve(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))

# python mapper.py id num_mappers num_reducers