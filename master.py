import os
import random
import grpc
import common_pb2
import common_pb2_grpc
import sys
from typing import List, Tuple
from time import sleep
from datetime import datetime


class Master:
    def __init__(self, num_mappers: int, num_reducers: int, num_centroids: int, num_iterations: int):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_centroids = num_centroids
        self.num_iterations = num_iterations
        self.map_indices = {}
        self.centroids = []
        self.create_directories()
        self.dump_file = open("Data/dump.txt", "w")

    def create_directories(self):
        if not os.path.exists("Data"):
            os.makedirs("Data")

    def initialize_centroids(self):
        with open("Data/Input/points.txt", "r") as f:
            data_points = [common_pb2.Centroid(x=float(x), y=float(y)) for x, y in [line.strip().split(",") for line in f]]

        points_per_mapper = len(data_points) // self.num_mappers
        remainder = len(data_points) % self.num_mappers

        start_index = 0
        for mapper_id in range(self.num_mappers):
            points_to_assign = points_per_mapper + (1 if remainder > 0 else 0)
            remainder -= 1
            end_index = start_index + points_to_assign
            self.map_indices[mapper_id] = common_pb2.Range(start=start_index, end=end_index)
            start_index = end_index

        self.centroids =  random.sample(data_points,self.num_centroids)
        self.log(f"Initialized Centroids: {self.centroids}")

    def run_iteration(self, iteration: int):
        self.log(f"Iteration {iteration}")

        # Invoke mappers
        self.invoke_mappers()

        # Invoke reducers
        return self.invoke_reducers()

    def invoke_mappers(self):
        failed = []
        for mapper_id in range(self.num_mappers):
            self.log(f"Invoking Mapper {mapper_id}")
            with grpc.insecure_channel(f"localhost:{8000 + mapper_id}") as channel:
                stub = common_pb2_grpc.MapperStub(channel)
                try:
                    response = stub.RunMap(common_pb2.MasterMapperRequest(
                        range=self.map_indices[mapper_id],
                        centroids=self.centroids,
                        id = mapper_id
                    ), timeout=3)
                    self.log(f"Mapper {mapper_id} response: {response.status}")
                except grpc.RpcError:
                    self.log(f"Mapper {mapper_id} failed to respond")
                    failed.append(mapper_id)

        if(len(failed)==self.num_mappers):
            self.log("ERROR: All mappers down. Exiting code.")
            sys.exit()

        working = [i for i in range(self.num_mappers) if i not in failed]
        i,j = 0,0
        while(i<len(failed)):
            self.log(f"Invoking Mapper {working[j]} in place of Mapper {failed[i]}")
            with grpc.insecure_channel(f"localhost:{8000 + working[j]}") as channel:
                stub = common_pb2_grpc.MapperStub(channel)
                response = stub.RunMap(common_pb2.MasterMapperRequest(
                    range=self.map_indices[failed[i]],
                    centroids=self.centroids,
                    id = failed[i]
                ))
                self.log(f"Mapper {working[j]} response: {response.status}")
            i += 1
            j += 1

            if(j>=len(working)):
                j = j%len(working)

    def invoke_reducers(self):
        failed = []
        for reducer_id in range(self.num_reducers):
            self.log(f"Invoking Reducer {reducer_id}")
            with grpc.insecure_channel(f"localhost:{9000 + reducer_id}") as channel:
                stub = common_pb2_grpc.ReducerStub(channel)
                try:
                    response = stub.RunReduce(common_pb2.MasterReducerRequest(
                        num_centroids=self.num_centroids,
                        id = reducer_id
                    ), timeout=3)
                    self.log(f"Reducer {reducer_id} response: {response.status}")
                except grpc.RpcError:
                    self.log(f"Reducer {reducer_id} failed to respond")
                    failed.append(reducer_id)

        if(len(failed)==self.num_reducers):
            self.log("ERROR: All reducers down. Exiting code.")
            sys.exit()

        working = [i for i in range(self.num_reducers) if i not in failed]
        i,j = 0,0
        while(i<len(failed)):
            self.log(f"Invoking Reducer {working[j]} in place of Reducer {failed[i]}")
            with grpc.insecure_channel(f"localhost:{9000 + working[j]}") as channel:
                stub = common_pb2_grpc.ReducerStub(channel)
                response = stub.RunReduce(common_pb2.MasterReducerRequest(
                        num_centroids=self.num_centroids,
                        id = failed[i]
                ))
                self.log(f"Reducer {working[j]} response: {response.status}")

            i += 1
            j = (j+1)%len(working)

        new_centroids = []
        for reducer_id in working:
            with grpc.insecure_channel(f"localhost:{9000 + reducer_id}") as channel:
                stub = common_pb2_grpc.ReducerStub(channel)
                response = stub.GetCentroids(common_pb2.CentroidRequest(id = reducer_id))
                new_centroids.extend(response.centroids)

        j = 0
        for reducer_id in failed:
            with grpc.insecure_channel(f"localhost:{9000 + working[j]}") as channel:
                stub = common_pb2_grpc.ReducerStub(channel)
                response = stub.GetCentroids(common_pb2.CentroidRequest(id = reducer_id))
                new_centroids.extend(response.centroids)

            j = (j+1) % len(working)
        
        self.log("")
        self.log(f"New Centroids: {new_centroids}")
        with open("Data/centroids.txt", "w") as f:
            for centroid in new_centroids:
                f.write(f"{centroid.x},{centroid.y}\n")

        return new_centroids

    def log(self, message: str):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        print(message)
        self.dump_file.write(f"{timestamp} {message}\n")

    def check_convergence(self, old_centroids, new_centroids):
        threshold = 10**(-5)
        for i in range(self.num_centroids):
            if(abs(old_centroids[i].x - new_centroids[i].x)>threshold or abs(old_centroids[i].y - new_centroids[i].y)>threshold):
                return False
        
        self.log("Means converged")
        return True

    def run(self):
        self.initialize_centroids()
        for i in range(self.num_iterations):
            updated_centroids = self.run_iteration(i)
            self.log("")

            if(self.check_convergence(self.centroids, updated_centroids)):
                break
            # Update centroids for the next iteration
            self.centroids = updated_centroids
            sleep(5)
            
        self.dump_file.close()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Insufficient arguments")
    else:
        master = Master(
            num_mappers=int(sys.argv[1]),
            num_reducers=int(sys.argv[2]),
            num_centroids=int(sys.argv[3]),
            num_iterations=int(sys.argv[4])
        )
        master.run()

# python master.py num_mappers num_reducers num_centroids num_iterations