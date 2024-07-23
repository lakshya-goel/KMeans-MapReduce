Here's the markdown file for the README:

```markdown
# K-Means Clustering using MapReduce

This project implements the K-Means clustering algorithm using a MapReduce framework from scratch. The implementation is designed to run on a single machine with multiple processes simulating a distributed environment.

## Overview

The system consists of three main components:
1. Master
2. Mappers
3. Reducers

These components communicate using gRPC, with each mapper and reducer running as a separate process.

## Features

- Distributed K-Means clustering
- MapReduce framework implementation
- Fault tolerance for mapper and reducer failures
- Configurable number of mappers, reducers, centroids, and iterations
- Input data partitioning across mappers
- Intermediate data persistence
- Final output compilation

## Requirements

- Python 3.x
- gRPC

## Setup

1. Install dependencies:
   ```
   pip install grpcio grpcio-tools
   ```

2. Generate gRPC code:
   ```
   python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/kmeans.proto
   ```

## Usage

1. Start the master process:
   ```
   python master.py --mappers 3 --reducers 2 --centroids 5 --iterations 10
   ```

2. The master will automatically spawn mapper and reducer processes as needed.

## Directory Structure

```
.
├── Data/
│   ├── Input/
│   │   └── points.txt
│   ├── Mappers/
│   │   ├── M1/
│   │   ├── M2/
│   │   └── M3/
│   ├── Reducers/
│   │   ├── R1.txt
│   │   └── R2.txt
│   └── centroids.txt
├── master.py
├── mapper.py
├── reducer.py
├── protos/
│   └── kmeans.proto
└── README.md
```

## Implementation Details

- The master program coordinates the entire process and handles fault tolerance.
- Mappers read input data, perform the mapping step, and partition the output.
- Reducers shuffle and sort the intermediate data, then perform the reduce step to update centroids.
- All inter-process communication is done via gRPC.
- Intermediate and final results are persisted to the local file system.

## Logging

All important events and data are logged to `dump.txt` for debugging and monitoring purposes.

## Testing

The system can be tested with different numbers of mappers, reducers, centroids, and iterations. Fault tolerance can be tested by manually terminating mapper or reducer processes during execution.

## Note

This implementation is for educational purposes and simulates a distributed environment on a single machine. In a real-world scenario, the components would be deployed on separate machines in a cluster.
```
