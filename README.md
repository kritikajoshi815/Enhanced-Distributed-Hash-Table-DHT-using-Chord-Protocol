# Enhanced Chord Distributed Hash Table (DHT) with Replication

This project implements an enhanced version of the Chord Distributed Hash Table (DHT), a peer-to-peer system that provides efficient and decentralized key-value storage. The enhancement includes data replication for fault tolerance and improved data availability.

PPT Link: https://docs.google.com/presentation/d/1950seXZlZKuwib9M8b18XL5bxA7AO9hppXKI083a0lw/edit?usp=sharing

## Features

### Core Chord Functionality

- **Chord Protocol Implementation**: Uses the standard Chord ring structure for decentralized key lookup.
- **gRPC Communication**: Nodes communicate using high-performance gRPC remote procedure calls.
- **Finger Table**: Implements the finger table for efficient \( O(\log N) \) routing.
- **Stabilization & Maintenance**: Background threads manage node stabilization and finger table updates.

### Enhanced Fault Tolerance

- **Data Replication**: Key-value pairs are replicated across multiple successor nodes (default replication factor = 3).
- **Fault Tolerance**: Automatic successor failure detection and promotion of replicas to primary data when needed.

---

## Setup and Installation

### Prerequisites

Ensure the following are installed:

- Python 3.x
- pip (Python package installer)

### Install Dependencies

This project uses gRPC for inter-node communication.

```bash
pip install grpcio grpcio-tools
```

### Compile Protocol Buffers

The `.proto` file defines the service and messages. Compile it before running the project:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. chord.proto
```

---

## Running the Demo

The `run_demo.sh` script demonstrates the system by launching nodes, storing data, simulating failures, and verifying replication.

### 1. Make the Script Executable

```bash
chmod +x run_demo.sh
./run_demo.sh
```

### 2. Demo Flow

The script performs the following steps:

1. Starts 4 nodes (ports 5001–5004), all joining the ring through node 5001.
2. Waits for ring stabilization.
3. Stores several key-value pairs (e.g., `user:1001 -> Alice`).
4. Retrieves data through different nodes to demonstrate routing.
5. Terminates node 5002 to simulate a failure.
6. Verifies that the data stored on the failed node remains accessible (replication working correctly).

---

## Manual Operation and Testing

### 1. Start the First Node (Ring Anchor)

```bash
python3 chord_node.py 5001
```

### 2. Join Additional Nodes

```bash
python3 chord_node.py 5002 --join localhost:5001
python3 chord_node.py 5003 --join localhost:5001
```

### 3. Interact Using the Client

Use `chord_client.py` to send operations to any node.

| Command | Description              | Example                                               |
| ------- | ------------------------ | ----------------------------------------------------- |
| put     | Store a key-value pair   | `python3 chord_client.py localhost:5001 put name Bob` |
| get     | Retrieve a value         | `python3 chord_client.py localhost:5002 get name`     |
| delete  | Delete a key             | `python3 chord_client.py localhost:5003 delete name`  |
| find    | Find successor for an ID | `python3 chord_client.py localhost:5001 find 12345`   |
| stats   | Get node statistics      | `python3 chord_client.py localhost:5004 stats`        |

---

## File Structure

- **chord_node.py**: Core logic of a Chord node, including gRPC service methods and background stabilization tasks.
- **chord_client.py**: CLI tool to interact with the Chord ring (put, get, delete, find, stats).
- **chord.proto**: Protocol Buffers definition for messages and RPC services.
- **run_demo.sh**: Demonstrates the system's core behavior, including replication and fault tolerance.
- **run_report_tests.sh**: Stress-test script for launching many nodes and collecting performance metrics.
