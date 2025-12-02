#!/usr/bin/env python3
import grpc
import sys
import argparse
import chord_pb2
import chord_pb2_grpc


class ChordClient:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.channel = grpc.insecure_channel(f"{address}:{port}")
        self.stub = chord_pb2_grpc.ChordServiceStub(self.channel)
    
    def put(self, key, value):
        """Store a key-value pair"""
        try:
            request = chord_pb2.PutRequest(
                key=key,
                value=value,
                is_replica=False
            )
            
            response = self.stub.Put(request, timeout=20)
            
            if response.success:
                print(f"PUT '{key}' = '{value}' : {response.message} (version: {response.version})")
                return True
            else:
                print(f"PUT failed: {response.message}")
                return False
        
        except grpc.RpcError as e:
            print(f"PUT error: {e.code()} - {e.details()}")
            return False
    
    def get(self, key):
        """Retrieve a value by key"""
        try:
            request = chord_pb2.GetRequest(key=key)
            response = self.stub.Get(request, timeout=20)
            
            if response.found:
                print(f"GET '{key}' = '{response.value}' (version: {response.version})")
                return response.value
            else:
                print(f"GET '{key}' : NOT FOUND")
                return None
        
        except grpc.RpcError as e:
            print(f"GET error: {e.code()} - {e.details()}")
            return None
    
    def delete(self, key):
        """Delete a key"""
        try:
            request = chord_pb2.DeleteRequest(key=key, is_replica=False)
            response = self.stub.Delete(request, timeout=20)
            
            if response.success:
                print(f" DELETE '{key}' : {response.message}")
                return True
            else:
                print(f"DELETE '{key}' : {response.message}")
                return False
        
        except grpc.RpcError as e:
            print(f" DELETE error: {e.code()} - {e.details()}")
            return False
    
    def find_successor(self, key_id):
        """Find the successor of a given ID"""
        try:
            request = chord_pb2.FindSuccessorRequest(key_id=key_id)
            response = self.stub.FindSuccessor(request, timeout=20)
            
            node = response.node
            print(f"\n=== Find Successor of {key_id} ===")
            print(f"Successor: Node {node.id} at {node.address}:{node.port}")
            print(f"Hops: {response.hops}")
            
            if response.path:
                path_str = " → ".join(str(n) for n in response.path)
                print(f"Path: {path_str} → {node.id}")
            
            return node
        
        except grpc.RpcError as e:
            print(f" FindSuccessor error: {e.code()} - {e.details()}")
            return None
    
    def get_stats(self):
        """Get node statistics"""
        try:
            request = chord_pb2.GetStatsRequest()
            response = self.stub.GetStats(request, timeout=20)
            
            print(f"\n=== Node Statistics ===")
            print(f"Node ID: {response.node_id}")
            print(f"Primary Keys: {response.primary_keys}")
            print(f"Replica Keys: {response.replica_keys}")
            print(f"Replication Factor: {response.replication_factor}")
            print(f"Alive Successors: {response.alive_successors}")
            print(f"Total Lookups: {response.lookups}")
            
            if response.lookups > 0:
                print(f"Average Hops: {response.avg_hops:.2f}")
            
            print(f"Status: {response.status}")
            print(f"======================\n")
            
            return response
        
        except grpc.RpcError as e:
            print(f" GetStats error: {e.code()} - {e.details()}")
            return None
    
    def ping(self):
        """Check if node is alive"""
        try:
            request = chord_pb2.PingRequest()
            response = self.stub.Ping(request, timeout=15)
            
            if response.alive:
                print(f" Node {response.node_id} is alive")
                return True
            else:
                print(f"Node not responding")
                return False
        
        except grpc.RpcError as e:
            print(f" Ping error: {e.code()} - {e.details()}")
            return False


def print_usage():
    """Print usage information"""
    print("""
Enhanced Chord DHT Client

Usage:
    chord_client.py <host:port> <command> [args...]

Commands:
    put <key> <value>    - Store key-value pair
    get <key>            - Retrieve value for key
    delete <key>         - Delete key
    find <id>            - Find successor of ID
    stats                - Get node statistics
    ping                 - Check if node is alive
""")


def main():
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    
    # Parse target
    target = sys.argv[1]
    parts = target.split(':')
    
    if len(parts) != 2:
        print("Error: Target must be in format host:port")
        sys.exit(1)
    
    address = parts[0]
    port = int(parts[1])
    
    # Parse command
    command = sys.argv[2].lower()
    
    # Create client
    client = ChordClient(address, port)
    
    # Execute command
    if command == 'put':
        if len(sys.argv) < 5:
            print("Usage: put <key> <value>")
            sys.exit(1)
        key = sys.argv[3]
        value = ' '.join(sys.argv[4:])  # Allow spaces in value
        client.put(key, value)
    
    elif command == 'get':
        if len(sys.argv) < 4:
            print("Usage: get <key>")
            sys.exit(1)
        key = sys.argv[3]
        client.get(key)
    
    elif command == 'delete':
        if len(sys.argv) < 4:
            print("Usage: delete <key>")
            sys.exit(1)
        key = sys.argv[3]
        client.delete(key)
    
    elif command == 'find':
        if len(sys.argv) < 4:
            print("Usage: find <id>")
            sys.exit(1)
        key_id = int(sys.argv[3])
        client.find_successor(key_id)
    
    elif command == 'stats':
        client.get_stats()
    
    elif command == 'ping':
        client.ping()
    
    else:
        print(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)


if __name__ == '__main__':
    main()
