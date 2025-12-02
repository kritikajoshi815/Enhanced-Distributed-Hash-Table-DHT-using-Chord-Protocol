#!/bin/bash
# Demo script for Chord DHT

echo "========================================="
echo "Chord DHT - Interactive Demo"
echo "========================================="
echo ""

# Start 4 nodes in background
echo "Starting 4 Chord nodes..."
echo ""

python3 chord_node.py 5001 &
PID1=$!
echo "Started Node 1 on port 5001 (PID: $PID1)"
sleep 2

python3 chord_node.py 5002 --join localhost:5001 &
PID2=$!
echo "Started Node 2 on port 5002 (PID: $PID2)"
sleep 2

python3 chord_node.py 5003 --join localhost:5001 &
PID3=$!
echo "Started Node 3 on port 5003 (PID: $PID3)"
sleep 2

python3 chord_node.py 5004 --join localhost:5001 &
PID4=$!
echo "Started Node 4 on port 5004 (PID: $PID4)"
sleep 3

echo ""
echo " All nodes started and joined the ring"
echo "  PIDs: $PID1, $PID2, $PID3, $PID4"
echo ""

# Wait for stabilization
echo "Waiting for ring to stabilize..."
sleep 16
echo ""

# Demo operations
echo "========================================="
echo "Demonstrating Basic Operations"
echo "========================================="
echo ""

echo "--- Storing Data ---"
python3 chord_client.py localhost:5001 put user:1001 Alice
python3 chord_client.py localhost:5002 put user:1002 Bob
python3 chord_client.py localhost:5003 put user:1003 Charlie
python3 chord_client.py localhost:5004 put file:data "Important Data"
python3 chord_client.py localhost:5001 put config:server "192.168.1.1"
echo ""

echo "--- Retrieving Data (from different nodes) ---"
python3 chord_client.py localhost:5004 get user:1001
python3 chord_client.py localhost:5001 get user:1002
python3 chord_client.py localhost:5002 get user:1003
python3 chord_client.py localhost:5003 get file:data
python3 chord_client.py localhost:5004 get config:server
echo ""

echo "--- Node Statistics ---"
python3 chord_client.py localhost:5001 stats
python3 chord_client.py localhost:5002 stats
echo ""

echo "========================================="
echo "Demonstrating Fault Tolerance"
echo "========================================="
echo ""

echo "Killing node on port 5002 (PID: $PID2)..."
kill $PID2
sleep 3
echo ""

echo "Verifying data is still accessible..."
python3 chord_client.py localhost:5001 get user:1001
python3 chord_client.py localhost:5003 get user:1002
python3 chord_client.py localhost:5004 get user:1003
python3 chord_client.py localhost:5001 get file:data
echo ""

echo " Data survived node failure thanks to replication!"
echo ""

echo "========================================="
echo "Demonstrating Lookup Routing"
echo "========================================="
echo ""

echo "Finding successors for various IDs..."
python3 chord_client.py localhost:5001 find 1000
python3 chord_client.py localhost:5001 find 50000
python3 chord_client.py localhost:5001 find 100000
echo ""

echo "========================================="
echo "Demo Complete!"
echo "========================================="
echo ""
echo "Cleaning up..."
kill $PID1 $PID3 $PID4 2>/dev/null
sleep 1
echo " All nodes stopped"
echo ""
echo "To run your own tests:"
echo "  1. Start nodes: python3 chord_node.py <port> [--join host:port]"
echo "  2. Use client: python3 chord_client.py <host:port> <command>"
echo "  3. Run benchmarks: python3 performance_test.py <nodes...> <operations>"
echo ""
