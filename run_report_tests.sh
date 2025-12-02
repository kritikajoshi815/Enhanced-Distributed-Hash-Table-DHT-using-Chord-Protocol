#!/bin/bash

set -e


print_msg() {
    echo -e "${2}${1}${NC}"
}

# Configuration
NUM_NODES=8
START_PORT=5001
BASE_ADDRESS="localhost"
RESULTS_FILE="report_metrics.json"
PIDS=()

cleanup() {
    print_msg "Cleaning up nodes..." 
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    wait 2>/dev/null || true
    print_msg " Cleanup complete" 
}

trap cleanup EXIT INT TERM

echo ""
print_msg "======================================" 
print_msg "Report Metrics Test Runner" 
print_msg "======================================" 
echo ""

# Start nodes
print_msg "Starting $NUM_NODES nodes..." 
PORT=$START_PORT
python3 chord_node.py $PORT > /dev/null 2>&1 &
PIDS+=($!)
print_msg "  Node 1 started (port $PORT)" 
NODE_ADDRESSES="${BASE_ADDRESS}:${PORT}"
sleep 3

for i in $(seq 2 $NUM_NODES); do
    PORT=$((START_PORT + i - 1))
    python3 chord_node.py $PORT --join ${BASE_ADDRESS}:${START_PORT} > /dev/null 2>&1 &
    PIDS+=($!)
    print_msg "  Node $i started (port $PORT)" 
    NODE_ADDRESSES="${NODE_ADDRESSES} ${BASE_ADDRESS}:${PORT}"
    sleep 2
done

echo ""
print_msg " All nodes running" 
print_msg "Waiting 20s for stabilization..." 
sleep 20
echo ""

# Run enhanced stress test
print_msg "Running comprehensive test with 5000 operations..." 
echo ""

python3 enhanced_stress_test.py $NODE_ADDRESSES \
    --operations 5000 \
    --threads 20 \
    --duration 180 \
    --output "$RESULTS_FILE"

echo ""
print_msg " Test complete!" 
echo ""

# Generate plots
print_msg "Generating plots and tables..." 
python3 plot_report.py "$RESULTS_FILE" --output-dir report_plots

echo ""
print_msg "======================================" 
print_msg " SUCCESS!" 
print_msg "======================================" 
echo ""


read -p "Keep nodes running? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    cleanup
else
    print_msg "Nodes still running. PIDs: ${PIDS[*]}" 
    trap - EXIT
fi
