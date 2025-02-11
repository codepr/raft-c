#!/bin/bash

# Configuration
NODES=(
    "node-0"
    "raft-0-0"
    "raft-0-1"
    "node-1"
    "raft-1-0"
    "raft-1-1"
    "node-2"
    "raft-2-0"
    "raft-2-1"
)

# Cleanup old logs
mkdir -p logs
rm -f logs/*.log

# Start nodes
for NODE in "${NODES[@]}"; do
    read -r CONF <<< "$NODE"
    echo "Starting node with config ${CONF}.conf"
    ./raft-c -c "conf/${CONF}.conf" > "logs/node_${CONF}.log" 2>&1 &
    sleep 0.5
done

echo "All nodes started. "
echo "Use 'pkill -f raft_node' to stop all nodes."
