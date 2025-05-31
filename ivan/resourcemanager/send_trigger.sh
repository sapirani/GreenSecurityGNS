#!/bin/sh

base_nodes_addresses="resourcemanager-1:65432,namenode-1:65432,historyserver-1:65432"

# Check if the datanodes argument is provided
if [ -z "$2" ]; then
  # Default number of datanodes if not provided
  datanodes=3
else
  # Use the value passed as the first argument
  datanodes=$2
fi

all_nodes_addresses="$base_nodes_addresses"

for i in $(seq 1 $datanodes); do
  all_nodes_addresses="$all_nodes_addresses,datanode-$i:65432"
done


PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_sender $1 -r "$all_nodes_addresses"
