#!/bin/bash

ip addr add 192.168.25.13/24 dev eth0

PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_receiver --python_path=/green_security_measurements/green_security_venv/bin/python --scanner_path=/green_security_measurements/Scanner/scanner.py -n=19 &

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR historyserver &
bash
