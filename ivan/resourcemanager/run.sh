#!/bin/bash

#TODO: add health check and use depends in the docker compose

sleep 40

PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_receiver --python_path=/green_security_measurements/green_security_venv/bin/python --scanner_path=/green_security_measurements/Scanner/scanner.py -n=19  &

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR resourcemanager > resourcemanager_log.txt &
bash
