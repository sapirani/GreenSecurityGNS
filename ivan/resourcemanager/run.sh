#!/bin/bash

#TODO: add health check and use depends in the docker compose

sleep 40

PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_receiver --python_path=/green_security_measurements/green_security_venv/bin/python --scanner_path=/green_security_measurements/Scanner/scanner.py -n=19  &
PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m hadoop_optimizer.job_runner.server.api.main  &

python /home/generate_random_words.py -s 0.3 -o /input/0.3_gb --skip-if-exists &
python /home/generate_random_words.py -s 0.7 -o /input/0.7_gb --skip-if-exists &
python /home/generate_random_words.py -s 1.0 -o /input/1.0_gb --skip-if-exists &

wait

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR resourcemanager > resourcemanager_log.txt &
bash
