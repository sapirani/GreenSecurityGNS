#!/bin/bash

datadir=`echo $HDFS_CONF_dfs_datanode_data_dir | perl -pe 's#file://##'`
if [ ! -d $datadir ]; then
  echo "Datanode data directory not found: $datadir"
  exit 2
fi

PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_receiver --python_path=/green_security_measurements/green_security_venv/bin/python --scanner_path=/green_security_measurements/Scanner/scanner.py -n=19 &

#NOTE: originally it was only $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode > datanode_log.txt &

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR nodemanager > nodemanager_log.txt &

bash
