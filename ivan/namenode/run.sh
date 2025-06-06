#!/bin/bash

namedir=`echo $HDFS_CONF_dfs_namenode_name_dir | perl -pe 's#file://##'`
if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

echo "remove lost+found from $namedir"
rm -r $namedir/lost+found

if [ "`ls -A $namedir`" == ".gns3_perms" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format $CLUSTER_NAME -force
fi

PYTHONPATH=/green_security_measurements/Scanner /green_security_measurements/green_security_venv/bin/python -m scanner_trigger.trigger_receiver --python_path=/green_security_measurements/green_security_venv/bin/python --scanner_path=/green_security_measurements/Scanner/scanner.py -n=19 &

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode > namenode_log.txt &
bash
