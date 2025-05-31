#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_datanodes>"
    exit 1
fi


hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar -D mapreduce.job.reduces=$1 -D mapreduce.job.maps=$1 \
-input hdfs://namenode-1:9000/input \
-output hdfs://namenode-1:9000/output \
-mapper /home/mapper.py \
-reducer /home/reducer.py \
-file /home/mapper.py \
-file /home/reducer.py
