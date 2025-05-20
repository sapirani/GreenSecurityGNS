#!/bin/sh


/home/send_trigger.sh start_measurement 2


hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar -D mapreduce.job.reduces=2 -D mapreduce.job.maps=2 \
-input hdfs://namenode:9000/input \
-output hdfs://namenode:9000/output \
-mapper /home/mapper.py \
-reducer /home/reducer.py \
-file /home/mapper.py \
-file /home/reducer.py

/home/send_trigger.sh stop_measurement 2
