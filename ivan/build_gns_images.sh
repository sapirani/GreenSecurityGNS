#!/bin/sh

sudo ./base/build_base.sh

wait

sudo ./measurements_base/build_measurements.sh

wait

sudo ./environment_setup/build_hadoop_env.sh

wait

cd ./resourcemanager

wait

sudo docker build -t resourcemanager .

wait

cd ..

wait

cd ./namenode

wait

sudo docker build -t namenode .

wait

cd ..

wait

cd ./datanode

wait

sudo docker build -t datanode .

wait

cd ..

wait

cd ./historyserver

wait

sudo docker build -t historyserver .

