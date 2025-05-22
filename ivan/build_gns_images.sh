#!/bin/sh

cd ./base

wait

sudo ./build_base.sh

wait

cd ..

wait

cd ./measurements_base

wait

sudo ./build_measurements.sh

wait

cd ..

wait

cd ./environment_setup

wait

sudo ./build_hadoop_env.sh

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

