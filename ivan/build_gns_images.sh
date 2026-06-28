#!/bin/sh

set -euo pipefail

cd ./base

wait

chmod +x /build_base.sh

sudo ./build_base.sh

wait

cd ..

wait

cd ./measurements_base

wait

chmod +x ./build_measurements.sh

sudo ./build_measurements.sh

wait

cd ..

wait

cd ./environment_setup

wait

chmod +x ./build_hadoop_env.sh

sudo ./build_hadoop_env.sh

wait

cd ..

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
