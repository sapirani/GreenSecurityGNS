sudo ./base/build_base.sh

sudo ./measurements_base/build_measurements.sh

sudo ./environment_setup/build_hadoop_env.sh

cd ./resourcemanager

sudo docker build -t resourcemanager .

cd ..

cd ./namenode

sudo docker build -t namenode .

cd ..

cd ./datanode

sudo docker build -t datanode .

cd ..

cd ./historyserver

sudo docker build -t historyserver .

