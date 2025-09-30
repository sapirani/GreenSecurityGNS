#!/bin/bash

sed -i '/127.0.1.1/d' /etc/hosts

# Update dhcp to generate dynamic ip address
dhclient

# avoid the restart of resolve.conf by the dhclient - make sure you run dhclient once beforehand
cat << 'EOF' > /etc/dhcp/dhclient-enter-hooks.d/noupdate-resolv
#!/bin/sh
# Prevent dhclient from modifying /etc/resolv.conf

make_resolv_conf() {
    echo "Skipping resolv.conf update by dhclient"
}
EOF

chmod +x /etc/dhcp/dhclient-enter-hooks.d/noupdate-resolv

# avoid IPV6 queries in DNS (because it takes so much time and eventually fails)
echo "options no-aaaa" >> /etc/resolv.conf


# Set some sensible defaults
export CORE_CONF_fs_defaultFS=${CORE_CONF_fs_defaultFS:-hdfs://`hostname -f`:8020}

function addProperty() {
  local path=$1
  local name=$2
  local value=$3

  local entry="<property><name>$name</name><value>${value}</value></property>"
  local escapedEntry=$(echo $entry | sed 's/\//\\\//g')
  sed -i "/<\/configuration>/ s/.*/${escapedEntry}\n&/" $path
}

function configure() {
    local path=$1
    local module=$2
    local envPrefix=$3

    local var
    local value

    echo "Configuring $module"
    for c in `printenv | perl -sne 'print "$1 " if m/^${envPrefix}_(.+?)=.*/' -- -envPrefix=$envPrefix`; do
        name=`echo ${c} | perl -pe 's/___/-/g; s/__/@/g; s/_/./g; s/@/_/g;'`
        var="${envPrefix}_${c}"
        value=${!var}
        echo " - Setting $name=$value"
        addProperty $path $name "$value"
    done
}

configure /etc/hadoop/core-site.xml core CORE_CONF
configure /etc/hadoop/hdfs-site.xml hdfs HDFS_CONF
configure /etc/hadoop/yarn-site.xml yarn YARN_CONF
configure /etc/hadoop/httpfs-site.xml httpfs HTTPFS_CONF
configure /etc/hadoop/kms-site.xml kms KMS_CONF
configure /etc/hadoop/mapred-site.xml mapred MAPRED_CONF

if [ "$MULTIHOMED_NETWORK" = "1" ]; then
    echo "Configuring for multihomed network"

    # HDFS
    addProperty /etc/hadoop/hdfs-site.xml dfs.namenode.rpc-bind-host 0.0.0.0
    addProperty /etc/hadoop/hdfs-site.xml dfs.namenode.servicerpc-bind-host 0.0.0.0
    addProperty /etc/hadoop/hdfs-site.xml dfs.namenode.http-bind-host 0.0.0.0
    addProperty /etc/hadoop/hdfs-site.xml dfs.namenode.https-bind-host 0.0.0.0
    addProperty /etc/hadoop/hdfs-site.xml dfs.client.use.datanode.hostname true
    addProperty /etc/hadoop/hdfs-site.xml dfs.datanode.use.datanode.hostname true

    # YARN
    addProperty /etc/hadoop/yarn-site.xml yarn.resourcemanager.bind-host 0.0.0.0
    addProperty /etc/hadoop/yarn-site.xml yarn.nodemanager.bind-host 0.0.0.0
    addProperty /etc/hadoop/yarn-site.xml yarn.timeline-service.bind-host 0.0.0.0

    # NOTE: I added that
    addProperty /etc/hadoop/yarn-site.xml yarn.scheduler.multi-node-scheduling.enabled true
    # addProperty /etc/hadoop/yarn-site.xml yarn.scheduler.maximum-allocation-vcores 8
    # addProperty /etc/hadoop/yarn-site.xml yarn.scheduler.maximum-allocation-mb 10000
    # addProperty /etc/hadoop/yarn-site.xml yarn.resourcemanager.node-blacklisting.enabled true

    # MAPRED
    addProperty /etc/hadoop/mapred-site.xml yarn.nodemanager.bind-host 0.0.0.0

    # Sagi and Sapir added configuration
    addProperty /etc/hadoop/mapred-site.xml mapreduce.shuffle.port 13562
    addProperty /etc/hadoop/mapred-site.xml mapreduce.map.log.level DEBUG
    addProperty /etc/hadoop/mapred-site.xml mapreduce.task.files.preserve.failedtasks true
    addProperty /etc/hadoop/mapred-site.xml mapreduce.map.speculative false
    addProperty /etc/hadoop/mapred-site.xml mapreduce.reduce.speculative false

    # instead of resource env variable
    addProperty /etc/hadoop/mapred-site.xml mapreduce.map.java.opts "-Xms512m -Xmx1024m -XX:+UseG1GC -Xloggc:/var/log/hadoop/gc_logs/map_gc.log"


fi

if [ -n "$GANGLIA_HOST" ]; then
    mv /etc/hadoop/hadoop-metrics.properties /etc/hadoop/hadoop-metrics.properties.orig
    mv /etc/hadoop/hadoop-metrics2.properties /etc/hadoop/hadoop-metrics2.properties.orig

    for module in mapred jvm rpc ugi; do
        echo "$module.class=org.apache.hadoop.metrics.ganglia.GangliaContext31"
        echo "$module.period=10"
        echo "$module.servers=$GANGLIA_HOST:8649"
    done > /etc/hadoop/hadoop-metrics.properties

    for module in namenode datanode resourcemanager nodemanager mrappmaster jobhistoryserver; do
        echo "$module.sink.ganglia.class=org.apache.hadoop.metrics2.sink.ganglia.GangliaSink31"
        echo "$module.sink.ganglia.period=10"
        echo "$module.sink.ganglia.supportsparse=true"
        echo "$module.sink.ganglia.slope=jvm.metrics.gcCount=zero,jvm.metrics.memHeapUsedM=both"
        echo "$module.sink.ganglia.dmax=jvm.metrics.threadsBlocked=70,jvm.metrics.memHeapUsedM=40"
        echo "$module.sink.ganglia.servers=$GANGLIA_HOST:8649"
    done > /etc/hadoop/hadoop-metrics2.properties
fi

function wait_for_it()
{
    local serviceport=$1
    local service=${serviceport%%:*}
    local port=${serviceport#*:}
    local retry_seconds=5
    local max_try=100
    let i=1

    nc -z $service $port
    result=$?

    until [ $result -eq 0 ]; do
      echo "[$i/$max_try] check for ${service}:${port}..."
      echo "[$i/$max_try] ${service}:${port} is not available yet"
      if (( $i == $max_try )); then
        echo "[$i/$max_try] ${service}:${port} is still not available; giving up after ${max_try} tries. :/"
        exit 1
      fi

      echo "[$i/$max_try] try in ${retry_seconds}s once again ..."
      let "i++"
      sleep $retry_seconds

      nc -z $service $port
      result=$?
    done
    echo "[$i/$max_try] $service:${port} is available."
}

for i in ${SERVICE_PRECONDITION[@]}
do
    wait_for_it ${i}
done

exec $@
