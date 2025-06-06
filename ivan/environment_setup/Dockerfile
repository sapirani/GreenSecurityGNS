FROM hadoop-measurements-base

ENV CORE_CONF_fs_defaultFS=hdfs://namenode-1:9000
ENV CORE_CONF_hadoop_http_staticuser_user=root
ENV CORE_CONF_hadoop_proxyuser_hue_hosts=*
ENV CORE_CONF_hadoop_proxyuser_hue_groups=*
ENV CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec

ENV HDFS_CONF_dfs_webhdfs_enabled=true
ENV HDFS_CONF_dfs_permissions_enabled=false
ENV HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false

ENV YARN_CONF_yarn_log___aggregation___enable=true
ENV YARN_CONF_yarn_log_server_url=http://historyserver-1:8188/applicationhistory/logs/
ENV YARN_CONF_yarn_resourcemanager_recovery_enabled=true
ENV YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
ENV YARN_CONF_yarn_resourcemanager_scheduler_class=org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
ENV YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___mb=8192
ENV YARN_CONF_yarn_scheduler_capacity_root_default_maximum___allocation___vcores=4
ENV YARN_CONF_yarn_resourcemanager_fs_state___store_uri=/rmstate
ENV YARN_CONF_yarn_resourcemanager_system___metrics___publisher_enabled=true
ENV YARN_CONF_yarn_resourcemanager_hostname=resourcemanager-1
ENV YARN_CONF_yarn_resourcemanager_address=resourcemanager-1:8032
ENV YARN_CONF_yarn_resourcemanager_scheduler_address=resourcemanager-1:8030
ENV YARN_CONF_yarn_resourcemanager_resource__tracker_address=resourcemanager-1:8031
ENV YARN_CONF_yarn_timeline___service_enabled=true
ENV YARN_CONF_yarn_timeline___service_generic___application___history_enabled=true
ENV YARN_CONF_yarn_timeline___service_hostname=historyserver-1
ENV YARN_CONF_mapreduce_map_output_compress=true
ENV YARN_CONF_mapred_map_output_compress_codec=org.apache.hadoop.io.compress.SnappyCodec
ENV YARN_CONF_yarn_nodemanager_resource_memory___mb=8192
ENV YARN_CONF_yarn_nodemanager_resource_cpu___vcores=1
ENV YARN_CONF_yarn_nodemanager_disk___health___checker_max___disk___utilization___per___disk___percentage=98.5
ENV YARN_CONF_yarn_nodemanager_remote___app___log___dir=/app-logs
ENV YARN_CONF_yarn_nodemanager_aux___services=mapreduce_shuffle

ENV MAPRED_CONF_mapreduce_framework_name=yarn
ENV MAPRED_CONF_mapred_child_java_opts=-Xmx4096m
ENV MAPRED_CONF_mapreduce_map_memory_mb=4096
ENV MAPRED_CONF_mapreduce_reduce_memory_mb=4096
ENV MAPRED_CONF_mapreduce_map_java_opts=-Xmx3072m
ENV MAPRED_CONF_mapreduce_reduce_java_opts=-Xmx6144m
ENV MAPRED_CONF_yarn_app_mapreduce_am_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.4.1/
ENV MAPRED_CONF_mapreduce_map_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.4.1/
ENV MAPRED_CONF_mapreduce_reduce_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.4.1/
