FROM hadoop-env

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870

ENV CLUSTER_NAME=test
ENV HDFS_NAMENODE_HTTP_ADDRESS=0.0.0.0:9870

COPY generate_random_words.py ..

CMD ["/run.sh"]
