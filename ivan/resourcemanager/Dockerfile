FROM hadoop-env

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

HEALTHCHECK CMD curl -f http://localhost:8088/ || exit 1

RUN /green_security_measurements/green_security_venv/bin/pip3 install -r pydantic==2.11.7

ADD run.sh /run.sh
RUN chmod a+x /run.sh

COPY mapper.py /home
COPY slow_mapper.py /home
COPY reducer.py /home
COPY slow_reducer.py /home

# TODO: UPDATE DOCKERFILE WITH THE NEW SCRIPTS FOR RUNNING AUTOMATIC EXPERIMENTS
WORKDIR /home
COPY run_task_with_scanner.sh .
COPY run_task_without_scanner.sh .
COPY send_trigger.sh .
RUN chmod +x run_task_with_scanner.sh
RUN chmod +x run_task_without_scanner.sh
RUN chmod +x send_trigger.sh

WORKDIR /green_security_measurements/Scanner

EXPOSE 8088

ENV YARN_RESOURCEMANAGER_HEAPSIZE=8192
##ENV HADOOP_OPTS="-Xloggc:/path/to/pc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
##ENV SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode1:9864 datanode2:9864 datanode3:9864"

CMD ["/run.sh"]
