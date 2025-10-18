FROM hadoop-env

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

HEALTHCHECK CMD curl -f http://localhost:8088/ || exit 1

RUN /green_security_measurements/green_security_venv/bin/pip3 install pydantic==2.11.7

ADD run.sh /run.sh
RUN chmod a+x /run.sh

COPY mapper.py /home
COPY slow_mapper.py /home
COPY reducer.py /home
COPY slow_reducer.py /home

WORKDIR /home
COPY automatic_experiments_parameters.py.example automatic_experiments_parameters.py
COPY automatic_experiments.py .
COPY hadoop_job_config.py .
COPY jobs_configurator.py .
COPY run_task.py .
COPY trigger_sender.py .
COPY send_trigger_deprecated.sh .

WORKDIR /green_security_measurements/Scanner

EXPOSE 8088

ENV YARN_RESOURCEMANAGER_HEAPSIZE=8192
##ENV SERVICE_PRECONDITION="namenode:9000 namenode:9870 datanode1:9864 datanode2:9864 datanode3:9864"

CMD ["/run.sh"]
