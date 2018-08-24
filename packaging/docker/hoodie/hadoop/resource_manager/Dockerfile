ARG HADOOP_VERSION=2.8.4 
ARG HADOOP_RM_PORT=8088
FROM hoodie-hadoop-base_${HADOOP_VERSION}:latest

HEALTHCHECK CMD curl -f http://localhost:${HADOOP_RM_PORT}/ || exit 1

ADD run_rm.sh /run_rm.sh
RUN chmod a+x /run_rm.sh

CMD ["/run_rm.sh"]
