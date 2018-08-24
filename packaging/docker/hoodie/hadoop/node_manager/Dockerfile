ARG HADOOP_VERSION=2.8.4 
ARG HADOOP_NM_PORT=8042
FROM hoodie-hadoop-base_${HADOOP_VERSION}:latest

HEALTHCHECK CMD curl -f http://localhost:${HADOOP_NM_PORT}/ || exit 1

ADD run_nm.sh /run_nm.sh
RUN chmod a+x /run_nm.sh

CMD ["/run_nm.sh"]
