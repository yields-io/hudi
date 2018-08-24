ARG HADOOP_VERSION=2.8.4 
ARG SECONDARY_HADOOP_WEBHDFS_PORT=50090
FROM hoodie-hadoop-base_${HADOOP_VERSION}:latest

HEALTHCHECK CMD curl -f http://localhost:${SECONDARY_HADOOP_WEBHDFS_PORT}/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run_sec_nn.sh /run_sec_nn.sh
RUN chmod a+x /run_sec_nn.sh

CMD ["/run_sec_nn.sh"]
