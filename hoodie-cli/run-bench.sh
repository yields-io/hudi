#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hoodie-cli-*-SNAPSHOT.jar`
if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi
if [ -z "$SPARK_CONF_DIR" ]; then
  echo "setting spark conf dir"
  SPARK_CONF_DIR="/etc/spark/conf"
fi
if [ -z "$CLIENT_JAR" ]; then
  echo "client jar location not set"
fi
java -cp ${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:$DIR/target/lib/*:$HOODIE_JAR:${CLIENT_JAR} com.uber.hoodie.cli.bench.CreateHandlePerfTestMain  --base-path /uber-data/tables/rawdata/kafka/hp-uflurry-event-trace/ -o /tmp/varadarb/perf/ -i /uber-data/tables/rawdata/kafka/hp-uflurry-event-trace/2018/05/01/fd2e8283-098c-4eed-acab-64cab744f019_0_20180501095828.parquet -s /home/varadarb/uflurry_event_trace.schema_wrapped.txt -n 1
