#!/bin/bash
export HADOOP_HOME=${HADOOP_HOME:-/path/to/hadoop}
export FLINK_HOME=${FLINK_HOME:-/path/to/flink}
export HIVE_HOME=${HIVE_HOME:-/path/to/hive}

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH:-$(hadoop classpath)}
export YARN_CONF_DIR=${YARN_CONF_DIR:-$HADOOP_CONF_DIR}
export HIVE_CONF_DIR=${HIVE_CONF_DIR:-$HIVE_HOME/conf}
export FLINK_CONF_DIR=${FLINK_CONF_DIR:-$FLINK_HOME/conf}
export FLINK_LOG_DIR=${FLINK_LOG_DIR:-/var/log/flink}
export FLINK_PID_DIR=${FLINK_PID_DIR:-/var/run/flink}
export PYFLINK_PYTHON=${PYFLINK_PYTHON:-python3}
export PYFLINK_CLIENT_EXECUTABLE=${PYFLINK_CLIENT_EXECUTABLE:-python3}
export MAX_LOG_FILE_NUMBER=${MAX_LOG_FILE_NUMBER:-3}
export FLINK_ENV_JAVA_OPTS="-XX:-HeapDumpOnOutOfMemoryError"
export _FLINK_HOME_DETERMINED=1
