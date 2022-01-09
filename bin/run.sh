#!/bin/bash

set -xe

SCRIPT_PATH="$(dirname $0)"
cd $SCRIPT_PATH
LOG_PATH="$(pwd)/../data/json/*"
JAR_PATH="$(pwd)/../target/scala-2.12/log-splitter_2.12-0.1.jar"
NUM_PARTITION=5

spark-submit --class etl.log.OrgLogSplit \
  --conf "spark.etl.logpath=${LOG_PATH}" \
  --conf "spark.sql.shuffle.partitions=${NUM_PARTITION}" \
  ${JAR_PATH}
