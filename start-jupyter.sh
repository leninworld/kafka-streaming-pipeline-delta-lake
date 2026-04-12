#!/usr/bin/env bash

set -euo pipefail

export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"
export PYSPARK_SUBMIT_ARGS="--master ${SPARK_MASTER_URL} --packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=/delta/warehouse pyspark-shell"

mkdir -p /opt/spark/notebooks
cd /opt/spark/notebooks

exec jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.allow_remote_access=True \
  --ServerApp.root_dir=/opt/spark/notebooks
