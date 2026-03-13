# Kafka + Spark + Delta + Superset

This project streams user events from Kafka into a Delta table with Spark Structured Streaming, registers that table in a Hive metastore, and exposes it through Spark Thrift Server for tools like Superset.

## Services

- `zookeeper` and `kafka`: event transport
- `spark-master` and `spark-worker`: Spark standalone cluster
- `spark-streaming`: Kafka to Delta writer
- `hive-metastore`: table metadata service
- `spark-thrift`: SQL endpoint for BI tools
- `spark-register`: one-shot job that registers `beauty_events`
- `superset`: optional BI UI

## Start The Stack

```bash
docker compose build spark-master
docker compose up -d zookeeper kafka hive-metastore spark-master spark-worker
docker compose up -d spark-thrift
docker compose up spark-register
docker compose up -d spark-streaming superset
```

## Produce Test Data

Run the local producer from this repo:

```bash
python3 kafka_producer_user_events.py
```

If you want a simple debug consumer on your Mac, use:

```bash
python3 kafka_consumer_user_events.py
```

## Query The Table

Use Spark SQL from the `spark-thrift` container:

```bash
docker exec -it spark-thrift /opt/spark/bin/spark-sql \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Then run:

```sql
SHOW TABLES;
SELECT * FROM beauty_events LIMIT 10;
```

## Superset Connection

Use Spark Thrift Server as the database:

```text
hive://spark-thrift:10000/default?auth=NOSASL
```

If you are connecting from outside Docker, use `localhost:10000` instead of `spark-thrift:10000`.

## Notes

- The metastore uses embedded Derby inside `hive-metastore`.
- The Delta table lives at `/delta/events`.
- `spark-streaming` enables schema merge so the Delta table can evolve from an empty initial schema.
