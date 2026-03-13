from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "beauty_events"
CHECKPOINT_LOCATION = "/tmp/events_checkpoint"
DELTA_PATH = "/delta/events"

EVENT_SCHEMA = (
    StructType()
    .add("username", StringType())
    .add("action", StringType())
    .add("timestamp", DoubleType())
    .add("event_date", StringType())
)


spark = SparkSession.builder.appName("KafkaDeltaStreaming").getOrCreate()

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

table_df = (
    kafka_df.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
)

query = (
    table_df.writeStream.format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start(DELTA_PATH)
)

query.awaitTermination()
