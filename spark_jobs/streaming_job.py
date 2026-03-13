from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("KafkaDeltaStreaming").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:9092") \
    .option("subscribe","beauty_events") \
    .option("startingOffsets","earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

schema = StructType() \
    .add("username", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("event_date", StringType())

parsed = json_df.select(from_json(col("json"),schema).alias("data"))

table_df = parsed.select("data.*")

query = table_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation","/tmp/events_checkpoint") \
    .start("/delta/events")

query.awaitTermination()