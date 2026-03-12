from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ---------------------------
# Read from Kafka
# ---------------------------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:9092") \
    .option("subscribe","beauty_events") \
    .option("startingOffsets","earliest") \
    .load()

# Convert Kafka binary value → string
json_df = df.selectExpr("CAST(value AS STRING) as json")

# ---------------------------
# Define JSON schema
# ---------------------------

address_schema = StructType() \
    .add("street", StringType()) \
    .add("city", StringType()) \
    .add("state", StringType())

schema = StructType() \
    .add("username", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", DoubleType()) \
    .add("event_date", StringType()) \
    .add("product_id", StringType()) \
    .add("product_name", StringType()) \
    .add("product_category", StringType()) \
    .add("product_subcategory1", StringType()) \
    .add("product_subcategory2", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("total_price", DoubleType()) \
    .add("delivery_address", address_schema)

# ---------------------------
# Parse JSON
# ---------------------------

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
)

# ---------------------------
# Flatten fields
# ---------------------------

table_df = parsed_df.select(
    col("data.username").alias("user"),  # rename username → user
    col("data.action"),
    col("data.timestamp"),
    col("data.event_date"),
    col("data.product_id"),
    col("data.product_name"),
    col("data.product_category"),
    col("data.product_subcategory1"),
    col("data.product_subcategory2"),
    col("data.price"),
    col("data.quantity"),
    col("data.total_price"),
    col("data.delivery_address.street").alias("street"),
    col("data.delivery_address.city").alias("city"),
    col("data.delivery_address.state").alias("state")
)

# ---------------------------
# Print decoded table
# ---------------------------

console_query = table_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

# ---------------------------
# Write to Delta Lake
# ---------------------------

delta_query = table_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("mergeSchema","true") \
    .partitionBy("event_date") \
    .option("checkpointLocation","/tmp/delta/events_checkpoint") \
    .start("/tmp/delta/events")

# ---------------------------
# Wait for streams
# ---------------------------

spark.streams.awaitAnyTermination()