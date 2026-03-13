from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterDeltaTable") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("""
CREATE TABLE IF NOT EXISTS beauty_events
USING DELTA
LOCATION '/delta/events'
""")

print("Delta table registered successfully")

spark.stop()