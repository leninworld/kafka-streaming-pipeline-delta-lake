from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterDeltaTable") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS default")

spark.sql("""
CREATE TABLE IF NOT EXISTS beauty_events
USING DELTA
LOCATION '/delta/events'
""")

print("Delta table registered successfully")

spark.sql("MSCK REPAIR TABLE beauty_events")

spark.stop()