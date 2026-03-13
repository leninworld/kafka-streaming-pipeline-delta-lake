from pyspark.sql import SparkSession

DATABASE_NAME = "default"
TABLE_NAME = "beauty_events"
DELTA_PATH = "/delta/events"

spark = (
    SparkSession.builder.appName("RegisterDeltaTable")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    USING DELTA
    LOCATION '{DELTA_PATH}'
    """
)

print("Delta table registered successfully")

spark.stop()
