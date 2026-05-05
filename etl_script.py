import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# --- STEP 1: LOAD DATA ---
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "clickstream_db", 
    table_name = "raw_clicks"
).toDF()

# --- STEP 2: CLICKSTREAM TRANSFORMATIONS ---
clicks_df = datasource.filter(F.col("metadata.table") == "clickstream")

# Define window for session logic
user_window = Window.partitionBy("data.user_id").orderBy("metadata.ts")

# Calculate session IDs AND extract the Date for Partitioning
clicks_final = clicks_df.withColumn("prev_ts", F.lag("metadata.ts").over(user_window)) \
    .withColumn("is_new", F.when((F.col("metadata.ts") - F.col("prev_ts")) > 1800, 1).otherwise(0)) \
    .withColumn("session_id", F.sum("is_new").over(user_window)) \
    .withColumn("real_time", F.to_timestamp(F.col("metadata.ts"))) \
    .withColumn("year", F.year(F.col("real_time"))) \
    .withColumn("month", F.month(F.col("real_time"))) \
    .withColumn("day", F.dayofmonth(F.col("real_time")))\
    .select(
        F.col("data.user_id").alias("user_id"),
        F.col("data.event_id").alias("event_id"),
        F.col("data.url").alias("url"),
        F.col("data.platform").alias("platform"),
        F.col("data.duration_sec").alias("duration_sec"),
        F.col("session_id").cast("string").alias("session_id"),
        F.col("real_time").alias("ts"),
        "year", "month", "day" # Partitions stay at the end
    )

# --- STEP 3: CDC TRANSFORMATIONS (SCD TYPE 1) ---
users_df = datasource.filter(F.col("metadata.table") == "users")

# Window to find the most recent update per user_id
latest_user_window = Window.partitionBy("data.user_id").orderBy(F.desc("metadata.ts"))

# Keep only the row where row_number is 1 (The latest truth)
users_final = users_df.withColumn("rn", F.row_number().over(latest_user_window)) \
    .filter(F.col("rn") == 1) \
    .select(
        F.col("data.user_id").alias("user_id"),
        F.col("data.name").alias("name"),
        F.col("data.email").alias("email"),
        F.col("data.country").alias("country")
    )

# --- STEP 4: WRITE TO GOLD ---
# Write Clicks (Partitioned by the columns we just created)
clicks_final.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://clickstream-project-gold-dev/clicks/")

# Write Users (Not partitioned, just the latest state)
users_final.write.mode("overwrite").parquet("s3://clickstream-project-gold-dev/users/")

job.commit()