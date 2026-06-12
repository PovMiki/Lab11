import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, sum as _sum, window

spark = SparkSession.builder.appName("LAB11_StructuredStreaming").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

df = spark.readStream.schema(schema).option("header", True).csv("data/input_stream")
df = df.withColumn("event_time", to_timestamp(col("event_time"))).dropna().filter(col("amount") > 0)

# Zadanie 4
tumbling = df.withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("category")) \
    .agg(count("*").alias("events_count"), _sum("amount").alias("total_amount"))

sliding = df.withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds"), col("category")) \
    .agg(count("*").alias("events_count"))

q_tumbling = tumbling.writeStream.format("console").outputMode("update") \
    .option("truncate", False).queryName("okno_stale").start()

q_sliding = sliding.writeStream.format("console").outputMode("update") \
    .option("truncate", False).queryName("okno_przesuwne").start()

# Zadanie 5
q_files = tumbling.writeStream.format("parquet").outputMode("append") \
    .option("path", "data/output_stream") \
    .option("checkpointLocation", "checkpoints/lab11") \
    .start()

spark.streams.awaitAnyTermination()
