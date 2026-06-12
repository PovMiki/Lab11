# Zadanie 1
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, sum as _sum, when

spark = SparkSession.builder.appName("LAB11_StructuredStreaming").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(spark.version)

# Zadanie 2
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

df = spark.readStream.schema(schema).option("header", True).csv("data/input_stream")
df = df.withColumn("event_time", to_timestamp(col("event_time"))).dropna().filter(col("amount") > 0)

print(df.isStreaming)
df.printSchema()

# Zadanie 3
paid = df.filter(col("status") == "paid") \
         .withColumn("size", when(col("amount") >= 100, "large").otherwise("small"))

summary = paid.groupBy("category").agg(
    count("*").alias("events_count"),
    _sum("amount").alias("total_amount"),
)

query = summary.writeStream.format("console").outputMode("complete").option("truncate", False).start()
query.awaitTermination()
