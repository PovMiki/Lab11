import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LAB11_BatchRead").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

result = spark.read.parquet("data/output_stream")
print("czy strumieniowy", result.isStreaming)
result.orderBy("window").show(50, truncate=False)
