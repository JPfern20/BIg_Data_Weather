from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "streaming-data") \
    .load()

# Define schema for JSON messages
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("value", DoubleType()) \
    .add("metric_type", StringType()) \
    .add("sensor_id", StringType()) \
    .add("location", StringType()) \
    .add("unit", StringType())

# Parse JSON values
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to HDFS as Parquet
query = json_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/data/weather/") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/weather/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
