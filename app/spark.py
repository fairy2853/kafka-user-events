from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

# spark builder
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.checkpointLocation", "./tmp/spark_checkpoints/") \
    .config("spark.hadoop.hadoop.native.io", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()

# scheme
schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", LongType()) \
    .add("ip", StringType()) \
    .add("user_agent", StringType()) \
    .add("page", StringType()) \
    .add("region", StringType())

# read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .load()

# JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write in Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "../tmp/output_parquet") \
    .option("checkpointLocation", "../tmp/spark_checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
