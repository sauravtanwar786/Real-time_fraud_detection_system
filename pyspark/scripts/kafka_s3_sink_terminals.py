from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

KAFKA_BROKERS = 'kafka:9092'
#KAFKA_TOPIC_NAME = "debezium.payment.terminals"
S3_ENDPOINT = "http://minio:9000"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/debezium.payment.terminals/"


from pyspark.sql import SparkSession
spark = (SparkSession.builder
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.hadoop:hadoop-aws:3.3.2")
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) 
                .config("spark.hadoop.fs.s3a.access.key", "minio") 
                .config("spark.hadoop.fs.s3a.secret.key", "minio123") 
                .config("spark.hadoop.fs.s3a.path.style.access", "true") 
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://commerce/warehouse/") 
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
                .appName("streaming_sink_terminals")
         .getOrCreate())
# Read data from Kafka (batch mode)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "debezium.payment.terminals") \
    .option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType

# Define the schema for the JSON messages
column_schema = StructType([
StructField("schema", StringType(), False),
StructField("payload", StructType([
    StructField("before", StructType([
        StructField("terminal_id", IntegerType(), False),
        StructField("x_location", DoubleType(), False),
        StructField("y_location", DoubleType(), False)
    ]), True),
    StructField("after", StructType([
        StructField("terminal_id", IntegerType(), False),
        StructField("x_location", DoubleType(), False),
        StructField("y_location", DoubleType(), False)
    ]), True),
    StructField("source", StructType([
        StructField("version", StringType(), False),
        StructField("connector", StringType(), False),
        StructField("name", StringType(), False),
        StructField("ts_ms", LongType(), False),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), False),
        StructField("sequence", StringType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("schema", StringType(), False),
        StructField("table", StringType(), False),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True)
    ]), False),
    StructField("transaction", StructType([
        StructField("id", StringType(), False),
        StructField("total_order", LongType(), False),
        StructField("data_collection_order", LongType(), False)
    ]), True),
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)
]))])

from pyspark.sql.functions import from_json

kafka_df = kafka_df.withColumn("value", col("value").cast(StringType())) \
    .withColumn("key", col("key").cast(StringType()))
# Apply the schema to the DataFrame
kafka_df = kafka_df.withColumn("value", from_json("value", column_schema))
kafka_df.createOrReplaceTempView("kafkadata")

spark.sql("""
create database if not exists payment
""").show(truncate=False)

spark.sql("""
        CREATE TABLE IF NOT EXISTS payment.terminal (
      terminal_id INT,
      x_location FLOAT,
      y_location FLOAT,
      row_created_timestamp TimeStamp,
      row_updated_timestamp TimeStamp
)
USING iceberg
""")

def foreach_batch_function(df, epoch_id):
    spark = df.sparkSession
    df.createOrReplaceTempView('micro_batch')
    latest_data = spark.sql(""" 
                with extracted_data  as (
                    select value.payload.after.terminal_id,
                    value.payload.after.x_location, 
                    value.payload.after.y_location,
                    timestamp
                    from micro_batch),
              ranked_data as (
                  select terminal_id,
                  x_location,
                  y_location, 
                  timestamp,
                  ROW_NUMBER() over (partition by terminal_id order by timestamp DESC) as rn
                  from extracted_data) 
              select terminal_id,
                  x_location,
                  y_location,
                  now() as row_created_timestamp,
                  now() as row_updated_timestamp
              from ranked_data 
              where rn=1""")
    latest_data.createOrReplaceTempView('latest_data')
    spark.sql("""
MERGE INTO payment.terminal AS target
USING latest_data AS source
ON target.terminal_id = source.terminal_id
WHEN MATCHED THEN
  UPDATE SET
    target.x_location = source.x_location,
    target.y_location = source.y_location,
    target.row_updated_timestamp = source.row_updated_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    terminal_id,
    x_location,
    y_location,
    row_created_timestamp,
    row_updated_timestamp
  )
  VALUES (
    source.terminal_id,
    source.x_location,
    source.y_location,
    source.row_created_timestamp,
    source.row_updated_timestamp
  )
""").show(truncate=False)
    
query = kafka_df.writeStream \
.foreachBatch(foreach_batch_function) \
.option("checkpointLocation", CHECKPOINT_PATH) \
.trigger(processingTime='5 seconds') \
.start()

query.awaitTermination()