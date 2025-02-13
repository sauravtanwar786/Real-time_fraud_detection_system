from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

KAFKA_BROKERS = 'kafka:9092'
S3_ENDPOINT = "http://minio:9000"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/debezium.payment.customers/"


# Initialize SparkSession
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
                .appName("streaming_sink_customer")
                .getOrCreate())

# Define the schema for the JSON messages
column_schema = StructType([
    StructField("schema", StringType(), False),
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("x_location", DoubleType(), False),
            StructField("y_location", DoubleType(), False)
        ]), True),
        StructField("after", StructType([
            StructField("customer_id", IntegerType(), False),
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
    ]))
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "debezium.payment.customers") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df = kafka_df.withColumn("value", col("value").cast(StringType())) \
    .withColumn("key", col("key").cast(StringType()))

# Apply the schema to the DataFrame
kafka_df = kafka_df.withColumn("value", from_json("value", column_schema))
kafka_df.createOrReplaceTempView("kafkadata")

# Create database if not exists
spark.sql("""
create database if not exists payment
""").show(truncate=False)

# Create Iceberg table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS payment.customer (
    customer_id INT,
    x_location FLOAT,
    y_location FLOAT,
    row_created_timestamp TIMESTAMP,
    row_updated_timestamp TIMESTAMP
)
USING iceberg
""").show(truncate=False)

def foreach_batch_function(df, epoch_id):
    spark = df.sparkSession
    df.createOrReplaceTempView('micro_batch')
    latest_data = spark.sql("""
              with extracted_data  as (
                    select value.payload.after.customer_id,
                    value.payload.after.x_location, 
                    value.payload.after.y_location,
                    timestamp
                    from micro_batch),
              ranked_data as (
                  select customer_id,
                  x_location,
                  y_location, 
                  timestamp,
                  ROW_NUMBER() over (partition by customer_id order by timestamp DESC) as rn
                  from extracted_data) 
              select customer_id,
                  x_location,
                  y_location,
                  now() as row_created_timestamp,
                  now() as row_updated_timestamp
              from ranked_data 
              where rn=1""")
    latest_data.createOrReplaceTempView('latest_data')
    spark.sql("""
        MERGE INTO payment.customer AS target
USING latest_data AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.x_location = source.x_location,
    target.y_location = source.y_location,
    target.row_updated_timestamp = source.row_updated_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    x_location,
    y_location,
    row_created_timestamp,
    row_updated_timestamp
  )
  VALUES (
    source.customer_id,
    source.x_location,
    source.y_location,
    source.row_created_timestamp,
    source.row_updated_timestamp
  )
    """).show(truncate=False)

# Write Stream with foreachBatch
query = kafka_df.writeStream \
.foreachBatch(foreach_batch_function) \
.option("checkpointLocation", CHECKPOINT_PATH) \
.trigger(processingTime='5 seconds') \
.start()

query.awaitTermination()
