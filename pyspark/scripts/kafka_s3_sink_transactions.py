from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

KAFKA_BROKERS = 'kafka:9092'
#KAFKA_TOPIC_NAME = "debezium.payment.transactions"
S3_ENDPOINT = "http://minio:9000"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/debezium.payment.transactions/"

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
                .appName("streaming_sink_transactions")
         .getOrCreate())

# Read data from Kafka (batch mode)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "debezium.payment.transactions") \
    .option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType,BinaryType
from pyspark.sql.types import StructType, StructField, IntegerType, BinaryType, StringType, LongType, DecimalType
from pyspark.sql.functions import udf, col, from_json
from decimal import Decimal

# Decode bytes to Decimal
def decode_decimal(bytes_value):
    if bytes_value is None:
        return None
    scale = 2  # Defined in the Debezium schema
    precision = 10
    # Decode the bytes to integer and apply scale
    unscaled_value = int.from_bytes(bytes_value, byteorder="big", signed=True)
    return Decimal(unscaled_value) / (10 ** scale)

decode_decimal_udf=spark.udf.register("decode_decimal_udf",decode_decimal,  DecimalType(precision=10, scale=2))
# Register the UDF
#decode_decimal_udf = udf(decode_decimal, DecimalType(precision=10, scale=2))
# Define the schema for the JSON messages
root_schema = StructType([
    StructField("schema", StructType([
        StructField("type", StringType(), False),
        StructField("fields", StringType(), True),  # Simplified for nested schema
        StructField("optional", StringType(), True),
        StructField("name", StringType(), True),
        StructField("version", IntegerType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("tx_id", IntegerType(), True),
            StructField("tx_datetime", LongType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("terminal_id", IntegerType(), True),
            StructField("tx_amount", BinaryType(), True)  # Use StringType to handle Base64-encoded Decimal
        ]), True),
        StructField("after", StructType([
            StructField("tx_id", IntegerType(), True),
            StructField("tx_datetime", LongType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("terminal_id", IntegerType(), True),
            StructField("tx_amount", BinaryType(), True) 
        ]), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("ts_us", LongType(), True),
            StructField("ts_ns", LongType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), True),
        StructField("transaction", StructType([
            StructField("id", StringType(), True),
            StructField("total_order", LongType(), True),
            StructField("data_collection_order", LongType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True)
    ]))
])

from pyspark.sql.types import StructType,DecimalType, StructField, IntegerType, DoubleType, StringType, LongType,BinaryType

from pyspark.sql.functions import from_json

kafka_df = kafka_df.withColumn("value", col("value").cast(StringType())) \
    .withColumn("key", col("key").cast(StringType()))
# Apply the schema to the DataFrame
kafka_df = kafka_df.withColumn("value", from_json("value", root_schema))


# Create a temporary view for further SQL operations


kafka_df.createOrReplaceTempView("kafkadata")

spark.sql("""
create database if not exists payment
""").show(truncate=False)

spark.sql("""
      CREATE TABLE IF NOT EXISTS payment.transactions (
                    tx_id INT,
                    tx_datetime Timestamp,
                    customer_id INT,
                    terminal_id INT,
                    tx_amount DECIMAL(10,2),
    row_created_timestamp TIMESTAMP,
    row_updated_timestamp TIMESTAMP
)
USING iceberg
""")

def foreach_batch_function(df, epoch_id):
    spark = df.sparkSession
    df.createOrReplaceTempView('micro_batch')
    latest_data = spark.sql(""" 
              with extracted_data  as (
                    select 
                    value.payload.after.tx_id,
                    to_timestamp(from_unixtime(value.payload.after.tx_datetime/1000000)) AS tx_datetime, -- Conversion from Unix to Timestamp
                    value.payload.after.customer_id,
                    value.payload.after.terminal_id,
                     decode_decimal_udf(value.payload.after.tx_amount) AS tx_amount, -- Conversion to DECIMAL(10,2)
                    timestamp
                    from micro_batch),
              ranked_data as (
                  select tx_id,
                tx_datetime,
                customer_id,
                terminal_id,
                tx_amount,
                  timestamp,
                  ROW_NUMBER() over (partition by tx_id order by timestamp DESC) as rn
                  from extracted_data) 
                select tx_id,
                    tx_datetime,
                    customer_id,
                    terminal_id,
                    tx_amount,
                  now() as row_created_timestamp,
                  now() as row_updated_timestamp
              from ranked_data 
              where rn=1""")
    latest_data.createOrReplaceTempView('latest_data')
    #insert data
    spark.sql("""
    MERGE INTO payment.transactions AS target
    USING latest_data AS source
ON target.tx_id = source.tx_id
WHEN MATCHED THEN
  UPDATE SET
    target.tx_datetime = source.tx_datetime,
    target.customer_id = source.customer_id,
    target.terminal_id = source.terminal_id,
    target.tx_amount = source.tx_amount,      
    target.row_updated_timestamp = source.row_updated_timestamp
WHEN NOT MATCHED THEN
  INSERT (          tx_id,
                    tx_datetime,
                    customer_id,
                    terminal_id,
                    tx_amount,
    row_created_timestamp,
    row_updated_timestamp
  )
  VALUES (
    source.tx_id,
    source.tx_datetime,
    source.customer_id,
    source.terminal_id,
    source.tx_amount,
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