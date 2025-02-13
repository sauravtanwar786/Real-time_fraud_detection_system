import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3a://commerce/warehouse/"  # S3 Address to Write to
KAFKA_BROKERS = 'kafka:9092'
S3_ENDPOINT = "http://minio:9000"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/debezium.payment.transactions/"

# S3 Credentials
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"

conf = (
    pyspark.SparkConf()
        .setAppName('transactions')
        # Packages
        .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,'
                                    'org.apache.hadoop:hadoop-aws:3.3.2,'
                                    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                                    'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1')
        # SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                                     'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        # Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.s3.secret.region', 'us-east-1')

        # S3 Configuration
        .set('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
        .set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .set('spark.hadoop.fs.s3a.path.style.access', 'true')
        .set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')  # Disable SSL if using MinIO locally
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

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
create database if not exists nessie.payment
""").show(truncate=False)

spark.sql("""
      CREATE TABLE IF NOT EXISTS nessie.payment.transactions (
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
    MERGE INTO nessie.payment.transactions AS target
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