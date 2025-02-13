import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, array
from pyspark.sql.types import DoubleType
import pickle
import pandas as pd
import time

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3a://commerce/warehouse/"  # S3 Address to Write to
KAFKA_BROKERS = 'kafka:9092'
S3_ENDPOINT = "http://minio:9000"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/debezium.payment.customers/"

# S3 Credentials
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"

conf = (
    pyspark.SparkConf()
        .setAppName('customers')
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

# Load the pre-trained model
MODEL_PATH = "s3://commerce/trained_model.pkl"
LOCAL_MODEL_PATH = "/tmp/trained_model.pkl"
input_table = "nessie.payment.transactions"
output_table = "nessie.payment.analyzed_transactions"
CHECKPOINT_PATH = "s3a://commerce/checkpoints/payment.analyzed_transactions/"
# Download the model from s3 and then load it
import boto3
import botocore

s3 = boto3.resource('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY)

try:
    s3.Bucket("commerce").download_file("trained_model.pkl", LOCAL_MODEL_PATH)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

with open(LOCAL_MODEL_PATH, "rb") as f:
    model = pickle.load(f)

# Define a Pandas UDF for inference
@pandas_udf(DoubleType())  # Assuming model outputs a float
def predict_udf(*cols: pd.Series) -> pd.Series:
    features = pd.concat(cols, axis=1)  # Combine all feature columns
    return pd.Series(model.predict(features))

# Read streaming data from Iceberg table and  pass option of stream-from-timestamp as the current timestamp value in  unix miliseconds

df = spark.readStream \
    .format("iceberg") \
    .option("stream-from-timestamp", int(time.time() * 1000)) \
    .load(input_table)

# # join the data with the feature_customer table and with feature_temrinal table basis cusotmer_id and terminal_id
# df = df.join(spark.table("nessie.payment.feature_customer"), df.customer_id == col("customer_id"), "left") \
#     .join(spark.table("nessie.payment.feature_terminal"), df.terminal_id == col("terminal_id"), "left")

# # compute two more features basis whether transaction happended on weekend or not and whether transaction happended on night or not basis column tx_datetime that is timestamp column
# df = df.withColumn("is_weekend", (col("tx_datetime").cast("timestamp").dayofweek >= 5).cast("int")) \
#     .withColumn("is_night", (col("tx_datetime").cast("timestamp").hour >= 20).cast("int"))

# write all the above transformations in a sql
df.createOrReplaceTempView("latest_transactions")

enriched_df = spark.sql("""
select
    lt.*,
    if(dayofweek(tx_datetime) >= 5, 1, 0) as is_weekend,
    if(hour(tx_datetime) >= 20, 1, 0) as is_night,
    customer_id_nb_tx_1day_window,
      customer_id_avg_amount_1day_window,
      customer_id_nb_tx_7day_window,
      customer_id_avg_amount_7day_window,
      customer_id_nb_tx_30day_window,
      customer_id_avg_amount_30day_window,
      terminal_id_nb_tx_1day_window,
      terminal_id_risk_1day_window,
      terminal_id_nb_tx_7day_window,
      terminal_id_risk_7day_window,
      terminal_id_nb_tx_30day_window,
      terminal_id_risk_30day_window,
      current_timestamp() as processed_at
from latest_transactions lt 
left join nessie.payment.feature_customer fc
on lt.customer_id = fc.customer_id
left join nessie.payment.feature_terminal ft
on lt.terminal_id = ft.terminal_id
""")



# feature columns are 'TX_AMOUNT','TX_DURING_WEEKEND', 'TX_DURING_NIGHT', 'CUSTOMER_ID_NB_TX_1DAY_WINDOW',
    #    'CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW', 'CUSTOMER_ID_NB_TX_7DAY_WINDOW',
    #    'CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW', 'CUSTOMER_ID_NB_TX_30DAY_WINDOW',
    #    'CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW', 'TERMINAL_ID_NB_TX_1DAY_WINDOW',
    #    'TERMINAL_ID_RISK_1DAY_WINDOW', 'TERMINAL_ID_NB_TX_7DAY_WINDOW',
    #    'TERMINAL_ID_RISK_7DAY_WINDOW', 'TERMINAL_ID_NB_TX_30DAY_WINDOW',
    #    'TERMINAL_ID_RISK_30DAY_WINDOW'

# make a list of feature columns in small case
feature_columns = ['tx_amount', 'is_weekend', 'is_night', 'customer_id_nb_tx_1day_window',
                   'customer_id_avg_amount_1day_window', 'customer_id_nb_tx_7day_window',
                   'customer_id_avg_amount_7day_window', 'customer_id_nb_tx_30day_window',
                   'customer_id_avg_amount_30day_window', 'terminal_id_nb_tx_1day_window',
                   'terminal_id_risk_1day_window', 'terminal_id_nb_tx_7day_window',
                   'terminal_id_risk_7day_window', 'terminal_id_nb_tx_30day_window',
                   'terminal_id_risk_30day_window']

# Apply the model for inference
df = enriched_df.withColumn("prediction", predict_udf(*[col(f) for f in feature_columns]))

# Write results to another Iceberg table

spark.sql("""
    create table if not exists nessie.payment.analyzed_transactions (
        tx_id INT,
                    tx_datetime Timestamp,
                    customer_id INT,
                    terminal_id INT,
                    tx_amount DECIMAL(10,2),
    row_created_timestamp TIMESTAMP,
    row_updated_timestamp TIMESTAMP,
    is_weekend INT,
    is_night INT,
    customer_id_nb_tx_1day_window INT,
    customer_id_avg_amount_1day_window DOUBLE,
    customer_id_nb_tx_7day_window INT,
    customer_id_avg_amount_7day_window DOUBLE,
    customer_id_nb_tx_30day_window INT,
    customer_id_avg_amount_30day_window DOUBLE,
    terminal_id_nb_tx_1day_window INT,
    terminal_id_risk_1day_window DOUBLE,
    terminal_id_nb_tx_7day_window INT,
    terminal_id_risk_7day_window DOUBLE,
    terminal_id_nb_tx_30day_window INT,
    terminal_id_risk_30day_window DOUBLE,
    processed_at TIMESTAMP,
    prediction DOUBLE
    )
    using iceberg
          """).show()

query = df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start(output_table)

query.awaitTermination()
