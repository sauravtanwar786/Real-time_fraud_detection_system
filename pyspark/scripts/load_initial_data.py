import os
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import current_timestamp

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3a://commerce/warehouse/"  # S3 Address to Write to
S3_ENDPOINT = "http://minio:9000"
BASE_DATA_PATH = "/opt/data/raw/"  # Base Path to Data

# S3 Credentials
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"

# Define the start and end dates
TRANSACTIONS_START_DATE = "2025-01-01"
TRANSACTIONS_END_DATE = "2025-01-30"

FEATURES_COMPUTATION_DATE = "2025-01-31"

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


def load_customers():
  global spark
  global BASE_DATA_PATH
  customer_profiles_table=pd.read_pickle(f"{BASE_DATA_PATH}customer.pkl")

  # Select specific columns
  customer_sdf = customer_profiles_table[["CUSTOMER_ID", "x_customer_id", "y_customer_id"]]

  customer_sdf = spark.createDataFrame(customer_sdf)
  customer_sdf = customer_sdf.withColumnRenamed("CUSTOMER_ID","customer_id") \
                              .withColumnRenamed("x_customer_id","x_location") \
                              .withColumnRenamed("y_customer_id","y_location")

  customer_sdf=customer_sdf.withColumn("row_created_timestamp", current_timestamp()) \
  .withColumn("row_updated_timestamp", current_timestamp())

  customer_sdf.createOrReplaceTempView("customer_sdf")

  spark.sql("create database if not exists nessie.payment").show()

  spark.sql("""
  CREATE TABLE IF NOT EXISTS nessie.payment.customer (
      customer_id INT,
      x_location FLOAT,
      y_location FLOAT,
      row_created_timestamp TIMESTAMP,
      row_updated_timestamp TIMESTAMP
  )
  USING iceberg
  """).show(truncate=False)

  spark.sql("""
  MERGE INTO nessie.payment.customer AS target
  USING customer_sdf AS source
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
  """)

  del customer_sdf
  del customer_profiles_table
  print("Customer Data Loaded")

def load_terminal():
  global spark
  global BASE_DATA_PATH
  terminal_profiles_table=pd.read_pickle(f"{BASE_DATA_PATH}temrinal.pkl")
  terminal_sdf=spark.createDataFrame(terminal_profiles_table)
  terminal_sdf=terminal_sdf.withColumnRenamed("TERMINAL_ID","terminal_id")

  terminal_sdf=terminal_sdf.withColumnRenamed("x_terminal_id","x_location")
  terminal_sdf=terminal_sdf.withColumnRenamed("y_terminal_id","y_location")

  terminal_sdf=terminal_sdf.withColumn("row_created_timestamp", current_timestamp())
  terminal_sdf=terminal_sdf.withColumn("row_updated_timestamp", current_timestamp())

  terminal_sdf.createOrReplaceTempView("terminal_sdf")

  spark.sql("""
          CREATE TABLE IF NOT EXISTS nessie.payment.terminal (
        terminal_id INT,
        x_location FLOAT,
        y_location FLOAT,
        row_created_timestamp TimeStamp,
        row_updated_timestamp TimeStamp
  )
  USING iceberg
  """)

  spark.sql("""
  MERGE INTO nessie.payment.terminal AS target
  USING terminal_sdf AS source
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
  """)

  del terminal_sdf
  del terminal_profiles_table
  print("Terminal Data Loaded")


def load_transactions():
  global spark
  global BASE_DATA_PATH
  global TRANSACTIONS_START_DATE
  global TRANSACTIONS_END_DATE

  # Path to the folder containing the pickle files
  folder_path = f"{BASE_DATA_PATH}transaction/"

  # List all pickle files in the folder
  pickle_files = [f for f in os.listdir(folder_path) if f.endswith(".pkl")]

  # Filter files within the date range
  filtered_files = [
      f for f in pickle_files
      if TRANSACTIONS_START_DATE <= f.split(".pkl")[0] <= TRANSACTIONS_END_DATE
  ]

  # Initialize an empty list to store DataFrames
  dataframes = []

  # Iterate through each filtered pickle file and load it into a DataFrame
  for pickle_file in filtered_files:
      file_path = os.path.join(folder_path, pickle_file)
      df = pd.read_pickle(file_path)
      dataframes.append(df)

  # Concatenate all DataFrames into one
  transactions_df = pd.concat(dataframes, ignore_index=True)
  transactions_df=transactions_df[["TRANSACTION_ID","TX_DATETIME","CUSTOMER_ID","TERMINAL_ID","TX_AMOUNT"]]

  transactions_sdf=spark.createDataFrame(transactions_df)
  t1=transactions_sdf.withColumnRenamed("TRANSACTION_ID","tx_id") \
  .withColumnRenamed("TX_DATETIME","tx_datetime") \
  .withColumnRenamed("CUSTOMER_ID","customer_id") \
  .withColumnRenamed("TERMINAL_ID","terminal_id") \
  .withColumnRenamed("TX_AMOUNT","tx_amount")

  transactions_sdf1=t1.withColumn("row_created_timestamp", current_timestamp()) \
  .withColumn("row_updated_timestamp", current_timestamp())

  transactions_sdf1.createOrReplaceTempView("transaction1")

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
  PARTITIONED  by (date(tx_datetime))
  """).show()

  spark.sql("""
    MERGE INTO nessie.payment.transactions AS target
    USING transaction1 AS source
ON  date(target.tx_datetime) = date(source.tx_datetime) and target.tx_id = source.tx_id
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
  """)

  del transactions_sdf
  del transactions_df
  print("Transaction Data Loaded")

def load_model():

  # Upload the /data/trained_model.pkl file s3://commerce/trained_model.pkl
  import boto3
  import botocore

  s3 = boto3.resource('s3',
                      endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY)

  try:
      s3.meta.client.upload_file(f"{BASE_DATA_PATH}../trained_model.pkl", "commerce", "trained_model.pkl")
      print("Model uploaded successfully")
  except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
          print("The object does not exist.")
      else:
          raise

def load_customer_features():
  global spark
  global FEATURES_COMPUTATION_DATE

  data = pd.read_pickle(f"{BASE_DATA_PATH}../processed/feature_customer.pkl")
  data = data[["customer_id", "customer_id_nb_tx_1day_window", "customer_id_avg_amount_1day_window",
               "customer_id_nb_tx_7day_window", "customer_id_avg_amount_7day_window",
               "customer_id_nb_tx_30day_window", "customer_id_avg_amount_30day_window"]]
  data["dt"] = FEATURES_COMPUTATION_DATE
  data["row_created_timestamp"] = pd.Timestamp.now()
  data["row_updated_timestamp"] = pd.Timestamp.now()

  latest_data = spark.createDataFrame(data)

  # cast dt column to appropiate data type
  latest_data = latest_data.withColumn("dt", latest_data["dt"].cast("date"))
                                       

  latest_data.createOrReplaceTempView("latest_data")

  # spark.sql(f"""create or replace temp view latest_data as 
  # WITH one_day_window AS (
  #     SELECT  
  #         customer_id,
  #         COUNT(*) AS customer_id_nb_tx_1day_window,
  #         AVG(tx_amount) AS customer_id_avg_amount_1day_window
  #     FROM nessie.payment.transactions
  #     WHERE date(tx_datetime) = date'{FEATURES_COMPUTATION_DATE}' - 1
  #     GROUP BY customer_id
  # ),
  # seven_day_window AS (
  #     SELECT  
  #         customer_id,
  #         COUNT(*) AS customer_id_nb_tx_7day_window,
  #         AVG(tx_amount) AS customer_id_avg_amount_7day_window
  #     FROM nessie.payment.transactions
  #     WHERE date(tx_datetime) BETWEEN date'{FEATURES_COMPUTATION_DATE}' - 7 AND date'{FEATURES_COMPUTATION_DATE}' - 1
  #     GROUP BY customer_id
  # ),
  # thirty_day_window AS (
  #     SELECT  
  #         customer_id,
  #         COUNT(*) AS customer_id_nb_tx_30day_window,
  #         AVG(tx_amount) AS customer_id_avg_amount_30day_window
  #     FROM nessie.payment.transactions
  #     WHERE date(tx_datetime) BETWEEN date'{FEATURES_COMPUTATION_DATE}' - 30 AND date'{FEATURES_COMPUTATION_DATE}' - 1
  #     GROUP BY customer_id
  # )
  # SELECT 
  #     one_day.customer_id,
  #     current_date AS dt,
  #     one_day.customer_id_nb_tx_1day_window,
  #     one_day.customer_id_avg_amount_1day_window,
  #     seven_day.customer_id_nb_tx_7day_window,
  #     seven_day.customer_id_avg_amount_7day_window,
  #     thirty_day.customer_id_nb_tx_30day_window,
  #     thirty_day.customer_id_avg_amount_30day_window,
  #     current_timestamp() AS row_created_timestamp,
  #     current_timestamp() AS row_updated_timestamp
  # FROM one_day_window AS one_day
  # LEFT JOIN seven_day_window AS seven_day
  #     ON one_day.customer_id = seven_day.customer_id 
  # LEFT JOIN thirty_day_window AS thirty_day
  #     ON one_day.customer_id = thirty_day.customer_id
  # """).show()


  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS nessie.payment.feature_customer (
            dt DATE,
            customer_id INT,
            customer_id_nb_tx_1day_window INT,
            customer_id_avg_amount_1day_window DOUBLE,
            customer_id_nb_tx_7day_window INT,
            customer_id_avg_amount_7day_window DOUBLE,
            customer_id_nb_tx_30day_window INT,
            customer_id_avg_amount_30day_window DOUBLE,
      row_created_timestamp TIMESTAMP,
      row_updated_timestamp TIMESTAMP
  )
  USING iceberg
  """).show()

  spark.sql(f"""
  MERGE INTO nessie.payment.feature_customer AS target
  USING latest_data AS source
  ON target.customer_id = source.customer_id and target.dt = source.dt
  WHEN MATCHED THEN
    UPDATE SET
      target.customer_id_nb_tx_1day_window = source.customer_id_nb_tx_1day_window,
      target.customer_id_avg_amount_1day_window = source.customer_id_avg_amount_1day_window,
      target.customer_id_nb_tx_7day_window = source.customer_id_nb_tx_7day_window,
      target.customer_id_avg_amount_7day_window = source.customer_id_avg_amount_7day_window,
      target.customer_id_nb_tx_30day_window = source.customer_id_nb_tx_30day_window,
      target.customer_id_avg_amount_30day_window = source.customer_id_avg_amount_30day_window,
      target.row_updated_timestamp = source.row_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
      dt,
      customer_id,
      customer_id_nb_tx_1day_window,
      customer_id_avg_amount_1day_window,
      customer_id_nb_tx_7day_window,
      customer_id_avg_amount_7day_window,
      customer_id_nb_tx_30day_window,
      customer_id_avg_amount_30day_window,
      row_created_timestamp,
      row_updated_timestamp
    )
    VALUES (
      source.dt,
      source.customer_id,
      source.customer_id_nb_tx_1day_window,
      source.customer_id_avg_amount_1day_window,
      source.customer_id_nb_tx_7day_window,
      source.customer_id_avg_amount_7day_window,
      source.customer_id_nb_tx_30day_window,
      source.customer_id_avg_amount_30day_window,
      source.row_created_timestamp,
      source.row_updated_timestamp
    )
  """).show()

  print("Feature Data Loaded")


def load_terminal_features():
  global spark
  global FEATURES_COMPUTATION_DATE

  global spark
  global FEATURES_COMPUTATION_DATE

  data = pd.read_pickle(f"{BASE_DATA_PATH}../processed/feature_terminal.pkl")
  data = data[['terminal_id', 'terminal_id_nb_tx_1day_window',
       'terminal_id_risk_1day_window', 'terminal_id_nb_tx_7day_window',
       'terminal_id_risk_7day_window', 'terminal_id_nb_tx_30day_window',
       'terminal_id_risk_30day_window']]
  data["row_created_timestamp"] = pd.Timestamp.now()
  data["row_updated_timestamp"] = pd.Timestamp.now()

  latest_data = spark.createDataFrame(data)

  latest_data.createOrReplaceTempView("latest_data")

  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS nessie.payment.feature_terminal (
      terminal_id INT,
      terminal_id_nb_tx_1day_window INT,
      terminal_id_risk_1day_window DOUBLE,
      terminal_id_nb_tx_7day_window INT,
      terminal_id_risk_7day_window DOUBLE,
      terminal_id_nb_tx_30day_window INT,
      terminal_id_risk_30day_window DOUBLE,
      row_created_timestamp TIMESTAMP,
      row_updated_timestamp TIMESTAMP
  )
  USING iceberg
  """).show()

  spark.sql(f"""
  MERGE INTO nessie.payment.feature_terminal AS target
  USING latest_data AS source
            on target.terminal_id = source.terminal_id
  WHEN MATCHED THEN
    UPDATE SET
      target.terminal_id_nb_tx_1day_window = source.terminal_id_nb_tx_1day_window,
      target.terminal_id_risk_1day_window = source.terminal_id_risk_1day_window,
      target.terminal_id_nb_tx_7day_window = source.terminal_id_nb_tx_7day_window,
      target.terminal_id_risk_7day_window = source.terminal_id_risk_7day_window,
      target.terminal_id_nb_tx_30day_window = source.terminal_id_nb_tx_30day_window,
      target.terminal_id_risk_30day_window = source.terminal_id_risk_30day_window,
      target.row_updated_timestamp = source.row_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
      terminal_id,
      terminal_id_nb_tx_1day_window,
      terminal_id_risk_1day_window,
      terminal_id_nb_tx_7day_window,
      terminal_id_risk_7day_window,
      terminal_id_nb_tx_30day_window,
      terminal_id_risk_30day_window,
      row_created_timestamp,
      row_updated_timestamp
    )
    VALUES (
      source.terminal_id,
      source.terminal_id_nb_tx_1day_window,
      source.terminal_id_risk_1day_window,
      source.terminal_id_nb_tx_7day_window,
      source.terminal_id_risk_7day_window,
      source.terminal_id_nb_tx_30day_window,
      source.terminal_id_risk_30day_window,
      source.row_created_timestamp,
      source.row_updated_timestamp
    )
  """).show()

  print("Feature terminal Data Loaded")


if __name__ == "__main__":
  try:
    load_customers()
    load_terminal()
    #load_transactions()
    load_model()
    load_customer_features()
    load_terminal_features()
  except Exception as e:
    print(f"An error occurred: {e}")
  finally:
    spark.stop()