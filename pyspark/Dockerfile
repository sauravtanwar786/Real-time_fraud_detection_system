FROM bitnami/spark:3.5.4

# Switch to root for installation purposes
USER root

# Install required packages
RUN install_packages curl python3-pip

# Install Python dependencies
RUN pip install --no-cache-dir pandas boto3 scikit-learn pyarrow

# Reset to the default non-root user
USER 1001

# Define variables for JAR versions and paths for maintainability
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars
ENV SPARK_KAFKA_JAR_VERSION=3.3.1
ENV ICEBERG_SPARK_VERSION=1.7.1
ENV HADOOP_AWS_VERSION=3.3.2
ENV COMMONS_POOL2_VERSION=2.11.1

# Download Spark Kafka JAR
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_JAR_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_JAR_VERSION}.jar \
    --output ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_JAR_VERSION}.jar

# Download Iceberg runtime JAR
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_SPARK_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_VERSION}.jar \
    --output ${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_VERSION}.jar

# Download Hadoop AWS JAR
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    --output ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar --output /opt/bitnami/spark/jars/kafka-clients-3.3.1.jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar --output /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar

# Download commons-pool2 JAR
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
    --output ${SPARK_JARS_DIR}/commons-pool2-${COMMONS_POOL2_VERSION}.jar
RUN curl -fsSL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar \
 --output ${SPARK_JARS_DIR}/nessie-spark-extensions-3.5_2.12-0.77.1.jar