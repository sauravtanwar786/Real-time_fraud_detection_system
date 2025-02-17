# Commands
up:
	docker compose up -d --build

remove-minio-data:
	sudo rm -rf ./volume/minio

compose-down:
	docker compose down

down: compose-down remove-minio-data

restart: down up

minio-ui:
	open http://localhost:9001

pg:
	pgcli -h localhost -p 5432 -U postgres -d postgres

pg-src:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '@./connect/pg-src-connector.json'

connectors: pg-src

# Variables
SPARK_DOCKER_EXEC = docker exec -it pyspark
SPARK_JOBS_PATH = /opt/code
SPARK_SUBMIT = $(SPARK_DOCKER_EXEC) spark-submit \
    --master local[*] \
    --deploy-mode client \
    --driver-memory 10G \
    --executor-memory 6G \
    --executor-cores 4 \
    --jars /opt/bitnami/spark/jars/*

# Job targets
load_initial_data:
	$(SPARK_SUBMIT) $(SPARK_JOBS_PATH)/load_initial_data.py

fraud_detection:
	$(SPARK_SUBMIT) $(SPARK_JOBS_PATH)/fraud_detection.py

job1:
	$(SPARK_SUBMIT) $(SPARK_JOBS_PATH)/kafka_s3_sink_customers.py

job2:
	$(SPARK_SUBMIT) $(SPARK_JOBS_PATH)/kafka_s3_sink_terminals.py

job3:
	$(SPARK_SUBMIT) $(SPARK_JOBS_PATH)/kafka_s3_sink_transactions.py

# Run all jobs sequentially
run-all: job1 job2 job3

# Clean up
clean:
	docker compose down
