up:
	docker compose up -d --build

remove-minio-data:
	rm -rf ./minio/data

compose-down:
	docker compose down -v

down: compose-down remove-minio-data

restart: down up

minio-ui:
	open http://localhost:9001

pg:
	pgcli -h localhost -p 5432 -U postgres -d postgres

pg-src:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '@./connect/pg-src-connector.json'

s3-sink:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '@./connect/s3-sink.json'

connectors: pg-src s3-sink