.PHONY: up up-all down logs report test dbt-parse

up:            ## start Kafka, MinIO, producer and the Spark streaming job
	docker compose up -d --build

up-all:        ## ... plus Airflow (http://localhost:8088, admin/admin)
	docker compose --profile orchestration up -d --build

down:          ## stop everything and delete volumes (resets the lake)
	docker compose --profile orchestration down -v

logs:
	docker compose logs -f spark-streaming producer

report:        ## wait until bronze/silver/gold have data and Silver has no duplicates
	docker compose exec spark-streaming python -m lakehouse.report --wait --timeout 900

test:
	python -m pytest -q

dbt-parse:
	cd dbt_transformations && dbt deps && dbt parse
