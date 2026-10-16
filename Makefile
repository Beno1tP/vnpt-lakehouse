COMPOSE = docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env
DATE ?= $(shell date +%F)

up:            ## build + start MinIO, Postgres, Hive Metastore, Spark runner
	$(COMPOSE) up -d --build
down:          ## stop (keeps data volumes)
	$(COMPOSE) down
clean:         ## stop AND delete data volumes (destructive)
	$(COMPOSE) down -v
ps:
	$(COMPOSE) ps
logs:          ## make logs SVC=hive-metastore
	$(COMPOSE) logs -f $(SVC)
bronze:        ## make bronze DATE=2026-10-15
	$(COMPOSE) exec spark /opt/spark/bin/spark-submit --master local[2] /opt/jobs/bronze_ingest.py --ingest-date $(DATE)
sql:           ## interactive Spark SQL against the lake catalog
	$(COMPOSE) exec spark /opt/spark/bin/spark-sql --master local[2]
stats:
	docker stats --no-stream
