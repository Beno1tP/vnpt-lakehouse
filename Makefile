COMPOSE = docker compose -f infra/compose/docker-compose.yml --env-file infra/compose/.env
DATE ?= $(shell date +%F)

up: secrets    ## render secrets, then start MinIO, Postgres, Hive Metastore, Spark runner
	$(COMPOSE) up -d
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
build:         ## build the 2 thin runtime images (once per version change)
	$(COMPOSE) build
secrets:       ## render credential files from .env into infra/compose/secrets/ (mode 600)
	python3 -I infra/compose/render-secrets.py
smoke:         ## end-to-end check: Spark -> Iceberg -> HMS -> Postgres
	$(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1] -e "SHOW NAMESPACES IN lake" < /dev/null 2>/dev/null | grep -v "^Time taken"
