# Task 1 operator interface. Run `make` (or `make help`) to list targets.
# Environment-specific settings are variables with local defaults; override per run, e.g.
#   make bronze DATE=2026-10-16 SPARK_MASTER=local[4]      make up ENV_FILE=infra/compose/.env.other
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c          # a pipeline fails if any command in it fails (not only the last one)
.DEFAULT_GOAL := help

ENV_FILE            ?= infra/compose/.env
DATE                ?= $(shell date +%F)
SPARK_MASTER        ?= local[2]
SPARK_DRIVER_MEMORY ?= 2g
VALIDATE_ARGS       ?=
K8S_ENV_FILE        ?= infra/k8s/secrets.env

COMPOSE      = docker compose -f infra/compose/docker-compose.yml --env-file $(ENV_FILE)
SPARK_SUBMIT = $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master $(SPARK_MASTER) --driver-memory $(SPARK_DRIVER_MEMORY)
SPARK_SQL    = $(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1]
NOISE        = ^Time taken|^:: loading settings

.PHONY: help build secrets up down clean ps logs stats sql smoke bronze validate acceptance acceptance-from-zero k8s-secrets

help:          ## list targets and variables
	@awk -F':.*## ' '/^[a-zA-Z_-]+:.*## /{printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo "  variables: ENV_FILE=$(ENV_FILE) DATE=$(DATE) SPARK_MASTER=$(SPARK_MASTER) SPARK_DRIVER_MEMORY=$(SPARK_DRIVER_MEMORY)"

build:         ## build the 2 thin runtime images (once per version change)
	$(COMPOSE) build
secrets:       ## render credential files from ENV_FILE into infra/compose/secrets/ (mode 600)
	python3 -I infra/compose/render-secrets.py $(ENV_FILE)
up: secrets    ## render secrets, then start MinIO, Postgres, Hive Metastore, Spark runner (also the fix after a reboot)
	$(COMPOSE) up -d --remove-orphans
down:          ## stop (keeps data volumes)
	$(COMPOSE) down
clean:         ## stop AND delete data volumes (destructive)
	$(COMPOSE) down -v
ps:            ## container status
	$(COMPOSE) ps -a --format "table {{.Service}}\t{{.Status}}"
logs:          ## follow a service log: make logs SVC=hive-metastore
	$(COMPOSE) logs -f $(SVC)
stats:         ## memory/CPU per container
	docker stats --no-stream
sql:           ## interactive Spark SQL against the lake catalog
	$(COMPOSE) exec spark /opt/spark/bin/spark-sql --master $(SPARK_MASTER)

smoke:         ## end-to-end check: Spark -> Iceberg -> HMS -> Postgres (fails if the catalog does not answer)
	$(SPARK_SQL) -e "SHOW NAMESPACES IN lake" < /dev/null 2>/dev/null | grep -x "default"
	@echo "[smoke] OK"
bronze:        ## load (or reload) one ingest date: make bronze DATE=2026-10-16
	$(SPARK_SUBMIT) /opt/jobs/bronze_ingest.py --ingest-date $(DATE) < /dev/null 2>&1 | grep -E "^\[bronze\]|Error|Exception|Caused by"
validate:      ## assert Bronze for DATE against tests/expected/bronze.json (exit 1 on any FAIL)
	$(SPARK_SUBMIT) /opt/jobs/validate_bronze.py --ingest-date $(DATE) --expected /opt/tests/expected/bronze.json --sql /opt/tests/sql/bronze_checks.sql $(VALIDATE_ARGS) < /dev/null 2>&1 | grep -E "^\[validate\]|Error|Exception|Caused by"
acceptance:    ## smoke -> load -> validate -> reload -> validate + idempotency (keeps data; exit 1 on failure)
	$(MAKE) --no-print-directory smoke
	$(MAKE) --no-print-directory bronze DATE=$(DATE)
	$(MAKE) --no-print-directory validate DATE=$(DATE)
	$(MAKE) --no-print-directory bronze DATE=$(DATE)
	$(MAKE) --no-print-directory validate DATE=$(DATE) VALIDATE_ARGS=--check-idempotency
	@echo "[acceptance] PASSED for _ingest_date=$(DATE)"
acceptance-from-zero: ## DESTRUCTIVE: clean -> up -> acceptance (needs CONFIRM=yes)
	@test "$(CONFIRM)" = yes || { echo "refusing: this deletes all data volumes; run: make acceptance-from-zero CONFIRM=yes DATE=..."; exit 1; }
	$(MAKE) --no-print-directory clean
	$(MAKE) --no-print-directory up
	$(MAKE) --no-print-directory acceptance DATE=$(DATE)

# ---- Task 2 (Minikube) ----
k8s-secrets:   ## Task 2: render K8s secret files from K8S_ENV_FILE into infra/k8s/secrets/ (mode 600)
	python3 -I infra/k8s/render-secrets.py $(K8S_ENV_FILE)
