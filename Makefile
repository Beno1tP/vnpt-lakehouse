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
K8S_RAW_DIR         ?= $(HOME)/data/vnpt-lab
JOB_TAG              = $(shell git log -1 --format=%h -- jobs/)
JOB_IMAGE            = vnpt-lake-spark-job:$(JOB_TAG)

COMPOSE      = docker compose -f infra/compose/docker-compose.yml --env-file $(ENV_FILE)
SPARK_SUBMIT = $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master $(SPARK_MASTER) --driver-memory $(SPARK_DRIVER_MEMORY)
SPARK_SQL    = $(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1]
NOISE        = ^Time taken|^:: loading settings

.PHONY: help build secrets up down clean ps logs stats sql smoke bronze validate acceptance acceptance-from-zero k8s-secrets landing k8s-job-image sparkapp

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
landing:       ## Task 2: split the Excel files by day -> s3://landing/<table>/dt=YYYY-MM-DD/ (one-off pod, deleted after)
	kubectl apply -f infra/k8s/spark-jobs/landing-simulator-pod.yaml
	kubectl -n spark-jobs wait --for=condition=Ready pod/landing-simulator --timeout=120s
	set -e; trap 'kubectl -n spark-jobs delete pod landing-simulator --wait=false' EXIT; \
	for f in bot_dvc_hcm.xlsx bot_ddvc_hcm_bot_predict.xlsx; do kubectl -n spark-jobs cp $(K8S_RAW_DIR)/$$f landing-simulator:/work/$$f; done; \
	kubectl -n spark-jobs cp jobs/landing_simulator.py landing-simulator:/work/landing_simulator.py; \
	kubectl -n spark-jobs cp tests/expected/bronze_daily.json landing-simulator:/work/bronze_daily.json; \
	kubectl -n spark-jobs exec landing-simulator -- python3 /work/landing_simulator.py --src /work --dest s3://landing --expected /work/bronze_daily.json
k8s-job-image: ## Task 2: build the job image (base + jobs/*.py, tag = last commit of jobs/) and load it into Minikube
	@git diff --quiet HEAD -- jobs/ || { echo "commit jobs/ first: the image tag is the last commit that changed jobs/"; exit 1; }
	docker build -f docker/spark-job/Dockerfile -t $(JOB_IMAGE) jobs/
	minikube image load $(JOB_IMAGE)
	rm -rf $(HOME)/.minikube/cache/images
	minikube image ls | grep vnpt-lake-spark-job
sparkapp:      ## Task 2: run the Bronze SparkApplication for one day by hand: make sparkapp DATE=2024-07-24
	kubectl -n spark-jobs delete sparkapplication bronze-$(DATE) --ignore-not-found --wait=true
	sed -e 's/__NAME__/bronze-$(DATE)/' -e 's/__DATE__/$(DATE)/g' -e 's/__IMAGE__/$(JOB_IMAGE)/' \
	    infra/k8s/spark-jobs/bronze-sparkapp.yaml | kubectl apply -f -
	@for i in $$(seq 1 120); do \
	  s=$$(kubectl -n spark-jobs get sparkapplication bronze-$(DATE) -o jsonpath='{.status.applicationState.state}'); \
	  echo "[sparkapp] $$(date +%T) state=$${s:-PENDING}"; \
	  case "$$s" in COMPLETED) break;; FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done
	kubectl -n spark-jobs logs bronze-$(DATE)-driver | grep -E "^\[bronze\]|Error|Exception|Caused by" || true
	@test "$$(kubectl -n spark-jobs get sparkapplication bronze-$(DATE) -o jsonpath='{.status.applicationState.state}')" = COMPLETED
