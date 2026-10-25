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
AIRFLOW_CHART        = $(HOME)/.cache/vnpt-lake/charts/airflow-1.22.0.tgz
AIRFLOW_CHART_SHA512 = 59a6b157949f37e96acd7351f795fd4347b2c4ac7f21d421542088b019874b202a80740389b71e0e9494fe6827674e596272297b493c419b03b263abb1a71c70
include infra/k8s/images.env
JOB_SRC              = jobs/ tests/ docker/spark-job/
JOB_TAG              = $(shell git log -1 --format=%h -- $(JOB_SRC))
APP                 ?= bronze
IDEMPOTENCY_SED      = -e 's|"/opt/tests/sql/bronze_rules.sql"]|"/opt/tests/sql/bronze_rules.sql", "--check-idempotency"]|'
LOCAL_JOB_IMAGE      = vnpt-lake-spark-job:$(JOB_TAG)
JOB_IMAGE           ?= $(SPARK_JOB_IMAGE_REF)

COMPOSE      = docker compose -f infra/compose/docker-compose.yml --env-file $(ENV_FILE)
SPARK_SUBMIT = $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master $(SPARK_MASTER) --driver-memory $(SPARK_DRIVER_MEMORY)
SPARK_SQL    = $(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1]
NOISE        = ^Time taken|^:: loading settings

.PHONY: help build secrets up down clean ps logs stats sql smoke bronze validate acceptance acceptance-from-zero k8s-secrets landing k8s-job-image s3-access-test sparkapp airflow-chart airflow-install airflow-admin airflow-ui

help:          ## list targets and variables
	@awk -F':.*## ' '/^[a-zA-Z0-9_-]+:.*## /{printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
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
	sed 's|__IMAGE__|$(JOB_IMAGE)|' infra/k8s/spark-jobs/landing-simulator-pod.yaml | kubectl apply -f -
	kubectl -n spark-jobs wait --for=condition=Ready pod/landing-simulator --timeout=600s
	set -e; trap 'kubectl -n spark-jobs delete pod landing-simulator --wait=false' EXIT; \
	for f in bot_dvc_hcm.xlsx bot_ddvc_hcm_bot_predict.xlsx; do kubectl -n spark-jobs cp $(K8S_RAW_DIR)/$$f landing-simulator:/work/$$f; done; \
	kubectl -n spark-jobs exec landing-simulator -- python3 /opt/jobs/landing_simulator.py --src /work --dest s3://landing \
	  --expected /opt/tests/expected/bronze_daily.json
s3-access-test: ## Task 2: prove least privilege of the MinIO users spark-jobs + landing-writer (allowed AND denied ops)
	kubectl -n spark-jobs create configmap s3-access-check --from-file=tests/k8s/s3_access_check.py --dry-run=client -o yaml | kubectl apply -f -
	@set -e; rc=0; for p in spark-jobs:s3-credentials landing-writer:landing-writer-credentials; do \
	  prof=$${p%%:*}; sec=$${p#*:}; \
	  kubectl -n spark-jobs delete pod s3-access-test-$$prof --ignore-not-found >/dev/null; \
	  sed -e "s|__PROFILE__|$$prof|g" -e "s|__SECRET__|$$sec|" -e 's|__IMAGE__|$(JOB_IMAGE)|' tests/k8s/s3-access-test-pod.yaml | kubectl apply -f - >/dev/null; \
	  kubectl -n spark-jobs wait --for=jsonpath='{.status.phase}'=Succeeded pod/s3-access-test-$$prof --timeout=120s >/dev/null || rc=1; \
	  kubectl -n spark-jobs logs s3-access-test-$$prof; kubectl -n spark-jobs delete pod s3-access-test-$$prof --wait=false >/dev/null; \
	done; exit $$rc
k8s-job-image: ## Task 2 FALLBACK (G6, CI blocked): build locally + load; then make sparkapp JOB_IMAGE=<LOCAL_JOB_IMAGE>
	@git diff --quiet HEAD -- $(JOB_SRC) || { echo "commit $(JOB_SRC) first: the image tag is their last commit"; exit 1; }
	docker build -f docker/spark-job/Dockerfile --build-context tests=tests -t $(LOCAL_JOB_IMAGE) jobs/
	minikube image load $(LOCAL_JOB_IMAGE)
	rm -rf $(HOME)/.minikube/cache/images
	minikube image ls | grep vnpt-lake-spark-job
	@echo "fallback image loaded: run make sparkapp JOB_IMAGE=$(LOCAL_JOB_IMAGE) DATE=..."
sparkapp:      ## Task 2: run one SparkApplication by hand (same template as the DAG): make sparkapp [APP=bronze|validate] DATE=2024-07-24 [IDEMPOTENCY=yes]
	kubectl -n spark-jobs delete sparkapplication $(APP)-$(DATE) --ignore-not-found --wait=true
	sed -e 's/{{ ds }}/$(DATE)/g' -e 's|{{ var.value.spark_job_image }}|$(JOB_IMAGE)|' \
	    $(if $(filter yes,$(IDEMPOTENCY)),$(IDEMPOTENCY_SED)) \
	    dags/templates/$(APP)_sparkapp.yaml | kubectl apply -f -
	@for i in $$(seq 1 120); do \
	  s=$$(kubectl -n spark-jobs get sparkapplication $(APP)-$(DATE) -o jsonpath='{.status.applicationState.state}'); \
	  echo "[sparkapp] $$(date +%T) state=$${s:-PENDING}"; \
	  case "$$s" in COMPLETED) break;; FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done
	kubectl -n spark-jobs logs $(APP)-$(DATE)-driver | grep -E "^\[(bronze|validate)\]|Error|Exception|Caused by" || true
	@test "$$(kubectl -n spark-jobs get sparkapplication $(APP)-$(DATE) -o jsonpath='{.status.applicationState.state}')" = COMPLETED
airflow-chart: ## Task 2: download Airflow chart 1.22.0 from the Apache CDN and check its SHA-512 (E29: helm repo index -> slow archive)
	mkdir -p $(dir $(AIRFLOW_CHART))
	test -f $(AIRFLOW_CHART) || curl -fL --retry 3 -o $(AIRFLOW_CHART) https://dlcdn.apache.org/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz
	echo "$(AIRFLOW_CHART_SHA512)  $(AIRFLOW_CHART)" | sha512sum --check
airflow-install: airflow-chart ## Task 2: helm upgrade --install Airflow 3.3.2 (chart 1.22.0); image = AIRFLOW_IMAGE_REF in images.env
	@test -n "$(AIRFLOW_IMAGE_REF)" || { echo "AIRFLOW_IMAGE_REF missing in infra/k8s/images.env (bump it after the images run)"; exit 1; }
	helm upgrade --install airflow $(AIRFLOW_CHART) -n airflow \
	  -f infra/helm/airflow/values-local.yaml \
	  --set images.airflow.digest=$(lastword $(subst @, ,$(AIRFLOW_IMAGE_REF))) \
	  --set env[0].name=AIRFLOW_VAR_SPARK_JOB_IMAGE --set env[0].value=$(SPARK_JOB_IMAGE_REF) \
	  --wait --timeout 15m
airflow-admin: ## Task 2: (re)create the Airflow UI user admin with the password from infra/k8s/secrets/airflow-admin-password
	@test -s infra/k8s/secrets/airflow-admin-password || { echo "run make k8s-secrets first"; exit 1; }
	-@kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow users delete --username admin >/dev/null 2>&1
	@kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow users create --username admin --role Admin \
	  --firstname admin --lastname lab --email admin@lab.local --password "$$(cat infra/k8s/secrets/airflow-admin-password)" \
	  | grep -v -i password
airflow-ui:    ## Task 2: open the Airflow UI on http://localhost:8080 (Ctrl+C to stop)
	kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
