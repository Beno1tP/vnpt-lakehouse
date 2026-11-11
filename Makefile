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
# Spark Operator chart 2.2.1 cached once (E58: GitHub Releases 504 broke make k8s-up). SHA-256 = GitHub's own asset digest
# (asset created 2025-06-27T00:17Z, never updated); the chart index lists cf223b6c… because it was computed from a
# second, non-byte-identical `helm package` 47 min later.
SPARK_OPERATOR_CHART        = $(HOME)/.cache/vnpt-lake/charts/spark-operator-2.2.1.tgz
# Trino chart 1.44.0 (appVersion 483): SHA-256 = GitHub asset digest = chart index digest (checked 2026-11-10)
TRINO_CHART        = $(HOME)/.cache/vnpt-lake/charts/trino-1.44.0.tgz
EVIDENCE_TRINO    ?= $(HOME)/data/vnpt-lab/evidence/task3-explore-$(shell date -u +%Y%m%dT%H%MZ).txt
TRINO_CHART_SHA256 = ce3c2f6525d896513c5e20cac19a1260bf108f1d17f65188be45ccea9fb36caa
# kube-starrocks chart 1.11.8 (operator 1.11.8; image tag default 4.1-latest, overridden): SHA-256 = GitHub asset digest
STARROCKS_CHART        = $(HOME)/.cache/vnpt-lake/charts/kube-starrocks-1.11.8.tgz
STARROCKS_CHART_SHA256 = c05804d0dc38719557c68e23713437e38b82be97db73799e59e1d5bc113c2286
SR_SQL = kubectl -n starrocks exec -i starrocks-fe-0 -- sh -c 'read -r p; MYSQL_PWD="$$p" exec mysql -h127.0.0.1 -P9030 -uroot --table'
SPARK_OPERATOR_CHART_SHA256 = 1c47c850c8d6ecc7eda4ede03e7932aa59f23fa6d186a10da3c8359df3671cf1
include infra/k8s/images.env
JOB_SRC              = jobs/ tests/ docker/spark-job/
JOB_TAG              = $(shell git log -1 --format=%h -- $(JOB_SRC))
APP                 ?= bronze
FROM                ?= 2024-07-02
TO                  ?= 2024-07-25
REPROCESS           ?= none
DRILL_DATE          ?= 2024-07-24
IDEMPOTENCY_SED      = -e 's|"/opt/tests/sql/bronze_rules.sql"]|"/opt/tests/sql/bronze_rules.sql", "--check-idempotency"]|'
LOCAL_JOB_IMAGE      = vnpt-lake-spark-job:$(JOB_TAG)
JOB_IMAGE           ?= $(SPARK_JOB_IMAGE_REF)
# Airflow CLI for make: in the api-server container, never in the scheduler (it runs the tasks; a CLI process there
# pushed it over its memory limit, E52)
AF                   = kubectl -n airflow exec deploy/airflow-api-server -c api-server --
AFI                  = kubectl -n airflow exec -i deploy/airflow-api-server -c api-server --

COMPOSE      = docker compose -f infra/compose/docker-compose.yml --env-file $(ENV_FILE)
SPARK_SUBMIT = $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master $(SPARK_MASTER) --driver-memory $(SPARK_DRIVER_MEMORY)
SPARK_SQL    = $(COMPOSE) exec -T spark /opt/spark/bin/spark-sql --master local[1]
NOISE        = ^Time taken|^:: loading settings

.PHONY: help build secrets up down clean ps logs stats sql smoke bronze validate acceptance acceptance-from-zero k8s-secrets landing k8s-job-image s3-access-test sparkapp airflow-chart deploy-guard airflow-install airflow-admin airflow-ui backfill dag-runs dag-report drill-driver drill-executor drill-scheduler backup-now drill-lose-metastore restore-metastore catalog maintenance venv lint fmt test all hooks ghcr-prune k8s-up k8s-demo airflow-viewer airflow-viewer-check minio-ui history-ui freeze freeze-check evidence evidence-export spark-operator-chart demo-task2 demo-pipeline demo-check trino-chart trino-install trino-cli trino-ui mode-query mode-pipeline trino-check trino-explore trino-tag-check demo-task3 starrocks-chart starrocks-render starrocks-install starrocks-sql starrocks-up starrocks-down starrocks-ready starrocks-init dbt dbt-parse mode-gold

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
s3-access-test: ## Task 2: prove least privilege of the MinIO users spark-jobs, landing-writer, demo-viewer (allowed AND denied ops)
	kubectl -n spark-jobs create configmap s3-access-check --from-file=tests/k8s/s3_access_check.py --dry-run=client -o yaml | kubectl apply -f -
	@set -e; rc=0; for p in spark-jobs:s3-credentials landing-writer:landing-writer-credentials demo-viewer:demo-viewer-credentials trino:trino-access-test-credentials starrocks:starrocks-access-test-credentials; do \
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
SPARKAPP_NAME = $(subst _,-,$(APP))-$(DATE)
sparkapp:      ## Task 2: run one SparkApplication by hand (same template as the DAG): make sparkapp [APP=bronze|validate|silver|validate_silver] DATE=2024-07-24 [IDEMPOTENCY=yes]
	kubectl -n spark-jobs delete sparkapplication $(SPARKAPP_NAME) --ignore-not-found --wait=true
	sed -e 's/{{ ds }}/$(DATE)/g' -e 's|{{ var.value.spark_job_image }}|$(JOB_IMAGE)|' \
	    $(if $(filter yes,$(IDEMPOTENCY)),$(IDEMPOTENCY_SED)) \
	    dags/templates/$(APP)_sparkapp.yaml | kubectl apply -f -
	@for i in $$(seq 1 120); do \
	  s=$$(kubectl -n spark-jobs get sparkapplication $(SPARKAPP_NAME) -o jsonpath='{.status.applicationState.state}'); \
	  echo "[sparkapp] $$(date +%T) state=$${s:-PENDING}"; \
	  case "$$s" in COMPLETED) break;; FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done
	kubectl -n spark-jobs logs $(SPARKAPP_NAME)-driver | grep -E "^\[(bronze|validate|silver|validate_silver)\]|Error|Exception|Caused by" || true
	@test "$$(kubectl -n spark-jobs get sparkapplication $(SPARKAPP_NAME) -o jsonpath='{.status.applicationState.state}')" = COMPLETED
airflow-chart: ## Task 2: download Airflow chart 1.22.0 from the Apache CDN and check its SHA-512 (E29: helm repo index -> slow archive)
	mkdir -p $(dir $(AIRFLOW_CHART))
	test -f $(AIRFLOW_CHART) || curl -fL --retry 3 -o $(AIRFLOW_CHART) https://dlcdn.apache.org/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz
	echo "$(AIRFLOW_CHART_SHA512)  $(AIRFLOW_CHART)" | sha512sum --check
spark-operator-chart: ## Task 2: download Spark Operator chart 2.2.1 once from GitHub Releases and check its SHA-256 (E58: no GitHub needed afterwards)
	mkdir -p $(dir $(SPARK_OPERATOR_CHART))
	test -f $(SPARK_OPERATOR_CHART) || curl -fL --retry 3 -o $(SPARK_OPERATOR_CHART) https://github.com/kubeflow/spark-operator/releases/download/v2.2.1/spark-operator-2.2.1.tgz
	echo "$(SPARK_OPERATOR_CHART_SHA256)  $(SPARK_OPERATOR_CHART)" | sha256sum --check
trino-chart:   ## Task 3: download the Trino chart 1.44.0 once from GitHub Releases and check its SHA-256 (E58 pattern)
	mkdir -p $(dir $(TRINO_CHART))
	test -f $(TRINO_CHART) || curl -fL --retry 3 -o $(TRINO_CHART) https://github.com/trinodb/charts/releases/download/trino-1.44.0/trino-1.44.0.tgz
	echo "$(TRINO_CHART_SHA256)  $(TRINO_CHART)" | sha256sum --check
trino-install: trino-chart deploy-guard ## Task 3: helm upgrade --install Trino 483 (namespace trino); only in query mode (D12: Trino 2 Gi + Spark do not fit)
	@r=$$(kubectl -n airflow get deploy/airflow-scheduler -o jsonpath='{.spec.replicas}'); test "$$r" = 0 || \
	  { echo "refusing: Airflow scheduler is running (pipeline mode). Run make mode-query first (D12)"; exit 1; }
	helm upgrade --install trino $(TRINO_CHART) -n trino -f infra/helm/trino/values-local.yaml \
	  --description "git $$(git rev-parse --short HEAD)" --wait --timeout 15m
starrocks-chart: ## Task 4: download the kube-starrocks chart 1.11.8 once from GitHub Releases and check its SHA-256 (E58 pattern)
	mkdir -p $(dir $(STARROCKS_CHART))
	test -f $(STARROCKS_CHART) || curl -fL --retry 3 -o $(STARROCKS_CHART) https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v1.11.8/kube-starrocks-1.11.8.tgz
	echo "$(STARROCKS_CHART_SHA256)  $(STARROCKS_CHART)" | sha256sum --check
starrocks-render: starrocks-chart ## Task 4: render the chart and fail unless every image is pinned (no 4.1-latest, FE/BE 3.5.21 by digest)
	@out=$$(helm template starrocks $(STARROCKS_CHART) -n starrocks -f infra/helm/starrocks/values-local.yaml); \
	  echo "$$out" | grep -nE '^\s+(image|tag|repository):' ; \
	  ! echo "$$out" | grep -q '4.1-latest' || { echo "FAIL: 4.1-latest is still rendered"; exit 1; }; \
	  for c in fe-ubuntu be-ubuntu; do echo "$$out" | grep -q "starrocks/$$c" || { echo "FAIL: starrocks/$$c not rendered"; exit 1; }; done; \
	  echo "$$out" | grep -o '3.5.21@sha256:[0-9a-f]*' | sort -u; echo "starrocks-render: PASS (no 4.1-latest, FE/BE pinned)"
starrocks-install: starrocks-render deploy-guard ## Task 4: helm upgrade --install StarRocks (operator + FE + BE, namespace starrocks); refuses while a Spark run is active
	@! kubectl -n spark-jobs get sparkapplications --no-headers -o custom-columns=S:.status.applicationState.state | grep -vE '^(COMPLETED|FAILED)$$' | grep -q . || \
	  { echo "refusing: a SparkApplication is not finished; wait for it (StarRocks + Spark do not fit, D15)"; exit 1; }
	-kubectl -n trino scale deploy/trino-coordinator --replicas=0
	helm upgrade --install starrocks $(STARROCKS_CHART) -n starrocks -f infra/helm/starrocks/values-local.yaml \
	  --description "git $$(git rev-parse --short HEAD)" --wait --timeout 15m
	@echo "helm waited for the operator only; the operator now creates FE and BE (first image pull: several minutes)"
	@for i in $$(seq 1 180); do kubectl -n starrocks get sts starrocks-fe starrocks-be >/dev/null 2>&1 && break; sleep 5; done
	$(MAKE) --no-print-directory starrocks-ready
	kubectl -n starrocks get pods -o wide
starrocks-sql: ## Task 4: run SQL in the FE as root (password from infra/k8s/secrets, via stdin, never in args): make starrocks-sql SQL="SHOW BACKENDS"
	@test -n "$(SQL)" || { echo 'set SQL=, e.g. make starrocks-sql SQL="SHOW FRONTENDS; SHOW BACKENDS"'; exit 1; }
	@{ sed -n 's/^password=//p' infra/k8s/secrets/starrocks-root.env; echo "$(SQL)"; } | $(SR_SQL)
trino-cli:     ## Task 3: interactive Trino SQL on catalog lake (exit with quit; read-only)
	kubectl -n trino exec -it deploy/trino-coordinator -- trino --catalog lake
trino-ui:      ## Task 3: Trino web UI on http://localhost:8081 (8080 is Airflow's; Ctrl+C to stop)
	kubectl -n trino port-forward svc/trino 8081:8080
trino-check:   ## Task 3 step 3.4: every Silver number per day computed by Trino vs tests/expected/silver*.json (query mode)
	@r=$$(kubectl -n trino get deploy/trino-coordinator -o jsonpath='{.status.readyReplicas}' 2>/dev/null); test "$${r:-0}" -ge 1 || \
	  { echo "Trino is not running: make mode-query"; exit 1; }
	kubectl -n trino exec deploy/trino-coordinator -- trino --catalog lake --output-format CSV_UNQUOTED \
	  --execute "$$(cat sql/trino/reconcile.sql)" | python3 -I tools/trino_check.py
trino-explore: ## Task 3 step 3.5: plans (partition pruning), metadata tables, time travel, stats; output to EVIDENCE_TRINO (query mode)
	@r=$$(kubectl -n trino get deploy/trino-coordinator -o jsonpath='{.status.readyReplicas}' 2>/dev/null); test "$${r:-0}" -ge 1 || \
	  { echo "Trino is not running: make mode-query"; exit 1; }
	mkdir -p $(dir $(EVIDENCE_TRINO))
	kubectl -n trino exec deploy/trino-coordinator -- trino --catalog lake --execute "$$(cat sql/trino/explore.sql)" > $(EVIDENCE_TRINO)
	@echo "saved $$(wc -l < $(EVIDENCE_TRINO)) lines to $(EVIDENCE_TRINO)"
starrocks-init: deploy-guard ## Task 4 step 4.2 (D17): Job starrocks-init (catalog lake_iceberg, database gold, users dbt_rw/bi_ro, checks); gold mode
	@test -n "$(DBT_IMAGE_REF)" || { echo "DBT_IMAGE_REF missing in infra/k8s/images.env (images run + bump first)"; exit 1; }
	kubectl -n starrocks delete job starrocks-init --ignore-not-found --wait=true
	sed 's|__IMAGE__|$(DBT_IMAGE_REF)|' infra/k8s/starrocks/starrocks-init-job.yaml | kubectl apply -f -
	@for i in $$(seq 1 120); do s=$$(kubectl -n starrocks get job starrocks-init -o jsonpath='{.status.succeeded}{.status.failed}'); \
	  [ -n "$$s" ] && break; sleep 5; done; kubectl -n starrocks logs job/starrocks-init; \
	  test "$$(kubectl -n starrocks get job starrocks-init -o jsonpath='{.status.succeeded}')" = 1
dbt:           ## Task 4: one dbt command in a one-off pod (dbt image, user dbt_rw): make dbt CMD="debug --connection" | CMD="build --select +fct_user_message"
	@test -n "$(DBT_IMAGE_REF)" || { echo "DBT_IMAGE_REF missing in infra/k8s/images.env"; exit 1; }
	@test -n "$(CMD)" || { echo 'set CMD=, e.g. make dbt CMD="debug"'; exit 1; }
	@n=dbt-$$(date -u +%H%M%S); args=$$(python3 -c 'import json,shlex,sys; print(json.dumps(shlex.split(sys.argv[1])))' "$(CMD)"); \
	  sed -e "s|__NAME__|$$n|" -e 's|__IMAGE__|$(DBT_IMAGE_REF)|' -e "s|__ARGS__|$$args|" infra/k8s/starrocks/dbt-pod.yaml | kubectl apply -f - >/dev/null; \
	  for i in $$(seq 1 360); do p=$$(kubectl -n gold-jobs get pod $$n -o jsonpath='{.status.phase}'); case "$$p" in Succeeded|Failed) break;; esac; sleep 5; done; \
	  kubectl -n gold-jobs logs $$n; echo "[dbt] pod $$n: $$p"; kubectl -n gold-jobs delete pod $$n --wait=false >/dev/null; test "$$p" = Succeeded
dbt-parse:     ## Task 4: dbt parse of the LOCAL dbt/ folder in the current dbt image (no connection, no writes): run before a dbt PR
	@test -n "$(DBT_IMAGE_REF)" || { echo "DBT_IMAGE_REF missing in infra/k8s/images.env"; exit 1; }
	@n=dbt-parse-$$(date -u +%H%M%S); \
	  sed -e "s|__NAME__|$$n|" -e 's|__IMAGE__|$(DBT_IMAGE_REF)|' infra/k8s/starrocks/dbt-parse-pod.yaml | kubectl apply -f - >/dev/null; \
	  trap 'kubectl -n gold-jobs delete pod '"$$n"' --wait=false >/dev/null' EXIT; \
	  kubectl -n gold-jobs wait --for=condition=Ready pod/$$n --timeout=300s >/dev/null; \
	  kubectl -n gold-jobs cp dbt $$n:/tmp/dbt-local >/dev/null; \
	  kubectl -n gold-jobs exec $$n -- dbt parse --project-dir /tmp/dbt-local --profiles-dir /tmp/dbt-local
starrocks-down: ## Task 4: StarRocks -> 0: operator first, then FE/BE StatefulSets; waits until the pods are gone (graceful stop up to 120 s); PVCs + metadata stay
	-@kubectl -n starrocks scale deploy/kube-starrocks-operator --replicas=0 2>/dev/null
	-@kubectl -n starrocks scale statefulset/starrocks-be statefulset/starrocks-fe --replicas=0 2>/dev/null
	-@kubectl -n starrocks wait --for=delete pod/starrocks-fe-0 pod/starrocks-be-0 --timeout=300s 2>/dev/null; echo "starrocks-down: FE/BE stopped"
starrocks-up:  ## Task 4: StarRocks -> 1: FE/BE StatefulSets to 1 ourselves (E69: the operator only updates a StatefulSet when its spec hash changes), then the operator; waits for Ready + BE alive
	kubectl -n starrocks scale statefulset/starrocks-fe statefulset/starrocks-be --replicas=1
	kubectl -n starrocks scale deploy/kube-starrocks-operator --replicas=1
	$(MAKE) --no-print-directory starrocks-ready
starrocks-ready: ## Task 4: wait for the CURRENT FE/BE revision (rollout status, not an old pod: E69) and for the BE heartbeat (Alive)
	kubectl -n starrocks rollout status statefulset/starrocks-fe --timeout=900s
	kubectl -n starrocks rollout status statefulset/starrocks-be --timeout=900s
	@for i in $$(seq 1 36); do { sed -n 's/^password=//p' infra/k8s/secrets/starrocks-root.env; echo "SHOW BACKENDS"; } | $(SR_SQL) 2>/dev/null | grep -q '| true ' && break; sleep 5; done; \
	  for i in $$(seq 1 36); do { sed -n 's/^password=//p' infra/k8s/secrets/starrocks-root.env; echo "SELECT count(*) FROM TABLE(generate_series(1, 10))"; } | $(SR_SQL) >/dev/null 2>&1 && \
	    { echo "starrocks-ready: FE + BE alive and a BE query runs (E76: past the blacklist window)"; exit 0; }; sleep 5; done; \
	  echo "starrocks-ready: no query reached the BE after 3 min (make starrocks-sql SQL='SHOW BACKENDS')"; exit 1
mode-gold:     ## Task 4 (D15): gold mode. Refuses while a run is active; Trino + History Server -> 0; StarRocks -> 1 (Airflow stays up; Spark cannot start)
	@! kubectl -n spark-jobs get sparkapplications --no-headers -o custom-columns=S:.status.applicationState.state | grep -vE '^(COMPLETED|FAILED)$$' | grep -q . || \
	  { echo "refusing: a SparkApplication is not finished; wait for it"; exit 1; }
	@! $(AF) airflow dags list-runs chatbot_bronze -o plain 2>/dev/null | grep -qE " (running|queued) " || { echo "refusing: a chatbot_bronze run is running or queued; wait for it (E64)"; exit 1; }
	-kubectl -n trino scale deploy/trino-coordinator --replicas=0
	kubectl -n spark-jobs scale deploy/spark-history --replicas=0
	$(MAKE) --no-print-directory starrocks-up
mode-query:    ## Task 3 (D12): query mode. Refuses while a run is active; Airflow scheduler/dag-processor/triggerer + History Server -> 0; Trino -> 1
	@! kubectl -n spark-jobs get sparkapplications --no-headers -o custom-columns=S:.status.applicationState.state | grep -vE '^(COMPLETED|FAILED)$$' | grep -q . || \
	  { echo "refusing: a SparkApplication is not finished; wait for it"; exit 1; }
	@! $(AF) airflow dags list-runs chatbot_bronze -o plain 2>/dev/null | grep -qE " (running|queued) " || { echo "refusing: a chatbot_bronze run is running or queued; wait for it (E64)"; exit 1; }
	kubectl -n airflow scale $(DEMO_STOP) --replicas=0
	kubectl -n spark-jobs scale deploy/spark-history --replicas=0
	$(MAKE) --no-print-directory starrocks-down
	@if kubectl -n trino get deploy/trino-coordinator >/dev/null 2>&1; then \
	  kubectl -n trino scale deploy/trino-coordinator --replicas=1 && kubectl -n trino rollout status deploy/trino-coordinator --timeout=600s; \
	else echo "Trino not installed yet: make trino-install"; fi
mode-pipeline: ## Task 3 (D12): pipeline mode. Trino + StarRocks -> 0, then Airflow scheduler/dag-processor/triggerer -> 1 (Spark can run)
	-kubectl -n trino scale deploy/trino-coordinator --replicas=0
	$(MAKE) --no-print-directory starrocks-down
	$(MAKE) --no-print-directory demo-pipeline
airflow-install: airflow-chart deploy-guard ## Task 2: helm upgrade --install Airflow 3.3.2 (chart 1.22.0); image = AIRFLOW_IMAGE_REF in images.env
	@test -n "$(AIRFLOW_IMAGE_REF)" || { echo "AIRFLOW_IMAGE_REF missing in infra/k8s/images.env (bump it after the images run)"; exit 1; }
	helm upgrade --install airflow $(AIRFLOW_CHART) -n airflow \
	  -f infra/helm/airflow/values-local.yaml \
	  --set images.airflow.digest=$(lastword $(subst @, ,$(AIRFLOW_IMAGE_REF))) \
	  --set env[0].name=AIRFLOW_VAR_SPARK_JOB_IMAGE --set env[0].value=$(SPARK_JOB_IMAGE_REF) \
  --set env[1].name=AIRFLOW_VAR_DBT_IMAGE --set env[1].value=$(DBT_IMAGE_REF) \
	  --description "git $$(git rev-parse --short HEAD)" \
	  --wait --timeout 15m
deploy-guard:  ## Task 2: refuse to deploy unless on main with a clean tree (deployed = reviewed + merged, E39); ALLOW_BRANCH=yes overrides
	@test "$(ALLOW_BRANCH)" = yes || { b=$$(git rev-parse --abbrev-ref HEAD); \
	  test "$$b" = main || { echo "refusing: on branch '$$b'. Merge the PR, then: git switch main && git pull"; exit 1; }; \
	  git diff --quiet HEAD || { echo "refusing: uncommitted changes"; exit 1; }; }
	@echo "deploying git $$(git rev-parse --short HEAD) from $$(git rev-parse --abbrev-ref HEAD)"
airflow-admin: ## Task 2: (re)create the Airflow UI user admin with the password from infra/k8s/secrets/airflow-admin-password
	@test -s infra/k8s/secrets/airflow-admin-password || { echo "run make k8s-secrets first"; exit 1; }
	-@$(AF) airflow users delete --username admin >/dev/null 2>&1
	@$(AF) airflow users create --username admin --role Admin \
	  --firstname admin --lastname lab --email admin@lab.local --password "$$(cat infra/k8s/secrets/airflow-admin-password)" \
	  | grep -v -i password
airflow-viewer: ## Demo: (re)create the Airflow UI user viewer (Viewer role: read-only) with the password from infra/k8s/secrets/airflow-viewer-password
	@test -s infra/k8s/secrets/airflow-viewer-password || { echo "run make k8s-secrets first"; exit 1; }
	-@$(AF) airflow users delete --username viewer >/dev/null 2>&1
	@$(AF) airflow users create --username viewer --role Viewer \
	  --firstname viewer --lastname demo --email viewer@lab.local --password "$$(cat infra/k8s/secrets/airflow-viewer-password)" \
	  | grep -v -i password
airflow-viewer-check: ## Demo: prove viewer can read runs but not trigger or clear them (harmless attempts, HTTP codes only)
	@$(AFI) python - "$$(cat infra/k8s/secrets/airflow-viewer-password)" < tests/k8s/airflow_viewer_check.py
minio-ui:      ## Demo: MinIO console on http://localhost:9001; log in as demo-viewer (read-only; password = MINIO_DEMO_VIEWER_SECRET in secrets.env)
	kubectl -n lake port-forward svc/minio 9001:9001
history-ui:    ## Demo: Spark History Server on http://localhost:18080 (scales it to 1, back to 0 on Ctrl+C); runs since event logs were enabled
	kubectl -n spark-jobs scale deploy/spark-history --replicas=1
	set -e; trap 'kubectl -n spark-jobs scale deploy/spark-history --replicas=0' EXIT; \
	kubectl -n spark-jobs rollout status deploy/spark-history --timeout=600s; \
	kubectl -n spark-jobs port-forward svc/spark-history 18080:18080
airflow-ui:    ## Task 2: open the Airflow UI on http://localhost:8080 (Ctrl+C to stop)
	kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
DEMO_STOP  = deploy/airflow-scheduler deploy/airflow-dag-processor deploy/airflow-triggerer
DEMO_TASK ?= 2
demo-task2:    ## Demo profile (B3): view-only. Scheduler, dag-processor, triggerer -> 0 (nothing can start a run); api-server + History Server up. Refuses while a run is active
	@! kubectl -n spark-jobs get sparkapplications --no-headers -o custom-columns=S:.status.applicationState.state | grep -vE '^(COMPLETED|FAILED)$$' | grep -q . || \
	  { echo "refusing: a SparkApplication is not finished (state other than COMPLETED/FAILED); wait for it"; exit 1; }   # E61: finished apps stay until their TTL
	@! $(AF) airflow dags list-runs chatbot_bronze -o plain 2>/dev/null | grep -qE " (running|queued) " || { echo "refusing: a chatbot_bronze run is running or queued; wait for it (E64)"; exit 1; }
	kubectl -n airflow scale $(DEMO_STOP) --replicas=0
	kubectl -n spark-jobs scale deploy/spark-history --replicas=1
	kubectl -n spark-jobs rollout status deploy/spark-history --timeout=600s
	kubectl -n airflow rollout status deploy/airflow-api-server --timeout=600s
	@echo "demo-task2: view-only (no scheduler). UIs: make airflow-ui (viewer), make minio-ui (demo-viewer), port-forward History: kubectl -n spark-jobs port-forward svc/spark-history 18080:18080. Back to building: make demo-pipeline"
demo-task3:    ## Demo profile (Task 3): view-only like demo-task2 + Trino up (~5 Gi requests; no Spark can start)
	$(MAKE) --no-print-directory demo-task2
	kubectl -n trino scale deploy/trino-coordinator --replicas=1
	kubectl -n trino rollout status deploy/trino-coordinator --timeout=600s
	@echo "demo-task3: + Trino. UIs: make trino-ui (8081), make trino-cli. Back to building: make mode-pipeline"
demo-pipeline: ## Demo profile (B3): building mode. Airflow scheduler, dag-processor, triggerer -> 1; History Server -> 0 (frees 512 Mi for Spark)
	kubectl -n airflow scale $(DEMO_STOP) --replicas=1
	kubectl -n spark-jobs scale deploy/spark-history --replicas=0
	for d in $(DEMO_STOP); do kubectl -n airflow rollout status $$d --timeout=600s; done
demo-check:    ## Demo: before presenting. Workloads ready, UIs answer (temporary port-forwards), freeze tag of DEMO_TASK (2: Spark check; 3: + Trino, tag checked by Trino). Fails loudly
	@set -u; fail=0; \
	for w in lake:statefulset/postgres lake:statefulset/minio lake:deploy/hive-metastore airflow:deploy/airflow-api-server spark-jobs:deploy/spark-history $(if $(filter 3,$(DEMO_TASK)),trino:deploy/trino-coordinator); do \
	  ns=$${w%%:*}; obj=$${w#*:}; \
	  r=$$(kubectl -n $$ns get $$obj -o jsonpath='{.status.readyReplicas}' 2>/dev/null); \
	  if [ "$${r:-0}" -ge 1 ]; then echo "[demo-check] PASS ready  $$ns/$$obj ($$r ready)"; \
	  else echo "[demo-check] FAIL ready  $$ns/$$obj (0 ready; in demo-task2 the History Server must be 1)"; fail=1; fi; \
	done; \
	kubectl -n airflow port-forward svc/airflow-api-server 28080:8080 >/dev/null 2>&1 & p1=$$!; \
	kubectl -n lake port-forward svc/minio 29001:9001 >/dev/null 2>&1 & p2=$$!; \
	kubectl -n spark-jobs port-forward svc/spark-history 28081:18080 >/dev/null 2>&1 & p3=$$!; \
	p4=; $(if $(filter 3,$(DEMO_TASK)),kubectl -n trino port-forward svc/trino 28082:8080 >/dev/null 2>&1 & p4=$$!;) \
	trap 'kill $$p1 $$p2 $$p3 $$p4 2>/dev/null' EXIT; sleep 4; \
	for u in airflow:http://localhost:28080/api/v2/monitor/health minio-console:http://localhost:29001/ spark-history:http://localhost:28081/ $(if $(filter 3,$(DEMO_TASK)),trino:http://localhost:28082/v1/info); do \
	  name=$${u%%:*}; url=$${u#*:}; code=$$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 $$url || true); \
	  if [ "$$code" = 200 ]; then echo "[demo-check] PASS http   $$name ($$code)"; else echo "[demo-check] FAIL http   $$name ($$code)"; fail=1; fi; \
	done; \
	kill $$p1 $$p2 $$p3 $$p4 2>/dev/null; trap - EXIT; \
	if $(MAKE) --no-print-directory $(if $(filter 3,$(DEMO_TASK)),trino-tag-check,freeze-check) TASK=$(DEMO_TASK) | grep -E "^\[catalog\] (PASS|FAIL)"; then :; else fail=1; fi; \
	if [ $$fail = 0 ]; then echo "[demo-check] ALL PASS"; else echo "[demo-check] FAILED (see FAIL lines)"; exit 1; fi
backfill:      ## Task 2: backfill chatbot_bronze: make backfill [FROM=2024-07-02] [TO=2024-07-25] [REPROCESS=none|completed|failed] [DRY=yes]
	$(AF) airflow backfill create --dag-id chatbot_bronze --from-date $(FROM) --to-date $(TO) \
	  --reprocess-behavior $(REPROCESS) --max-active-runs 1 $(if $(filter yes,$(DRY)),--dry-run)
dag-runs:      ## Task 2: runs of chatbot_bronze with state, logical date (= the day) and run type
	@$(AF) airflow dags list-runs chatbot_bronze -o plain | awk 'NR==1 || /chatbot_bronze/' | cut -c1-160
dag-report:    ## Task 2: per day from the task logs in s3://airflow-logs (E51): landing found?, rows loaded, validation result
	$(AFI) python - chatbot_bronze < tests/k8s/dag_report.py
drill-driver:  ## Task 2 drill 1: re-run DRILL_DATE (default 2024-07-24) and delete the bronze DRIVER pod mid-run
	$(MAKE) --no-print-directory backfill FROM=$(DRILL_DATE) TO=$(DRILL_DATE) REPROCESS=completed 2>&1 | grep -c "backfill" >/dev/null
	@for i in $$(seq 1 300); do p=$$(kubectl -n spark-jobs get pods -l 'task_id=bronze,spark-role=driver' \
	    --field-selector=status.phase=Running -o name 2>/dev/null | head -1); [ -n "$$p" ] && break; sleep 1; done; \
	  test -n "$$p" || { echo "[drill] no running bronze driver within 300 s"; exit 1; }; sleep 5; \
	  echo "[drill] $$(date -u +%T) UTC: deleting $$p (driver)"; kubectl -n spark-jobs delete $$p --wait=false
drill-executor: ## Task 2 drill 2: re-run DRILL_DATE and delete the bronze EXECUTOR pod mid-run
	$(MAKE) --no-print-directory backfill FROM=$(DRILL_DATE) TO=$(DRILL_DATE) REPROCESS=completed 2>&1 | grep -c "backfill" >/dev/null
	@for i in $$(seq 1 300); do p=$$(kubectl -n spark-jobs get pods -l 'task_id=bronze,spark-role=executor' \
	    --field-selector=status.phase=Running -o name 2>/dev/null | head -1); [ -n "$$p" ] && break; sleep 1; done; \
	  test -n "$$p" || { echo "[drill] no running bronze executor within 300 s"; exit 1; }; sleep 2; \
	  echo "[drill] $$(date -u +%T) UTC: deleting $$p (executor)"; kubectl -n spark-jobs delete $$p --wait=false
drill-scheduler: ## Task 2 drill 3: re-run DRILL_DATE and restart the Airflow scheduler while the bronze driver runs
	$(MAKE) --no-print-directory backfill FROM=$(DRILL_DATE) TO=$(DRILL_DATE) REPROCESS=completed 2>&1 | grep -c "backfill" >/dev/null
	@for i in $$(seq 1 300); do p=$$(kubectl -n spark-jobs get pods -l 'task_id=bronze,spark-role=driver' \
	    --field-selector=status.phase=Running -o name 2>/dev/null | head -1); [ -n "$$p" ] && break; sleep 1; done; \
	  test -n "$$p" || { echo "[drill] no running bronze driver within 300 s"; exit 1; }; sleep 5; \
	  echo "[drill] $$(date -u +%T) UTC: restarting the scheduler while $$p runs"; kubectl -n airflow rollout restart deploy/airflow-scheduler
backup-now:    ## Task 2 2.5b: run the pg-backup CronJob now and print its log (incl. "backup ts=…")
	@j=pg-backup-manual-$$(date -u +%H%M%S); kubectl -n lake create job $$j --from=cronjob/pg-backup && \
	  kubectl -n lake wait --for=condition=complete job/$$j --timeout=300s && kubectl -n lake logs job/$$j -c dump && kubectl -n lake logs job/$$j -c upload | tail -3
drill-lose-metastore: ## Task 2 2.5b DESTRUCTIVE drill: drop the metastore DB (catalog lost; Iceberg files stay). Needs CONFIRM=yes
	@test "$(CONFIRM)" = yes || { echo "refusing: drops the Iceberg catalog database. Run make backup-now first, then CONFIRM=yes"; exit 1; }
	kubectl -n lake scale deploy/hive-metastore --replicas=0 && kubectl -n lake wait --for=delete pod -l app=hive-metastore --timeout=120s
	kubectl -n lake exec postgres-0 -- sh -c 'psql -U "$$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE metastore WITH (FORCE)" -c "CREATE DATABASE metastore"'
	kubectl -n lake scale deploy/hive-metastore --replicas=1 && kubectl -n lake rollout status deploy/hive-metastore --timeout=300s
	kubectl -n lake logs deploy/hive-metastore -c schema-init | tail -2
restore-metastore: ## Task 2 2.5b: restore the metastore DB from backup TS (make restore-metastore TS=20261026T…Z)
	@test -n "$(TS)" || { echo "set TS= to a backup timestamp (backup ts=… in make backup-now)"; exit 1; }
	kubectl -n lake scale deploy/hive-metastore --replicas=0 && kubectl -n lake wait --for=delete pod -l app=hive-metastore --timeout=120s
	kubectl -n lake delete job pg-restore-metastore --ignore-not-found
	pg=$$(kubectl -n lake get cronjob pg-backup -o jsonpath='{.spec.jobTemplate.spec.template.spec.initContainers[0].env[0].valueFrom.secretKeyRef.name}'); \
	  su=$$(kubectl -n lake get cronjob pg-backup -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].env[0].valueFrom.secretKeyRef.name}'); \
	  test -n "$$pg" -a -n "$$su" || { echo "cannot resolve the current secret names from cronjob/pg-backup"; exit 1; }; \
	  sed -e 's/__TS__/$(TS)/g' -e "s/__PG_SECRET_NAME__/$$pg/" -e "s/__BACKUP_SECRET_NAME__/$$su/" infra/k8s/lake/pg-restore-job.yaml | kubectl apply -f -
	kubectl -n lake wait --for=condition=complete job/pg-restore-metastore --timeout=300s; kubectl -n lake logs job/pg-restore-metastore -c restore | tail -3
	kubectl -n lake scale deploy/hive-metastore --replicas=1 && kubectl -n lake rollout status deploy/hive-metastore --timeout=300s
	kubectl -n lake logs deploy/hive-metastore -c schema-init | tail -1
catalog:       ## Task 2 2.5b drill 7 + freeze: make catalog CMD=info|forget|register|tag|verify-tag TABLE=bronze.chatlog[,bronze.bot_predict] [METADATA=…] [TAG=task2_v1]
	@n=catalog-$$(date -u +%H%M%S); \
	  args=$$(python3 -c 'import json; t=[x for x in "$(TABLE)".split(",") if x]; a=["$(CMD)"]+[y for x in t for y in ("--table",x)]+(["--metadata","$(METADATA)"] if "$(METADATA)" else [])+(["--tag","$(TAG)"] if "$(TAG)" else []); print(json.dumps(a))'); \
	  sed -e "s|__NAME__|$$n|" -e 's|__IMAGE__|$(JOB_IMAGE)|' -e "s|__ARGS__|$$args|" infra/k8s/spark-jobs/catalog-sparkapp.yaml | kubectl apply -f - >/dev/null; \
	  for i in $$(seq 1 120); do s=$$(kubectl -n spark-jobs get sparkapplication $$n -o jsonpath='{.status.applicationState.state}'); \
	    case "$$s" in COMPLETED|FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done; echo "[catalog] $$n state=$$s"; \
	  kubectl -n spark-jobs logs $$n-driver | grep -E "^\[catalog\]|Error|Exception" || true; test "$$s" = COMPLETED
FREEZE_TABLES_2 = bronze.chatlog,bronze.bot_predict
FREEZE_TABLES_3 = $(FREEZE_TABLES_2),silver.chat_message,silver.nlu_prediction,silver.user_message
FREEZE_TABLES  ?= $(FREEZE_TABLES_$(TASK))
freeze:        ## Freeze (B2): Iceberg tag task<TASK>_v1 on FREEZE_TABLES (RETAIN 365 DAYS, kept if it exists) + verify rows = expected: make freeze TASK=2
	@test -n "$(TASK)" || { echo "set TASK= (e.g. make freeze TASK=2)"; exit 1; }
	$(MAKE) --no-print-directory catalog CMD=tag TABLE=$(FREEZE_TABLES) TAG=task$(TASK)_v1
trino-tag-check: ## Freeze (Task 3): verify tag task<TASK>_v1 through Trino (no Spark: fits next to Trino in demo-task3): make trino-tag-check TASK=3
	@test -n "$(TASK)" || { echo "set TASK= (e.g. make trino-tag-check TASK=3)"; exit 1; }
	@sql=$$(for t in $$(echo $(FREEZE_TABLES) | tr , ' '); do \
	  printf "SELECT '%s', count(*) FROM lake.%s FOR VERSION AS OF 'task$(TASK)_v1' UNION ALL " $$t $$t; done); \
	  kubectl -n trino exec deploy/trino-coordinator -- trino --catalog lake --output-format CSV_UNQUOTED \
	  --execute "$${sql% UNION ALL }" | python3 -I tools/trino_tag_check.py task$(TASK)_v1
freeze-check:  ## Freeze (B2): verify the tag still reads the frozen rows (run after maintenance): make freeze-check TASK=2
	@test -n "$(TASK)" || { echo "set TASK= (e.g. make freeze-check TASK=2)"; exit 1; }
	$(MAKE) --no-print-directory catalog CMD=verify-tag TABLE=$(FREEZE_TABLES) TAG=task$(TASK)_v1
EVIDENCE_DIR ?= $(HOME)/data/vnpt-lab/evidence/task$(or $(TASK),2)-$(shell date -u +%Y%m%d)
evidence:      ## Freeze (B2): save run states OUTSIDE Git to EVIDENCE_DIR (counts and states only, no rows)
	mkdir -p $(EVIDENCE_DIR)
	$(MAKE) --no-print-directory dag-runs   > $(EVIDENCE_DIR)/dag-runs.txt
	$(MAKE) --no-print-directory dag-report > $(EVIDENCE_DIR)/dag-report.txt
	kubectl get pods -A -o wide > $(EVIDENCE_DIR)/pods.txt
	kubectl get pvc -A > $(EVIDENCE_DIR)/pvc.txt
	kubectl describe node minikube | grep -A8 "Allocated resources" > $(EVIDENCE_DIR)/node-allocated.txt
	helm history airflow -n airflow > $(EVIDENCE_DIR)/helm-airflow.txt
	helm history spark-operator -n spark-operator > $(EVIDENCE_DIR)/helm-spark-operator.txt
	kubectl -n lake get jobs,cronjobs > $(EVIDENCE_DIR)/lake-jobs.txt
	@wc -l $(EVIDENCE_DIR)/*.txt
evidence-export: ## Freeze (B2): copy airflow-logs + backup (MinIO) and Spark event logs (PVC) to EVIDENCE_DIR; checks the file count
	mkdir -p $(EVIDENCE_DIR)
	kubectl -n lake create configmap evidence-export --from-file=tools/evidence_export.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	su=$$(kubectl -n lake get cronjob pg-backup -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].env[0].valueFrom.secretKeyRef.name}'); \
	  test -n "$$su" || { echo "cannot resolve the minio-service-users secret name from cronjob/pg-backup"; exit 1; }; \
	  kubectl -n lake delete pod evidence-export --ignore-not-found >/dev/null; \
	  sed -e 's|__IMAGE__|$(JOB_IMAGE)|' -e "s|__USERS_SECRET__|$$su|" infra/k8s/lake/evidence-export-pod.yaml | kubectl apply -f -
	set -e; trap 'kubectl -n lake delete pod evidence-export --wait=false >/dev/null' EXIT; \
	kubectl -n lake wait --for=condition=Ready pod/evidence-export --timeout=600s; \
	kubectl -n lake exec evidence-export -- python3 /scripts/evidence_export.py /export; \
	kubectl -n lake cp evidence-export:/export $(EVIDENCE_DIR)/s3 >/dev/null; \
	want=$$(wc -l < $(EVIDENCE_DIR)/s3/manifest.tsv); got=$$(find $(EVIDENCE_DIR)/s3 -type f ! -name manifest.tsv | wc -l); \
	echo "[evidence] copied $$got of $$want files"; test "$$got" = "$$want"
	kubectl -n spark-jobs scale deploy/spark-history --replicas=1 >/dev/null
	set -e; trap 'kubectl -n spark-jobs scale deploy/spark-history --replicas=0 >/dev/null' EXIT; \
	kubectl -n spark-jobs rollout status deploy/spark-history --timeout=600s >/dev/null; \
	pod=$$(kubectl -n spark-jobs get pod -l app=spark-history -o jsonpath='{.items[0].metadata.name}'); \
	kubectl -n spark-jobs cp $$pod:/spark-events $(EVIDENCE_DIR)/spark-events >/dev/null; \
	echo "[evidence] spark event logs: $$(find $(EVIDENCE_DIR)/spark-events -type f | wc -l) files"
	@du -sh $(EVIDENCE_DIR)
maintenance:   ## Task 2 2.5c: Iceberg maintenance now (same template as DAG lake_maintenance): [SNAP_HOURS=168] [RETAIN=5] [ORPHAN_HOURS=72] [DRY=yes]
	@test $(or $(ORPHAN_HOURS),72) -ge 24 || { echo "refusing: ORPHAN_HOURS must be >= 24 (Iceberg guard against deleting files of a running write, E46)"; exit 1; }
	kubectl -n spark-jobs delete sparkapplication maintenance-manual --ignore-not-found --wait=true
	sed -e 's/name: maintenance  /name: maintenance-manual  /' -e 's|{{ var.value.spark_job_image }}|$(JOB_IMAGE)|' \
	    -e 's/{{ params.snapshot_hours }}/$(or $(SNAP_HOURS),168)/' -e 's/{{ params.retain_last }}/$(or $(RETAIN),5)/' \
	    -e 's/{{ params.orphan_hours }}/$(or $(ORPHAN_HOURS),72)/' -e 's/{{ params.dry_run }}/$(if $(filter yes,$(DRY)),true,false)/' \
	    dags/templates/maintenance_sparkapp.yaml | kubectl apply -f -
	@for i in $$(seq 1 180); do s=$$(kubectl -n spark-jobs get sparkapplication maintenance-manual -o jsonpath='{.status.applicationState.state}'); \
	  case "$$s" in COMPLETED|FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done; echo "[maint] state=$$s"; \
	  kubectl -n spark-jobs logs maintenance-manual-driver | grep -E "^\[maint\]|Error|Exception" || true; test "$$s" = COMPLETED
venv:          ## dev: local .venv with the pinned tools + job deps (requirements-dev.txt, ~330 MB, git-ignored)
	test -x .venv/bin/python || python3 -m venv .venv
	.venv/bin/pip install -q -r requirements-dev.txt
lint:          ## dev: ruff check + ruff format --check (same as CI)
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .
fmt:           ## dev: apply ruff format + safe ruff fixes
	.venv/bin/ruff format .
	.venv/bin/ruff check --fix .
test:          ## dev: unit tests (no Spark needed; DAG tests run in CI inside the Airflow image)
	.venv/bin/pytest
all: lint test ## dev: lint + test
hooks:         ## dev: install the pre-commit hooks (.pre-commit-config.yaml) into .git/hooks
	.venv/bin/pre-commit install
ghcr-prune:    ## Task 2 2.7: list old GHCR versions (keeps images.env digests + newest KEEP=3); CONFIRM=yes deletes
	python3 .github/scripts/ghcr_prune.py --keep $(or $(KEEP),3) $(if $(filter yes,$(CONFIRM)),--confirm,)

# ---- Task 2 from zero (2.8) ----
MINIKUBE_START = minikube start --driver=docker --kubernetes-version=v1.37.0 --memory=8192 --cpus=6 \
                 --extra-config=kubelet.system-reserved=memory=3686Mi
DEMO_DAYS      = $$(( ($$(date -ud $(TO) +%s) - $$(date -ud $(FROM) +%s)) / 86400 + 1 ))

k8s-up: deploy-guard ## Task 2: whole stack, idempotent (also from zero after minikube delete): cluster, secrets, lake, operator, Airflow
	minikube status >/dev/null 2>&1 || $(MINIKUBE_START)
	minikube addons enable metrics-server
	$(MAKE) --no-print-directory k8s-secrets
	kubectl apply -k infra/k8s
	kubectl -n lake rollout status statefulset/postgres --timeout=600s
	kubectl -n lake rollout status statefulset/minio --timeout=600s
	kubectl -n lake wait --for=condition=complete job/bucket-init job/minio-users --timeout=600s
	kubectl -n lake annotate cronjob pg-backup vnpt-lake/nudge="$$(date -u +%FT%TZ)" --overwrite >/dev/null   # E66: re-evaluate after a sleep
	kubectl -n lake rollout status deploy/hive-metastore --timeout=900s
	$(MAKE) --no-print-directory spark-operator-chart
	helm upgrade --install spark-operator $(SPARK_OPERATOR_CHART) -n spark-operator \
	  -f infra/helm/spark-operator/values-local.yaml --wait --timeout 10m
	@apps=$$(kubectl -n spark-jobs get sparkapplications --no-headers -o custom-columns=N:.metadata.name,S:.status.applicationState.state 2>/dev/null | awk '$$2=="COMPLETED"||$$2=="FAILED"{print $$1}'); \
	  for a in $$apps; do kubectl -n spark-jobs annotate sparkapplication $$a vnpt-lake/nudge="$$(date -u +%FT%TZ)" --overwrite >/dev/null; done; \
	  echo "nudged $$(echo $$apps | wc -w) finished SparkApplications (E68: the operator re-checks their TTL after a sleep)"
	$(MAKE) --no-print-directory airflow-install
	$(MAKE) --no-print-directory airflow-admin
	kubectl get pods -A --no-headers | awk '{print $$1, $$2, $$4}' | column -t
k8s-demo:      ## Task 2: landing -> unpause -> backfill FROM..TO -> wait -> per-day report -> unpause maintenance (~40 min)
	$(MAKE) --no-print-directory landing
	$(AF) airflow dags unpause chatbot_bronze >/dev/null
	$(MAKE) --no-print-directory backfill
	@days=$(DEMO_DAYS); for i in $$(seq 1 120); do \
	  runs=$$($(AF) airflow dags list-runs chatbot_bronze -o plain | grep backfill__ || true); \
	  done_n=$$(echo "$$runs" | grep -cE " (success|failed) " || true); active=$$(echo "$$runs" | grep -cE " (queued|running) " || true); \
	  echo "[demo] $$(date +%T) finished $$done_n/$$days, active $$active"; \
	  [ "$$done_n" -ge "$$days" ] && [ "$$active" -eq 0 ] && break; sleep 60; done
	$(MAKE) --no-print-directory dag-report
	@failed=$$($(AF) airflow dags list-runs chatbot_bronze -o plain | grep backfill__ | grep -c " failed " || true); \
	  test "$$failed" -eq 0 || { echo "[demo] $$failed backfill runs failed"; exit 1; }
	@r=$$(kubectl -n airflow get pods -l component=scheduler -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="scheduler")].restartCount}'); \
	  test "$$r" -eq 0 || { echo "[demo] scheduler container restarted $$r times (E52: OOM?)"; exit 1; }
	$(AF) airflow dags unpause lake_maintenance >/dev/null
	@echo "[demo] PASSED: $(FROM)..$(TO) backfilled, no failed run; lake_maintenance unpaused"
