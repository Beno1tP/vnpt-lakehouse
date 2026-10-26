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

.PHONY: help build secrets up down clean ps logs stats sql smoke bronze validate acceptance acceptance-from-zero k8s-secrets landing k8s-job-image s3-access-test sparkapp airflow-chart deploy-guard airflow-install airflow-admin airflow-ui backfill dag-runs dag-report drill-driver drill-executor drill-scheduler backup-now drill-lose-metastore restore-metastore catalog maintenance venv lint fmt test all hooks ghcr-prune k8s-up k8s-demo

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
airflow-install: airflow-chart deploy-guard ## Task 2: helm upgrade --install Airflow 3.3.2 (chart 1.22.0); image = AIRFLOW_IMAGE_REF in images.env
	@test -n "$(AIRFLOW_IMAGE_REF)" || { echo "AIRFLOW_IMAGE_REF missing in infra/k8s/images.env (bump it after the images run)"; exit 1; }
	helm upgrade --install airflow $(AIRFLOW_CHART) -n airflow \
	  -f infra/helm/airflow/values-local.yaml \
	  --set images.airflow.digest=$(lastword $(subst @, ,$(AIRFLOW_IMAGE_REF))) \
	  --set env[0].name=AIRFLOW_VAR_SPARK_JOB_IMAGE --set env[0].value=$(SPARK_JOB_IMAGE_REF) \
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
airflow-ui:    ## Task 2: open the Airflow UI on http://localhost:8080 (Ctrl+C to stop)
	kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
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
catalog:       ## Task 2 2.5b drill 7: make catalog CMD=info|forget|register TABLE=bronze.chatlog [METADATA=s3a://…metadata.json]
	@n=catalog-$$(date -u +%H%M%S); \
	  args=$$(python3 -c 'import json,sys; a=["$(CMD)","--table","$(TABLE)"]+(["--metadata","$(METADATA)"] if "$(METADATA)" else []); print(json.dumps(a))'); \
	  sed -e "s|__NAME__|$$n|" -e 's|__IMAGE__|$(JOB_IMAGE)|' -e "s|__ARGS__|$$args|" infra/k8s/spark-jobs/catalog-sparkapp.yaml | kubectl apply -f - >/dev/null; \
	  for i in $$(seq 1 120); do s=$$(kubectl -n spark-jobs get sparkapplication $$n -o jsonpath='{.status.applicationState.state}'); \
	    case "$$s" in COMPLETED|FAILED|SUBMISSION_FAILED) break;; esac; sleep 5; done; echo "[catalog] $$n state=$$s"; \
	  kubectl -n spark-jobs logs $$n-driver | grep -E "^\[catalog\]|Error|Exception" || true; test "$$s" = COMPLETED
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
	kubectl -n lake rollout status deploy/hive-metastore --timeout=900s
	helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update >/dev/null
	helm upgrade --install spark-operator spark-operator/spark-operator --version 2.2.1 -n spark-operator \
	  -f infra/helm/spark-operator/values-local.yaml --wait --timeout 10m
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
