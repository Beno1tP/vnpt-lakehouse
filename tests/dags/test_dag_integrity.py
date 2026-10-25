"""DAG integrity check, run INSIDE the Airflow image (CI job `dags`): every file imports, no cycles, and the
rules from docs/task2/01_knowledge.md A3 hold. Plain asserts (no pytest in the Airflow image)."""
import inspect
import os
import sys

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")   # set before importing airflow (config is read once)

try:                                     # Airflow 3.x location, older alias as fallback
    from airflow.dag_processing.dagbag import DagBag
except ImportError:
    from airflow.models import DagBag

DAGS = "/opt/airflow/dags"
# Airflow 3.3's DagBag no longer takes include_examples (E34): examples follow [core] load_examples instead
kwargs = {"include_examples": False} if "include_examples" in inspect.signature(DagBag).parameters else {}
bag = DagBag(dag_folder=DAGS, **kwargs)
foreign = [d for d, dag in bag.dags.items() if not str(dag.fileloc).startswith(DAGS)]
if foreign:
    print(f"[dags] ignoring {len(foreign)} DAGs outside {DAGS} (examples?)")
    for d in foreign:
        bag.dags.pop(d)
errors = 0
for path, err in bag.import_errors.items():
    print(f"[dags] IMPORT ERROR {path}: {err.splitlines()[-1] if err else err}")
    errors += 1
for dag_id, dag in sorted(bag.dags.items()):
    problems = []
    if dag.catchup:
        problems.append("catchup must be False (history = explicit backfill)")
    if dag.max_active_runs != 1:
        problems.append("max_active_runs must be 1 (runs write the same tables)")
    if not dag.tags:
        problems.append("needs tags")
    if dag.start_date is None or dag.start_date.tzinfo is None:
        problems.append("start_date must be timezone-aware")
    for p in problems:
        print(f"[dags] RULE {dag_id}: {p}")
    errors += len(problems)
    print(f"[dags] {dag_id}: {len(dag.tasks)} tasks, {'OK' if not problems else 'FAIL'}")
print(f"[dags] {len(bag.dags)} DAGs, {errors} problems")
sys.exit(1 if errors or not bag.dags else 0)
