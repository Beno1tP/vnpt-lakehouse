"""DAG integrity check, run INSIDE the Airflow image (CI job `dags`): every file imports, no cycles, and the
rules from docs/task2/01_knowledge.md A3 hold. Plain asserts (no pytest in the Airflow image)."""

import inspect
import os
import sys
from pathlib import Path

import jinja2
import yaml

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")  # set before importing airflow (config is read once)

try:  # Airflow 3.x location, older alias as fallback
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
    if "lake" in dag.tags and "DataIntervalTimetable" not in type(dag.timetable).__name__:
        problems.append(f"schedule must be a data-interval timetable, got {type(dag.timetable).__name__} (E38)")
    if "lake" in dag.tags:  # production DAGs: every task bounded in time, validation retried <= 1
        problems += [f"{t.task_id}: needs execution_timeout" for t in dag.tasks if t.execution_timeout is None]
        problems += [
            f"{t.task_id}: needs on_failure_callback (alert, 2.6)" for t in dag.tasks if not t.on_failure_callback
        ]
        if not dag.deadline:
            problems.append("needs a DeadlineAlert (2.6)")
        problems += [
            f"{t.task_id}: validation retries at most once (deterministic failures, E41)"
            for t in dag.tasks
            if "validate" in t.task_id and t.retries > 1
        ]
    # E47: render every application_file template like Airflow does (strict Jinja), with AND without a logical date.
    # Without one, a template may only use ds if a check_landing task upstream stops such runs first.
    env = (
        dag.get_template_env()
        if hasattr(dag, "get_template_env")
        else jinja2.Environment(undefined=jinja2.StrictUndefined)
    )
    base = {"var": {"value": {"spark_job_image": "img@sha256:0"}}, "params": dag.params.dump(), "run_id": "manual__x"}
    for t in dag.tasks:
        app_file = getattr(t, "application_file", None)
        if not app_file:
            continue
        # DagBag already replaced the path with the file's content (template_ext .yaml is resolved at parse time)
        text = app_file if "\n" in app_file else (Path(dag.fileloc).parent / app_file).read_text()
        try:
            doc = yaml.safe_load(env.from_string(text).render(**base, ds="2024-07-24"))
            if doc.get("kind") != "SparkApplication":
                problems.append(f"{t.task_id}: application_file does not render to a SparkApplication")
        except Exception as e:  # noqa: BLE001
            problems.append(f"{t.task_id}: application_file fails to render with a date: {type(e).__name__}: {e}")
            continue
        try:
            env.from_string(text).render(**base)
        except jinja2.UndefinedError as e:
            if "check_landing" not in t.get_flat_relative_ids(upstream=True):
                problems.append(
                    f"{t.task_id}: application_file needs a logical date ({e}) but nothing upstream stops "
                    f"runs without one (E47)"
                )
    for p in problems:
        print(f"[dags] RULE {dag_id}: {p}")
    errors += len(problems)
    print(f"[dags] {dag_id}: {len(dag.tasks)} tasks, {'OK' if not problems else 'FAIL'}")
print(f"[dags] {len(bag.dags)} DAGs, {errors} problems")
sys.exit(1 if errors or not bag.dags else 0)
