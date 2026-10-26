"""Task 2 step 2.6 (gap #8): Telegram alerts for the lake DAGs. Lives in the Airflow plugins folder, which is on the
import path of every component (task runner, triggerer), as the deadline-alert docs require for custom callbacks.

Rules: metadata only (DAG, task, day, try, times) — never row contents, never exception text (it could carry data);
an alert must never break or slow the pipeline, so sending errors are logged and swallowed.
Credentials: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars from the Secret airflow-alerts (make k8s-secrets).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)
TIMEOUT_S = 10


def _get(obj, name, default=None):
    """Read a field from an Airflow object or a plain dict (the context shape differs per callback type)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def failure_text(context: dict) -> str:
    ti = context.get("ti") or context.get("task_instance")
    dag_id = _get(ti, "dag_id") or _get(context.get("dag"), "dag_id", "?")
    task_id = _get(ti, "task_id", "?")
    tries = _get(ti, "try_number", "?")
    max_tries = _get(ti, "max_tries")
    attempt = f"{tries}/{max_tries + 1}" if isinstance(max_tries, int) else f"{tries}"
    day = context.get("ds") or "none (run without a logical date)"
    return f"❌ {dag_id}.{task_id} FAILED for day {day} (attempt {attempt}, final)\nrun: {context.get('run_id', '?')}"


def deadline_text(context: dict) -> str:
    # 3.3.2 passes dag_run as the REST DAGRunResponse dump: the id is "dag_run_id"; logical_date is None for manual
    # runs triggered without one (E44)
    run = context.get("dag_run") or {}
    deadline = context.get("deadline") or {}
    day = _get(run, "logical_date") or f"none ({_get(run, 'run_type', '?')} run without a logical date)"
    run_id = _get(run, "dag_run_id") or _get(run, "run_id", "?")
    return (
        f"⏰ {_get(run, 'dag_id', '?')} run for {day} is still not finished: "
        f"deadline {_get(deadline, 'deadline_time', '?')} passed\nrun: {run_id}"
    )


def send_telegram(text: str) -> bool:
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        log.warning("lake_alerts: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set, alert not sent")
        return False
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=TIMEOUT_S
        ) as resp:
            ok = json.load(resp).get("ok", False)
    except Exception as e:  # noqa: BLE001 - alerting must never fail the task
        log.warning("lake_alerts: Telegram send failed: %s", type(e).__name__)  # no URL: it contains the token
        return False
    if not ok:
        log.warning("lake_alerts: Telegram answered ok=false")
    return ok


def task_failure_alert(context: dict) -> None:
    """on_failure_callback: Airflow calls it once, after the LAST try failed (retries exhausted)."""
    send_telegram(failure_text(context))


async def deadline_alert(**kwargs) -> None:
    """AsyncCallback of a DeadlineAlert (runs in the triggerer); Airflow passes `context`."""
    await asyncio.to_thread(send_telegram, deadline_text(kwargs.get("context") or {}))
