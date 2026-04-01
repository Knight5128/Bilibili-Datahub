from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR

from .shared import build_credential_from_cookie


DEFAULT_BACKGROUND_TASKS_ROOT = DEFAULT_VIDEO_DATA_OUTPUT_DIR / "background_tasks"
DEFAULT_COOKIE_PATH = Path(".local") / "bilibili-datahub.cookie.txt"
STATUS_FILENAME = "task_status.json"
CONFIG_FILENAME = "task_config.json"


@dataclass(slots=True)
class BatchedCrawlOutcome:
    reports: list[Any]
    credential_refresh_count: int


def load_cookie_text(path: Path | str | None = None) -> str:
    target = Path(path or DEFAULT_COOKIE_PATH)
    if not target.exists():
        return ""
    return target.read_text(encoding="utf-8").strip()


def save_cookie_text(cookie_text: str, *, path: Path | str | None = None) -> Path:
    target = Path(path or DEFAULT_COOKIE_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text((cookie_text or "").strip(), encoding="utf-8")
    return target


def load_credential_from_cookie_file(path: Path | str | None = None):
    return build_credential_from_cookie(load_cookie_text(path))


def create_background_task_dir(
    task_kind: str,
    *,
    root_dir: Path | str | None = None,
    started_at: datetime | None = None,
) -> Path:
    root = Path(root_dir or DEFAULT_BACKGROUND_TASKS_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    token = (started_at or datetime.now()).strftime("%Y%m%d_%H%M%S")
    candidate = root / f"{task_kind}_{token}"
    suffix = 2
    while candidate.exists():
        candidate = root / f"{task_kind}_{token}_{suffix}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def update_background_task_status(task_dir: Path | str, payload: dict[str, Any]) -> Path:
    target = Path(task_dir) / STATUS_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_background_task_status(task_dir: Path | str) -> dict[str, Any]:
    target = Path(task_dir) / STATUS_FILENAME
    if not target.exists():
        return {}
    return json.loads(target.read_text(encoding="utf-8"))


def write_background_task_config(task_dir: Path | str, payload: dict[str, Any]) -> Path:
    target = Path(task_dir) / CONFIG_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_background_task_config(task_dir: Path | str) -> dict[str, Any]:
    target = Path(task_dir) / CONFIG_FILENAME
    return json.loads(target.read_text(encoding="utf-8"))


def register_active_background_task(
    scope: str,
    *,
    task_dir: Path | str,
    registry_root: Path | str | None = None,
    pid: int | None = None,
) -> Path:
    root = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"active_{scope}.json"
    payload = {
        "scope": scope,
        "task_dir": str(Path(task_dir)),
        "pid": int(pid) if pid is not None else None,
        "updated_at": datetime.now().isoformat(),
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_active_background_task(scope: str, *, registry_root: Path | str | None = None) -> dict[str, Any] | None:
    target = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT) / f"active_{scope}.json"
    if not target.exists():
        return None
    return json.loads(target.read_text(encoding="utf-8"))


def background_task_is_running(scope: str, *, registry_root: Path | str | None = None) -> bool:
    payload = load_active_background_task(scope, registry_root=registry_root)
    if not payload:
        return False
    task_dir = payload.get("task_dir")
    if not task_dir:
        return False
    status = load_background_task_status(task_dir).get("status")
    return status in {"queued", "running"}


def clear_active_background_task(scope: str, *, registry_root: Path | str | None = None, task_dir: Path | str | None = None) -> None:
    target = Path(registry_root or DEFAULT_BACKGROUND_TASKS_ROOT) / f"active_{scope}.json"
    if not target.exists():
        return
    if task_dir is not None:
        payload = json.loads(target.read_text(encoding="utf-8"))
        if str(payload.get("task_dir") or "") != str(Path(task_dir)):
            return
    target.unlink(missing_ok=True)


def _read_csv_rows(csv_path: Path | str) -> tuple[list[str], list[dict[str, str]]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: (value if value is not None else "") for key, value in row.items()} for row in reader]
    return fieldnames, rows


def _write_csv_rows(fieldnames: list[str], rows: list[dict[str, str]], path: Path | str) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    return target


def build_background_worker_command(task_dir: Path | str) -> list[str]:
    project_root = Path(__file__).resolve().parents[3]
    worker_script = project_root / "scripts" / "datahub_background_worker.py"
    return [sys.executable, str(worker_script), "--task-dir", str(Path(task_dir))]


def launch_background_worker(task_dir: Path | str) -> int:
    task_path = Path(task_dir)
    task_path.mkdir(parents=True, exist_ok=True)
    stdout_path = task_path / "worker_stdout.log"
    stderr_path = task_path / "worker_stderr.log"
    command = build_background_worker_command(task_path)
    creationflags = 0
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
    project_root = Path(__file__).resolve().parents[3]
    with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open("a", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=str(project_root),
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creationflags,
        )
    return int(process.pid)


def run_batched_crawl_from_csv(
    csv_path: Path | str,
    *,
    batch_size: int,
    crawl_fn,
    credential_provider=None,
    credential: Any | None = None,
    should_retry_remaining_fn=None,
    max_remaining_retries_per_batch: int = 1,
    **crawl_kwargs,
) -> BatchedCrawlOutcome:
    fieldnames, rows = _read_csv_rows(csv_path)
    if not fieldnames:
        raise ValueError("CSV 缺少表头。")
    effective_batch_size = max(1, int(batch_size or 1))
    should_retry = should_retry_remaining_fn or (lambda _report: False)
    session_dir = Path(crawl_kwargs.get("session_dir") or Path(csv_path).parent)
    batches_dir = session_dir / "_background_batches"
    batches_dir.mkdir(parents=True, exist_ok=True)

    reports: list[Any] = []
    credential_refresh_count = 0

    for batch_index in range(0, len(rows), effective_batch_size):
        batch_rows = rows[batch_index : batch_index + effective_batch_size]
        batch_no = batch_index // effective_batch_size + 1
        current_csv_path = _write_csv_rows(fieldnames, batch_rows, batches_dir / f"batch_input_{batch_no}.csv")
        remaining_retry_count = 0
        while True:
            current_credential = credential_provider() if credential_provider is not None else credential
            if credential_provider is not None:
                credential_refresh_count += 1
            report = crawl_fn(
                current_csv_path,
                credential=current_credential,
                source_csv_name=Path(current_csv_path).name,
                **crawl_kwargs,
            )
            reports.append(report)
            remaining_csv_path = str(getattr(report, "remaining_csv_path", "") or "").strip()
            remaining_count = int(getattr(report, "remaining_count", 0) or 0)
            if bool(getattr(report, "completed_all", False)) or remaining_count == 0 or not remaining_csv_path:
                break
            if remaining_retry_count >= max(0, int(max_remaining_retries_per_batch)):
                break
            if not should_retry(report):
                break
            remaining_retry_count += 1
            current_csv_path = Path(remaining_csv_path)

    return BatchedCrawlOutcome(
        reports=reports,
        credential_refresh_count=credential_refresh_count,
    )
