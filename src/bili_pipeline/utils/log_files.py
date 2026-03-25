"""Helpers for wrapping saved log files with timestamp markers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime


def _normalize_timestamp(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("timestamp must not be empty")
    return normalized


def build_timestamp_marker(stage: str, value: datetime | str) -> str:
    normalized_stage = stage.strip().upper()
    if not normalized_stage:
        raise ValueError("stage must not be empty")
    return f"[TIMESTAMP][{normalized_stage}] {_normalize_timestamp(value)}"


def wrap_log_text(content: str, *, started_at: datetime | str, finished_at: datetime | str) -> str:
    body = content.strip()
    lines = [build_timestamp_marker("BEGIN", started_at)]
    if body:
        lines.append(body)
    lines.append(build_timestamp_marker("END", finished_at))
    return "\n".join(lines) + "\n"


def wrap_log_lines(
    lines: Sequence[str],
    *,
    started_at: datetime | str,
    finished_at: datetime | str,
) -> str:
    return wrap_log_text(
        "\n".join(line.rstrip("\n") for line in lines).strip(),
        started_at=started_at,
        finished_at=finished_at,
    )
