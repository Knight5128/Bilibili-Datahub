from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd

from bili_pipeline.crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR, crawl_bvid_list_from_csv
from bili_pipeline.discover import BilibiliHotSource, BilibiliUserRecentVideoSource
from bili_pipeline.models import BatchCrawlReport, CandidateVideo, CrawlTaskMode, RankboardEntry, VideoPoolEntry

from bili_pipeline.datahub.discover_ops import (
    CUSTOM_EXPORT_MAX_RETRIES,
    CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
    CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
    CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
    FULL_EXPORT_MAX_RETRIES,
    FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
    FULL_EXPORT_REQUEST_JITTER_SECONDS,
    FULL_EXPORT_RETRY_BACKOFF_SECONDS,
    HOT_400_MAX_PAGES,
    HOT_400_PAGE_SIZE,
    build_rankboard_result,
    format_timestamp_token,
    load_rankboard_boards,
)

_T = TypeVar("_T")

_LOG = logging.getLogger(__name__)

SOURCE_TYPES_SERIAL_ORDER: tuple[str, ...] = ("author_recent", "hot", "rankboard")

_DISCOVERY_SOURCE_TO_CANONICAL: dict[str, str] = {
    "hot": "hot",
    "rankboard": "rankboard",
    "author_expand": "author_recent",
    "author_recent": "author_recent",
}

SESSION_MANUAL_CRAWL_PREFIX = "manual_crawl_"
HOT_RANKBOARD_SNAPSHOT_NAME = "hot_rankboard_candidates.csv"
AUTHOR_RECENT_SNAPSHOT_NAME = "author_recent_candidates.csv"
MERGED_BEFORE_PRUNE_NAME = "merged_candidates_before_prune.csv"
FILTERED_VIDEO_LIST_NAME = "filtered_video_list.csv"
SNAPSHOT_AFTER_RUN_NAME = "realtime_watchlist_snapshot_after_run.csv"
MANUAL_CRAWL_STATE_NAME = "manual_crawl_state.json"
LOGS_SUBDIR = "logs"

REALTIME_WATCHLIST_AUTHORS_FILENAME = "realtime_watchlist_authors.csv"
REALTIME_WATCHLIST_AUTHORS_STATE_FILENAME = "realtime_watchlist_authors_state.json"
REALTIME_WATCHLIST_CURRENT_FILENAME = "realtime_watchlist_current.csv"
REALTIME_WATCHLIST_STATE_FILENAME = "realtime_watchlist_state.json"


def realtime_watchlist_manual_crawls_dir(video_data_root: Path | str | None = None) -> Path:
    base = Path(video_data_root) if video_data_root is not None else DEFAULT_VIDEO_DATA_OUTPUT_DIR
    return base / "manual_crawls"


def realtime_watchlist_authors_csv_path(video_data_root: Path | str | None = None) -> Path:
    return realtime_watchlist_manual_crawls_dir(video_data_root) / REALTIME_WATCHLIST_AUTHORS_FILENAME


def realtime_watchlist_authors_state_path(video_data_root: Path | str | None = None) -> Path:
    return realtime_watchlist_manual_crawls_dir(video_data_root) / REALTIME_WATCHLIST_AUTHORS_STATE_FILENAME


def realtime_watchlist_current_csv_path(video_data_root: Path | str | None = None) -> Path:
    return realtime_watchlist_manual_crawls_dir(video_data_root) / REALTIME_WATCHLIST_CURRENT_FILENAME


def realtime_watchlist_state_json_path(video_data_root: Path | str | None = None) -> Path:
    return realtime_watchlist_manual_crawls_dir(video_data_root) / REALTIME_WATCHLIST_STATE_FILENAME


def _to_utc_timestamp(raw_value: Any) -> pd.Timestamp:
    if raw_value in (None, ""):
        return pd.NaT
    text = str(raw_value).strip()
    if not text:
        return pd.NaT
    normalized = text.replace("Z", "+00:00")
    parsed = pd.to_datetime(normalized, errors="coerce", utc=True)
    return parsed if not pd.isna(parsed) else pd.NaT


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_owner_mid_cell(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            numeric = Decimal(text)
        except InvalidOperation:
            return None
        if not numeric.is_finite() or numeric != numeric.to_integral_value():
            return None
        return int(numeric)


def normalize_owner_mid_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[int] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        mid = parse_owner_mid_cell(row.get("owner_mid"))
        if mid is None:
            continue
        if mid in seen:
            continue
        seen.add(mid)
        merged = {k: row[k] for k in row}
        merged["owner_mid"] = mid
        out.append(merged)
    return out


@dataclass(slots=True)
class SaveLocalAuthorListResult:
    authors_csv_path: str
    author_count: int


@dataclass(slots=True)
class LocalAuthorListState:
    """In-memory summary for local author list (subset of persisted JSON)."""

    authors_csv_path: str
    author_count: int
    uploaded_at: datetime | None


@dataclass(slots=True)
class LocalAuthorsPersistedState:
    status: str
    authors_csv_path: str
    source_filename: str
    author_count: int
    uploaded_at: str | None
    last_used_at: str | None


@dataclass(slots=True)
class RealtimeWatchlistRootState:
    status: str
    current_csv_path: str
    time_window_hours: int
    current_bvid_count: int
    last_run_started_at: str | None
    last_run_finished_at: str | None
    last_run_session_dir: str | None
    last_run_status: str | None
    updated_at: str | None


def save_local_author_csv(
    rows: Iterable[Mapping[str, Any]],
    path: Path,
) -> SaveLocalAuthorListResult:
    normalized = normalize_owner_mid_rows(rows)
    if not normalized:
        raise ValueError("no valid owner_mid values to persist")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(normalized)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return SaveLocalAuthorListResult(authors_csv_path=str(path.resolve()), author_count=len(normalized))


def load_local_author_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype=object)
    if "owner_mid" not in frame.columns:
        raise ValueError("author CSV must include an owner_mid column")
    rows = frame.to_dict(orient="records")
    normalized = normalize_owner_mid_rows(rows)
    if not normalized:
        raise ValueError("author CSV contains no valid owner_mid values")
    return normalized


def save_local_author_list(
    rows: Iterable[Mapping[str, Any]],
    *,
    video_data_root: Path | str | None = None,
    csv_path: Path | None = None,
) -> SaveLocalAuthorListResult:
    target = csv_path or realtime_watchlist_authors_csv_path(video_data_root)
    return save_local_author_csv(rows, target)


def load_local_author_list(
    *,
    video_data_root: Path | str | None = None,
    csv_path: Path | None = None,
) -> list[dict[str, Any]]:
    target = csv_path or realtime_watchlist_authors_csv_path(video_data_root)
    return load_local_author_csv(target)


def local_authors_state_to_dict(state: LocalAuthorsPersistedState) -> dict[str, Any]:
    return {
        "status": state.status,
        "authors_csv_path": state.authors_csv_path,
        "source_filename": state.source_filename,
        "author_count": state.author_count,
        "uploaded_at": state.uploaded_at,
        "last_used_at": state.last_used_at,
    }


def local_authors_state_from_dict(payload: Mapping[str, Any]) -> LocalAuthorsPersistedState:
    return LocalAuthorsPersistedState(
        status=str(payload["status"]),
        authors_csv_path=str(payload["authors_csv_path"]),
        source_filename=str(payload["source_filename"]),
        author_count=int(payload["author_count"]),
        uploaded_at=str(payload["uploaded_at"]) if payload.get("uploaded_at") not in (None, "") else None,
        last_used_at=str(payload["last_used_at"]) if payload.get("last_used_at") not in (None, "") else None,
    )


def save_local_authors_state(
    state: LocalAuthorsPersistedState,
    *,
    video_data_root: Path | str | None = None,
    path: Path | None = None,
) -> Path:
    target = path or realtime_watchlist_authors_state_path(video_data_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(local_authors_state_to_dict(state), ensure_ascii=False, indent=2) + "\n"
    target.write_text(text, encoding="utf-8")
    return target


def load_local_authors_state(
    *,
    video_data_root: Path | str | None = None,
    path: Path | None = None,
) -> LocalAuthorsPersistedState | None:
    target = path or realtime_watchlist_authors_state_path(video_data_root)
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid author state JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("author state JSON must be an object")
    required = (
        "status",
        "authors_csv_path",
        "source_filename",
        "author_count",
        "uploaded_at",
        "last_used_at",
    )
    for key in required:
        if key not in payload:
            raise ValueError(f"author state JSON missing required field: {key}")
    return local_authors_state_from_dict(payload)


def watchlist_root_state_to_dict(state: RealtimeWatchlistRootState) -> dict[str, Any]:
    return {
        "status": state.status,
        "current_csv_path": state.current_csv_path,
        "time_window_hours": state.time_window_hours,
        "current_bvid_count": state.current_bvid_count,
        "last_run_started_at": state.last_run_started_at,
        "last_run_finished_at": state.last_run_finished_at,
        "last_run_session_dir": state.last_run_session_dir,
        "last_run_status": state.last_run_status,
        "updated_at": state.updated_at,
    }


def watchlist_root_state_from_dict(payload: Mapping[str, Any]) -> RealtimeWatchlistRootState:
    return RealtimeWatchlistRootState(
        status=str(payload["status"]),
        current_csv_path=str(payload["current_csv_path"]),
        time_window_hours=int(payload["time_window_hours"]),
        current_bvid_count=int(payload["current_bvid_count"]),
        last_run_started_at=str(payload["last_run_started_at"])
        if payload.get("last_run_started_at") not in (None, "")
        else None,
        last_run_finished_at=str(payload["last_run_finished_at"])
        if payload.get("last_run_finished_at") not in (None, "")
        else None,
        last_run_session_dir=str(payload["last_run_session_dir"])
        if payload.get("last_run_session_dir") not in (None, "")
        else None,
        last_run_status=str(payload["last_run_status"]) if payload.get("last_run_status") not in (None, "") else None,
        updated_at=str(payload["updated_at"]) if payload.get("updated_at") not in (None, "") else None,
    )


_WATCHLIST_STATE_REQUIRED_KEYS = frozenset(
    {
        "status",
        "current_csv_path",
        "time_window_hours",
        "current_bvid_count",
        "last_run_started_at",
        "last_run_finished_at",
        "last_run_session_dir",
        "last_run_status",
        "updated_at",
    }
)


def _validate_watchlist_state_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("watchlist state JSON must be an object")
    missing = sorted(_WATCHLIST_STATE_REQUIRED_KEYS - set(payload.keys()))
    if missing:
        raise ValueError(f"watchlist state JSON missing required fields: {', '.join(missing)}")
    try:
        int(payload["time_window_hours"])
        int(payload["current_bvid_count"])
    except (TypeError, ValueError) as exc:
        raise ValueError("watchlist state JSON has invalid numeric fields") from exc
    if not isinstance(payload["status"], str) or not isinstance(payload["current_csv_path"], str):
        raise ValueError("watchlist state JSON has invalid string fields")
    return payload


def save_watchlist_state(
    state: RealtimeWatchlistRootState,
    *,
    video_data_root: Path | str | None = None,
    path: Path | None = None,
) -> Path:
    target = path or realtime_watchlist_state_json_path(video_data_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(watchlist_root_state_to_dict(state), ensure_ascii=False, indent=2) + "\n"
    target.write_text(text, encoding="utf-8")
    return target


def load_watchlist_state(
    *,
    video_data_root: Path | str | None = None,
    path: Path | None = None,
) -> RealtimeWatchlistRootState:
    target = path or realtime_watchlist_state_json_path(video_data_root)
    if not target.exists():
        raise FileNotFoundError(target)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid watchlist state JSON: {exc}") from exc
    validated = _validate_watchlist_state_payload(payload)
    return watchlist_root_state_from_dict(validated)


def save_watchlist_csv(
    rows: Iterable[Mapping[str, Any]],
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(list(rows))
    if not frame.empty and "bvid" in frame.columns:
        cols = ["bvid"] + [c for c in frame.columns if c != "bvid"]
        frame = frame[cols]
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def load_watchlist_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        frame = pd.read_csv(path, encoding="utf-8-sig", dtype=object)
    except pd.errors.EmptyDataError:
        return []
    return frame.to_dict(orient="records")


def _resolve_watchlist_csv_path(state: RealtimeWatchlistRootState, manual_crawls_dir: Path) -> Path:
    raw = Path(state.current_csv_path)
    if raw.is_absolute():
        return raw
    return (manual_crawls_dir / raw).resolve()


def load_active_watchlist(
    *,
    video_data_root: Path | str | None = None,
    now: datetime,
    time_window_hours: int,
    state_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Load active watchlist rows using the caller's current run window.

    The persisted watchlist state's ``time_window_hours`` is metadata about the
    last saved run. Pruning for the current view always uses the
    caller-provided ``time_window_hours`` so a new run can apply a different
    window without rewriting state first.
    """
    manual_dir = realtime_watchlist_manual_crawls_dir(video_data_root)
    st_path = state_path or realtime_watchlist_state_json_path(video_data_root)
    if not st_path.exists():
        return []
    try:
        payload = json.loads(st_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid watchlist state JSON: {exc}") from exc
    validated = _validate_watchlist_state_payload(payload)
    state = watchlist_root_state_from_dict(validated)
    csv_path = _resolve_watchlist_csv_path(state, manual_dir)
    if not csv_path.exists():
        return []
    rows = load_watchlist_csv(csv_path)
    if not rows:
        return []
    now_utc = _ensure_utc(now)
    window_start = now_utc - timedelta(hours=time_window_hours)
    active: list[dict[str, Any]] = []
    for row in rows:
        ts = _to_utc_timestamp(row.get("pubdate"))
        if pd.isna(ts):
            continue
        pub = ts.to_pydatetime()
        if window_start <= pub <= now_utc:
            active.append(dict(row))
    return active


_RISK_TEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\btoo many requests\b", re.IGNORECASE),
    re.compile(r"\bprecondition failed\b", re.IGNORECASE),
    re.compile(r"风控"),
)
_RISK_CODE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?<!\d)-352\b", re.IGNORECASE),
    re.compile(r"\b(?:http|bilibili|status|status code|response|error code|code|errno)\s*[:=#()\s-]*-?352\b", re.IGNORECASE),
    re.compile(r"\b(?:http|bilibili|status|status code|response|error code|code|errno)\s*[:=#()\s-]*412\b", re.IGNORECASE),
    re.compile(r"\b(?:http|bilibili|status|status code|response|error code|code|errno)\s*[:=#()\s-]*429\b", re.IGNORECASE),
)


def is_risk_error(exc: BaseException) -> bool:
    message = " ".join(str(exc).split()).lower()
    return any(pattern.search(message) for pattern in (*_RISK_TEXT_PATTERNS, *_RISK_CODE_PATTERNS))


def perform_risk_sleep_seconds(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def _log_stage_start(stage_id: int, label: str) -> None:
    _LOG.info("Stage %s start: %s", stage_id, label)


def _log_stage_end(stage_id: int, label: str, **details: Any) -> None:
    parts = ", ".join(f"{k}={v}" for k, v in details.items())
    _LOG.info("Stage %s end: %s%s%s", stage_id, label, " — " if parts else "", parts)


def _log_risk_sleep_notice(stage_label: str, sleep_minutes: float) -> None:
    _LOG.warning("Risk control triggered in %s; sleeping %.4g minutes then continuing.", stage_label, sleep_minutes)


def run_stage_with_risk_retry(
    stage_label: str,
    fn: Callable[[], _T],
    *,
    sleep_minutes: float,
    sleep_fn: Callable[[float], None],
) -> _T:
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if not is_risk_error(exc):
                raise
            _log_risk_sleep_notice(stage_label, sleep_minutes)
            sleep_fn(max(0.0, float(sleep_minutes)) * 60.0)


def parse_source_type_tokens(raw: Any) -> set[str]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return set()
    text = str(raw).strip()
    if not text:
        return set()
    return {token.strip() for token in re.split(r"[|,]+", text) if token.strip()}


def normalize_source_types_field(raw: Any) -> str:
    tokens = parse_source_type_tokens(raw)
    mapped: set[str] = set()
    for token in tokens:
        mapped.add(_DISCOVERY_SOURCE_TO_CANONICAL.get(token, token))
    ordered = [label for label in SOURCE_TYPES_SERIAL_ORDER if label in mapped]
    extras = sorted(mapped.difference(SOURCE_TYPES_SERIAL_ORDER))
    return "|".join([*ordered, *extras])


def _canonical_source_label(raw: str) -> str:
    return _DISCOVERY_SOURCE_TO_CANONICAL.get(raw, raw)


def _iso_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return _ensure_utc(value).isoformat()


def _row_pubdate_in_window(row: Mapping[str, Any], now_utc: datetime, window_start: datetime) -> bool:
    ts = _to_utc_timestamp(row.get("pubdate"))
    if pd.isna(ts):
        return False
    pub = ts.to_pydatetime()
    return window_start <= pub <= now_utc


def filter_watchlist_rows_by_window(
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    time_window_hours: int,
) -> tuple[list[dict[str, Any]], int]:
    now_utc = _ensure_utc(now)
    window_start = now_utc - timedelta(hours=time_window_hours)
    kept: list[dict[str, Any]] = []
    for row in rows:
        if _row_pubdate_in_window(row, now_utc, window_start):
            kept.append(dict(row))
    return kept, len(rows)


def row_from_discovery_candidate(
    item: CandidateVideo | VideoPoolEntry | RankboardEntry,
    *,
    canonical_source: str,
    title: str = "",
) -> dict[str, Any]:
    pub = getattr(item, "pubdate", None)
    disc = getattr(item, "discovered_at", None) or datetime.now(timezone.utc)
    owner_mid = getattr(item, "owner_mid", None)
    if isinstance(pub, datetime):
        iso_pub = _iso_datetime(pub)
    elif pub:
        iso_pub = str(pub)
    else:
        iso_pub = ""
    return {
        "bvid": str(getattr(item, "bvid", "") or "").strip(),
        "owner_mid": owner_mid if owner_mid is not None else "",
        "pubdate": iso_pub,
        "title": title,
        "source_types": normalize_source_types_field(canonical_source),
        "first_discovered_at": _iso_datetime(disc) if isinstance(disc, datetime) else str(disc),
        "last_discovered_at": _iso_datetime(disc) if isinstance(disc, datetime) else str(disc),
        "last_selected_at": "",
    }


def _parse_row_timestamp(value: Any) -> datetime | None:
    ts = _to_utc_timestamp(value)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def merge_watchlist_row_dicts(
    base: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    out = dict(base)
    tokens = parse_source_type_tokens(out.get("source_types")) | parse_source_type_tokens(incoming.get("source_types"))
    normalized_tokens: set[str] = set()
    for t in tokens:
        normalized_tokens.add(_canonical_source_label(t))
    out["source_types"] = normalize_source_types_field("|".join(sorted(normalized_tokens)))

    first_candidates = [
        _parse_row_timestamp(out.get("first_discovered_at")),
        _parse_row_timestamp(incoming.get("first_discovered_at")),
    ]
    first_valid = [x for x in first_candidates if x is not None]
    if first_valid:
        out["first_discovered_at"] = _iso_datetime(min(first_valid))

    last_candidates = [
        _parse_row_timestamp(out.get("last_discovered_at")),
        _parse_row_timestamp(incoming.get("last_discovered_at")),
    ]
    last_valid = [x for x in last_candidates if x is not None]
    if last_valid:
        out["last_discovered_at"] = _iso_datetime(max(last_valid))

    if not str(out.get("title") or "").strip() and str(incoming.get("title") or "").strip():
        out["title"] = incoming.get("title", "")

    def _empty_owner(v: Any) -> bool:
        if v is None or v == "":
            return True
        try:
            return bool(pd.isna(v))
        except (TypeError, ValueError):
            return False

    if _empty_owner(out.get("owner_mid")) and not _empty_owner(incoming.get("owner_mid")):
        out["owner_mid"] = incoming.get("owner_mid")

    ipub = _parse_row_timestamp(incoming.get("pubdate"))
    opub = _parse_row_timestamp(out.get("pubdate"))
    if opub is None and ipub is not None:
        out["pubdate"] = incoming.get("pubdate", "")

    sel_in = _parse_row_timestamp(incoming.get("last_selected_at"))
    sel_out = _parse_row_timestamp(out.get("last_selected_at"))
    if sel_out is None and sel_in is not None:
        out["last_selected_at"] = incoming.get("last_selected_at", "")
    elif sel_out is not None and sel_in is not None:
        out["last_selected_at"] = _iso_datetime(max(sel_out, sel_in))

    return out


def dedupe_merge_watchlist_rows(
    ordered_segments: Sequence[tuple[str, Sequence[Mapping[str, Any]]]],
) -> tuple[list[dict[str, Any]], int]:
    """Merge rows in segment order; later segments update same ``bvid`` keys."""
    pre_count = 0
    merged: dict[str, dict[str, Any]] = {}
    key_order: list[str] = []
    for _label, rows in ordered_segments:
        for raw in rows:
            pre_count += 1
            row = dict(raw)
            bvid = str(row.get("bvid") or "").strip()
            if not bvid:
                continue
            row["source_types"] = normalize_source_types_field(row.get("source_types"))
            if bvid not in merged:
                merged[bvid] = row
                key_order.append(bvid)
            else:
                merged[bvid] = merge_watchlist_row_dicts(merged[bvid], row)
    return [merged[k] for k in key_order if k in merged], pre_count


def _watchlist_csv_columns_order(frame: pd.DataFrame) -> pd.DataFrame:
    preferred = [
        "bvid",
        "owner_mid",
        "pubdate",
        "title",
        "source_types",
        "first_discovered_at",
        "last_discovered_at",
        "last_selected_at",
    ]
    cols = [c for c in preferred if c in frame.columns]
    cols.extend(c for c in frame.columns if c not in cols)
    return frame[cols]


def export_watchlist_snapshot_csv(rows: Sequence[Mapping[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    preferred_cols = [
        "bvid",
        "owner_mid",
        "pubdate",
        "title",
        "source_types",
        "first_discovered_at",
        "last_discovered_at",
        "last_selected_at",
    ]
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        frame = pd.DataFrame(columns=preferred_cols)
    frame = _watchlist_csv_columns_order(frame)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _count_history_rows(video_data_root: Path | str | None, manual_dir: Path) -> int:
    state_path = realtime_watchlist_state_json_path(video_data_root)
    if not state_path.exists():
        return 0
    try:
        state = load_watchlist_state(video_data_root=video_data_root)
    except (FileNotFoundError, ValueError):
        return 0
    csv_path = _resolve_watchlist_csv_path(state, manual_dir)
    if not csv_path.exists():
        return 0
    return len(load_watchlist_csv(csv_path))


def _summarize_crawl_report(crawl_report: BatchCrawlReport | None) -> dict[str, Any]:
    if crawl_report is None:
        return {
            "status": "not_run",
            "task_mode": CrawlTaskMode.REALTIME_ONLY.value,
        }

    def _safe_int(value: Any) -> int:
        return int(value) if isinstance(value, (bool, int, float)) else 0

    def _safe_bool(value: Any) -> bool:
        return bool(value) if isinstance(value, bool) else False

    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        return value if isinstance(value, str) else None

    return {
        "status": "completed",
        "task_mode": _safe_str(getattr(crawl_report, "task_mode", None)) or CrawlTaskMode.REALTIME_ONLY.value,
        "total_bvids": _safe_int(getattr(crawl_report, "total_bvids", 0)),
        "processed_count": _safe_int(getattr(crawl_report, "processed_count", 0)),
        "success_count": _safe_int(getattr(crawl_report, "success_count", 0)),
        "failed_count": _safe_int(getattr(crawl_report, "failed_count", 0)),
        "remaining_count": _safe_int(getattr(crawl_report, "remaining_count", 0)),
        "completed_all": _safe_bool(getattr(crawl_report, "completed_all", False)),
        "stop_reason": _safe_str(getattr(crawl_report, "stop_reason", None)) or "",
        "session_dir": _safe_str(getattr(crawl_report, "session_dir", None)) or "",
        "logs_dir": _safe_str(getattr(crawl_report, "logs_dir", None)) or "",
        "remaining_csv_path": _safe_str(getattr(crawl_report, "remaining_csv_path", None)),
        "task_log_path": _safe_str(getattr(crawl_report, "task_log_path", None)),
        "session_state_path": _safe_str(getattr(crawl_report, "session_state_path", None)),
        "session_summary_log_path": _safe_str(getattr(crawl_report, "session_summary_log_path", None)),
    }


def _extract_crawl_report_messages(crawl_report: BatchCrawlReport | None) -> list[str]:
    if crawl_report is None:
        return []
    messages: list[str] = []
    stop_reason = str(getattr(crawl_report, "stop_reason", "") or "").strip()
    if stop_reason:
        messages.append(stop_reason)
    for summary in getattr(crawl_report, "summaries", []) or []:
        for error in getattr(summary, "errors", []) or []:
            text = str(error).strip()
            if text:
                messages.append(text)
    return messages


def _crawl_report_hits_risk(crawl_report: BatchCrawlReport | None) -> bool:
    return any(is_risk_error(RuntimeError(message)) for message in _extract_crawl_report_messages(crawl_report))


def _summarize_stage7_sleep_resume(crawl_reports: Sequence[BatchCrawlReport], *, sleep_count: int) -> dict[str, Any]:
    last_report = crawl_reports[-1] if crawl_reports else None
    return {
        "task_count": len(crawl_reports),
        "sleep_count": int(sleep_count),
        "final_remaining_count": int(getattr(last_report, "remaining_count", 0) or 0) if last_report is not None else 0,
        "final_completed_all": bool(getattr(last_report, "completed_all", False)) if last_report is not None else False,
        "final_stop_reason": str(getattr(last_report, "stop_reason", "") or "").strip() if last_report is not None else "",
        "crawl_reports": [_summarize_crawl_report(report) for report in crawl_reports],
    }


def _write_manual_crawl_state(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _load_persisted_root_watchlist_count(root_csv: Path) -> int:
    if not root_csv.exists():
        return 0
    return len(load_watchlist_csv(root_csv))


def _default_hot_fetch() -> list[CandidateVideo]:
    hot_source = BilibiliHotSource(
        ps=HOT_400_PAGE_SIZE,
        fetch_all_pages=True,
        max_pages=HOT_400_MAX_PAGES,
        request_interval_seconds=FULL_EXPORT_REQUEST_INTERVAL_SECONDS,
        request_jitter_seconds=FULL_EXPORT_REQUEST_JITTER_SECONDS,
        max_retries=FULL_EXPORT_MAX_RETRIES,
        retry_backoff_seconds=FULL_EXPORT_RETRY_BACKOFF_SECONDS,
    )
    return hot_source.fetch()


def _default_rankboard_fetch() -> list[RankboardEntry]:
    result = build_rankboard_result(load_rankboard_boards(), logger=None)
    return list(result.entries)


def _build_author_recent_source() -> BilibiliUserRecentVideoSource:
    return BilibiliUserRecentVideoSource(
        page_size=30,
        max_pages=20,
        request_interval_seconds=CUSTOM_EXPORT_REQUEST_INTERVAL_SECONDS,
        request_jitter_seconds=CUSTOM_EXPORT_REQUEST_JITTER_SECONDS,
        max_retries=CUSTOM_EXPORT_MAX_RETRIES,
        retry_backoff_seconds=CUSTOM_EXPORT_RETRY_BACKOFF_SECONDS,
    )


@dataclass(slots=True)
class AuthorDiscoveryOutcome:
    rows: list[dict[str, Any]]
    failed_owner_count: int
    risk_retry_count: int
    aborts_on_non_risk_error: bool


def _default_fetch_author_candidates_for_owner(
    owner_mid: int,
    window_start: datetime,
    window_end: datetime,
) -> list[CandidateVideo]:
    # Prefer the public author source API here so this orchestration layer does not
    # depend on VideoPoolBuilder private methods for per-owner recent discovery.
    return _build_author_recent_source().fetch_recent_videos(int(owner_mid), window_start, window_end)


def discover_author_candidates_with_owner_risk_sleep(
    owner_mids: Sequence[int],
    *,
    window_start: datetime,
    window_end: datetime,
    sleep_minutes: float,
    sleep_fn: Callable[[float], None],
    fetch_one: Callable[[int, datetime, datetime], Sequence[CandidateVideo]],
) -> AuthorDiscoveryOutcome:
    """Retry risk errors per owner; non-risk errors abort the whole author stage.

    The current implementation does not abandon owners after risk retries, so
    ``failed_owner_count`` only reflects explicitly abandoned owners and remains
    ``0`` in this implementation path.
    """
    rows: list[dict[str, Any]] = []
    risk_retry_count = 0
    for owner_mid in list(dict.fromkeys(int(mid) for mid in owner_mids)):
        done = False
        while not done:
            try:
                for cand in fetch_one(owner_mid, window_start, window_end):
                    rows.append(
                        row_from_discovery_candidate(
                            cand,
                            canonical_source="author_recent",
                        )
                    )
                done = True
            except Exception as exc:  # noqa: BLE001
                if not is_risk_error(exc):
                    raise
                risk_retry_count += 1
                _log_risk_sleep_notice(f"author discovery owner {owner_mid}", sleep_minutes)
                sleep_fn(max(0.0, float(sleep_minutes)) * 60.0)
    return AuthorDiscoveryOutcome(
        rows=rows,
        failed_owner_count=0,
        risk_retry_count=risk_retry_count,
        aborts_on_non_risk_error=True,
    )


@dataclass(slots=True)
class RealtimeWatchlistRunResult:
    status: str
    filtered_bvid_count: int
    history_input_count: int
    history_active_count: int
    author_failed_owner_count: int
    hot_raw_count: int
    hot_window_count: int
    rank_raw_count: int
    rank_window_count: int
    author_raw_count: int
    author_window_count: int
    merge_pre_dedupe_count: int
    merge_post_count: int
    filtered_csv_path: str | None
    session_dir: str | None
    root_watchlist_csv_path: str | None
    crawl_report: BatchCrawlReport | None
    stage7_task_count: int = 0
    stage7_sleep_count: int = 0


def run_realtime_watchlist_cycle(
    *,
    video_data_root: Path | str | None = None,
    now: datetime | None = None,
    time_window_hours: int = 168,
    sleep_minutes: float = 5.0,
    parallelism: int = 4,
    sleep_fn: Callable[[float], None] | None = None,
    hot_fetch_fn: Callable[[], Sequence[CandidateVideo]] | None = None,
    rankboard_fetch_fn: Callable[[], Sequence[RankboardEntry]] | None = None,
    author_fetch_fn: Callable[[int, datetime, datetime], Sequence[CandidateVideo]] | None = None,
    crawl_fn: Callable[..., BatchCrawlReport] | None = None,
    enable_media: bool = True,
    comment_limit: int = 10,
    consecutive_failure_limit: int = 10,
    gcp_config: Any | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    media_strategy: Any | None = None,
    credential: Any | None = None,
) -> RealtimeWatchlistRunResult:
    """Orchestrate discovery, merge, snapshot export, and optional realtime crawl."""
    root = Path(video_data_root) if video_data_root is not None else DEFAULT_VIDEO_DATA_OUTPUT_DIR
    manual_dir = realtime_watchlist_manual_crawls_dir(root)
    manual_dir.mkdir(parents=True, exist_ok=True)
    run_now = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = run_now - timedelta(hours=time_window_hours)
    risk_sleep = sleep_fn or perform_risk_sleep_seconds
    crawl_executor = crawl_fn or crawl_bvid_list_from_csv

    started_iso = run_now.isoformat()
    session_token = format_timestamp_token(run_now.replace(tzinfo=None) if run_now.tzinfo else run_now)
    session_dir = manual_dir / f"{SESSION_MANUAL_CRAWL_PREFIX}{session_token}"
    logs_dir = session_dir / LOGS_SUBDIR
    session_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    hot_fetch = hot_fetch_fn or _default_hot_fetch
    rank_fetch = rankboard_fetch_fn or _default_rankboard_fetch
    author_fetch_one = author_fetch_fn or _default_fetch_author_candidates_for_owner

    history_input_count = _count_history_rows(root, manual_dir)

    hot_raw: list[dict[str, Any]] = []
    rank_raw: list[dict[str, Any]] = []
    author_raw: list[dict[str, Any]] = []
    author_failed_owner_count = 0
    hot_raw_count = 0
    rank_raw_count = 0
    author_raw_count = 0
    hot_filtered: list[dict[str, Any]] = []
    rank_filtered: list[dict[str, Any]] = []
    author_filtered: list[dict[str, Any]] = []
    history_active: list[dict[str, Any]] = []
    merge_pre = 0
    merge_post = 0
    final_rows: list[dict[str, Any]] = []
    filtered_path = session_dir / FILTERED_VIDEO_LIST_NAME
    root_csv = realtime_watchlist_current_csv_path(root)
    crawl_report: BatchCrawlReport | None = None
    stage7_crawl_reports: list[BatchCrawlReport] = []
    stage7_sleep_count = 0
    current_stage: dict[str, Any] | None = None
    owner_rows: list[dict[str, Any]] = []
    author_risk_retry_count = 0

    try:
        current_stage = {"stage_id": 0, "stage_label": "load local author list"}
        owner_rows = load_local_author_list(video_data_root=root)

        current_stage = {"stage_id": 1, "stage_label": "fetch hot ranking"}
        _log_stage_start(1, "fetch hot ranking")
        hot_entries = run_stage_with_risk_retry(
            "hot ranking",
            lambda: list(hot_fetch()),
            sleep_minutes=sleep_minutes,
            sleep_fn=risk_sleep,
        )
        hot_raw_count = len(hot_entries)
        for item in hot_entries:
            hot_raw.append(row_from_discovery_candidate(item, canonical_source=_canonical_source_label(item.source_type)))
        hot_filtered, _ = filter_watchlist_rows_by_window(hot_raw, now=run_now, time_window_hours=time_window_hours)
        _log_stage_end(1, "hot ranking", raw=hot_raw_count, in_window=len(hot_filtered))

        current_stage = {"stage_id": 2, "stage_label": "fetch rankboards"}
        _log_stage_start(2, "fetch rankboards")
        rank_entries = run_stage_with_risk_retry(
            "rankboards",
            lambda: list(rank_fetch()),
            sleep_minutes=sleep_minutes,
            sleep_fn=risk_sleep,
        )
        rank_raw_count = len(rank_entries)
        for item in rank_entries:
            rank_raw.append(row_from_discovery_candidate(item, canonical_source=_canonical_source_label(item.source_type)))
        rank_filtered, _ = filter_watchlist_rows_by_window(rank_raw, now=run_now, time_window_hours=time_window_hours)
        _log_stage_end(2, "rankboards", raw=rank_raw_count, in_window=len(rank_filtered))

        current_stage = {"stage_id": 3, "stage_label": "fetch author recent videos"}
        _log_stage_start(3, "fetch author recent videos")
        author_outcome = discover_author_candidates_with_owner_risk_sleep(
            [row["owner_mid"] for row in owner_rows],
            window_start=window_start,
            window_end=run_now,
            sleep_minutes=sleep_minutes,
            sleep_fn=risk_sleep,
            fetch_one=author_fetch_one,
        )
        author_rows = author_outcome.rows
        author_failed_owner_count = author_outcome.failed_owner_count
        author_risk_retry_count = author_outcome.risk_retry_count
        author_raw_count = len(author_rows)
        author_filtered, _ = filter_watchlist_rows_by_window(author_rows, now=run_now, time_window_hours=time_window_hours)
        _log_stage_end(
            3,
            "author recent videos",
            raw=author_raw_count,
            in_window=len(author_filtered),
            failed_owners=author_failed_owner_count,
            risk_retries=author_risk_retry_count,
            non_risk_errors_abort_run=author_outcome.aborts_on_non_risk_error,
        )

        current_stage = {"stage_id": 4, "stage_label": "load persisted watchlist history"}
        _log_stage_start(4, "load persisted watchlist history")
        history_active = load_active_watchlist(video_data_root=root, now=run_now, time_window_hours=time_window_hours)
        _log_stage_end(
            4,
            "persisted watchlist history",
            input_rows=history_input_count,
            active=len(history_active),
        )

        current_stage = {"stage_id": 5, "stage_label": "merge and dedupe candidates"}
        _log_stage_start(5, "merge and dedupe candidates")
        pre_merge_rows = [*history_active, *hot_filtered, *rank_filtered, *author_filtered]
        export_watchlist_snapshot_csv(pre_merge_rows, session_dir / MERGED_BEFORE_PRUNE_NAME)
        merged_rows, merge_pre = dedupe_merge_watchlist_rows(
            (
                ("history", history_active),
                ("hot", hot_filtered),
                ("rankboard", rank_filtered),
                ("author_recent", author_filtered),
            )
        )
        merge_post = len(merged_rows)
        _log_stage_end(5, "merge and dedupe", pre_dedupe=merge_pre, deduped=merge_post)

        hot_rank_out = [*hot_filtered, *rank_filtered]
        export_watchlist_snapshot_csv(hot_rank_out, session_dir / HOT_RANKBOARD_SNAPSHOT_NAME)
        export_watchlist_snapshot_csv(author_filtered, session_dir / AUTHOR_RECENT_SNAPSHOT_NAME)

        current_stage = {"stage_id": 6, "stage_label": "build final filtered list for crawl"}
        _log_stage_start(6, "build final filtered list for crawl")
        final_rows = [dict(r) for r in merged_rows]
        if final_rows:
            for r in final_rows:
                r["last_selected_at"] = run_now.isoformat()
            status = "completed"
        else:
            status = "skipped"

        snapshot_after = session_dir / SNAPSHOT_AFTER_RUN_NAME

        export_watchlist_snapshot_csv(final_rows, filtered_path)
        export_watchlist_snapshot_csv(final_rows, snapshot_after)
        save_watchlist_csv(final_rows, root_csv)

        if final_rows:
            _log_stage_end(6, "build final filtered list for crawl", final=len(final_rows))
            current_stage = {"stage_id": 7, "stage_label": "run realtime crawl"}
            _log_stage_start(7, "run realtime crawl")
            current_csv_path = filtered_path
            while True:
                crawl_report = crawl_executor(
                    current_csv_path,
                    parallelism=parallelism,
                    enable_media=enable_media,
                    task_mode=CrawlTaskMode.REALTIME_ONLY,
                    comment_limit=comment_limit,
                    consecutive_failure_limit=consecutive_failure_limit,
                    gcp_config=gcp_config,
                    max_height=max_height,
                    chunk_size_mb=chunk_size_mb,
                    media_strategy=media_strategy,
                    credential=credential,
                    output_root_dir=root,
                    source_csv_name=Path(current_csv_path).name,
                    session_dir=session_dir,
                )
                stage7_crawl_reports.append(crawl_report)
                remaining_csv_path = str(getattr(crawl_report, "remaining_csv_path", "") or "").strip()
                if bool(getattr(crawl_report, "completed_all", False)) or int(getattr(crawl_report, "remaining_count", 0) or 0) == 0:
                    break
                if _crawl_report_hits_risk(crawl_report) and remaining_csv_path:
                    _log_risk_sleep_notice("stage 7 realtime crawl", sleep_minutes)
                    stage7_sleep_count += 1
                    risk_sleep(max(0.0, float(sleep_minutes)) * 60.0)
                    current_csv_path = Path(remaining_csv_path)
                    continue
                break
            _log_stage_end(
                7,
                "run realtime crawl",
                processed=crawl_report.processed_count,
                success=crawl_report.success_count,
                failed=crawl_report.failed_count,
                task_count=len(stage7_crawl_reports),
                sleep_count=stage7_sleep_count,
            )
        else:
            _log_stage_end(6, "build final filtered list for crawl", final=0)

        finished_iso = datetime.now(timezone.utc).isoformat()
        root_state = RealtimeWatchlistRootState(
            status=status,
            current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
            time_window_hours=int(time_window_hours),
            current_bvid_count=len(merged_rows),
            last_run_started_at=started_iso,
            last_run_finished_at=finished_iso,
            last_run_session_dir=str(session_dir.resolve()),
            last_run_status=status,
            updated_at=finished_iso,
        )
        save_watchlist_state(root_state, video_data_root=root)

        manual_state_payload: dict[str, Any] = {
            "status": status,
            "session_dir": str(session_dir.resolve()),
            "run_started_at": started_iso,
            "run_finished_at": finished_iso,
            "time_window_hours": int(time_window_hours),
            "counts": {
                "hot_raw": hot_raw_count,
                "hot_in_window": len(hot_filtered),
                "rank_raw": rank_raw_count,
                "rank_in_window": len(rank_filtered),
                "author_raw": author_raw_count,
                "author_in_window": len(author_filtered),
                "history_input_rows": history_input_count,
                "history_active": len(history_active),
                "merge_pre_dedupe": merge_pre,
                "merge_post_dedupe": merge_post,
                "final_filtered": len(final_rows),
                "author_risk_retries": author_risk_retry_count,
            },
            "paths": {
                "filtered_video_list": str((session_dir / FILTERED_VIDEO_LIST_NAME).resolve()),
                "root_watchlist_csv": str(root_csv.resolve()),
            },
            "author_failed_owner_count": author_failed_owner_count,
            "author_discovery_contract": {
                "failed_owner_count_counts_abandoned_owners": False,
                "author_failed_owner_count_note": "当前实现对风控错误会持续睡眠重试；非风控错误直接中止本轮任务，因此该计数当前不表示已放弃作者，通常为 0。",
                "non_risk_errors_abort_run": True,
            },
            "stage7_sleep_resume": _summarize_stage7_sleep_resume(stage7_crawl_reports, sleep_count=stage7_sleep_count),
            "crawl_result": _summarize_crawl_report(crawl_report),
        }
        _write_manual_crawl_state(session_dir / MANUAL_CRAWL_STATE_NAME, manual_state_payload)

        return RealtimeWatchlistRunResult(
            status=status,
            filtered_bvid_count=len(final_rows),
            history_input_count=history_input_count,
            history_active_count=len(history_active),
            author_failed_owner_count=author_failed_owner_count,
            hot_raw_count=hot_raw_count,
            hot_window_count=len(hot_filtered),
            rank_raw_count=rank_raw_count,
            rank_window_count=len(rank_filtered),
            author_raw_count=author_raw_count,
            author_window_count=len(author_filtered),
            merge_pre_dedupe_count=merge_pre,
            merge_post_count=merge_post,
            filtered_csv_path=str(filtered_path.resolve()),
            session_dir=str(session_dir.resolve()),
            root_watchlist_csv_path=str(root_csv.resolve()),
            crawl_report=crawl_report,
            stage7_task_count=len(stage7_crawl_reports),
            stage7_sleep_count=stage7_sleep_count,
        )
    except Exception as exc:
        finished_iso = datetime.now(timezone.utc).isoformat()
        persisted_root_count = _load_persisted_root_watchlist_count(root_csv)
        err_state = RealtimeWatchlistRootState(
            status="failed",
            current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
            time_window_hours=int(time_window_hours),
            current_bvid_count=persisted_root_count,
            last_run_started_at=started_iso,
            last_run_finished_at=finished_iso,
            last_run_session_dir=str(session_dir.resolve()),
            last_run_status="failed",
            updated_at=finished_iso,
        )
        try:
            save_watchlist_state(err_state, video_data_root=root)
        except Exception:
            pass
        try:
            failure_payload = {
                "status": "failed",
                "session_dir": str(session_dir.resolve()),
                "run_started_at": started_iso,
                "run_finished_at": finished_iso,
                "time_window_hours": int(time_window_hours),
                "failed_stage": current_stage,
                "error": {
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                },
                "counts": {
                    "hot_raw": hot_raw_count,
                    "hot_in_window": len(hot_filtered),
                    "rank_raw": rank_raw_count,
                    "rank_in_window": len(rank_filtered),
                    "author_raw": author_raw_count,
                    "author_in_window": len(author_filtered),
                    "history_input_rows": history_input_count,
                    "history_active": len(history_active),
                    "merge_pre_dedupe": merge_pre,
                    "merge_post_dedupe": merge_post,
                    "final_filtered": len(final_rows),
                    "author_risk_retries": author_risk_retry_count,
                },
                "paths": {
                    "filtered_video_list": str(filtered_path.resolve()),
                    "root_watchlist_csv": str(root_csv.resolve()),
                },
                "author_failed_owner_count": author_failed_owner_count,
                "author_discovery_contract": {
                    "failed_owner_count_counts_abandoned_owners": False,
                    "author_failed_owner_count_note": "当前实现对风控错误会持续睡眠重试；非风控错误直接中止本轮任务，因此该计数当前不表示已放弃作者，通常为 0。",
                    "non_risk_errors_abort_run": True,
                },
                "stage7_sleep_resume": _summarize_stage7_sleep_resume(stage7_crawl_reports, sleep_count=stage7_sleep_count),
                "crawl_result": _summarize_crawl_report(crawl_report),
            }
            _write_manual_crawl_state(session_dir / MANUAL_CRAWL_STATE_NAME, failure_payload)
        except Exception:
            pass
        raise
