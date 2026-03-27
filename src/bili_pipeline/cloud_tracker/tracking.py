from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from bili_pipeline.collect import crawl_latest_comments, crawl_stat_snapshot
from bili_pipeline.storage import BigQueryCrawlerStore

from .settings import TrackerSettings


@dataclass(slots=True)
class SnapshotTaskResult:
    bvid: str
    stat_ok: bool
    comment_ok: bool
    stat_error: str = ""
    comment_error: str = ""
    risk_detected: bool = False
    risk_error: str = ""


def classify_tracker_error(exc: Exception) -> tuple[bool, str]:
    message = " ".join(str(exc).split())
    lowered = message.lower()
    risk_markers = (
        "352",
        "412",
        "429",
        "too many requests",
        "precondition failed",
        "风控",
    )
    transient_markers = (
        "timeout",
        "timed out",
        "network error",
        "service unavailable",
        "connection reset",
        "temporarily",
    )
    is_risk = any(marker in lowered for marker in risk_markers)
    if is_risk:
        return True, message or exc.__class__.__name__
    if any(marker in lowered for marker in transient_markers):
        return False, message or exc.__class__.__name__
    return False, message or exc.__class__.__name__


def snapshot_single_video(
    *,
    bvid: str,
    comment_limit: int,
    credential: Any | None,
    store: BigQueryCrawlerStore,
    request_pause_seconds: float = 0.0,
) -> SnapshotTaskResult:
    stat_ok = False
    comment_ok = False
    stat_error = ""
    comment_error = ""
    risk_detected = False
    risk_error = ""

    try:
        stat_snapshot = crawl_stat_snapshot(bvid, credential=credential)
        store.save_stat_snapshot(stat_snapshot)
        stat_ok = True
    except Exception as exc:  # noqa: BLE001
        risk_detected, stat_error = classify_tracker_error(exc)
        if risk_detected:
            risk_error = stat_error
        return SnapshotTaskResult(
            bvid=bvid,
            stat_ok=stat_ok,
            comment_ok=comment_ok,
            stat_error=stat_error,
            comment_error=comment_error,
            risk_detected=risk_detected,
            risk_error=risk_error,
        )

    if request_pause_seconds > 0:
        time.sleep(request_pause_seconds)

    try:
        comment_snapshot = crawl_latest_comments(bvid, limit=comment_limit, credential=credential)
        store.save_comment_snapshot(comment_snapshot)
        comment_ok = True
    except Exception as exc:  # noqa: BLE001
        risk_detected, comment_error = classify_tracker_error(exc)
        if risk_detected:
            risk_error = comment_error

    return SnapshotTaskResult(
        bvid=bvid,
        stat_ok=stat_ok,
        comment_ok=comment_ok,
        stat_error=stat_error,
        comment_error=comment_error,
        risk_detected=risk_detected,
        risk_error=risk_error,
    )


def snapshot_videos(
    *,
    bvids: list[str],
    comment_limit: int,
    credential: Any | None,
    store: BigQueryCrawlerStore,
    settings: TrackerSettings,
    on_result: Callable[[SnapshotTaskResult], None] | None = None,
) -> list[SnapshotTaskResult]:
    if not bvids:
        return []
    workers = max(1, settings.snapshot_workers)
    if workers == 1:
        results: list[SnapshotTaskResult] = []
        for bvid in bvids:
            result = snapshot_single_video(
                bvid=bvid,
                comment_limit=comment_limit,
                credential=credential,
                store=store,
                request_pause_seconds=settings.request_pause_seconds,
            )
            results.append(result)
            if on_result is not None:
                on_result(result)
            if result.risk_detected:
                break
        return results

    results: list[SnapshotTaskResult] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                snapshot_single_video,
                bvid=bvid,
                comment_limit=comment_limit,
                credential=credential,
                store=store,
                request_pause_seconds=settings.request_pause_seconds,
            ): bvid
            for bvid in bvids
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if on_result is not None:
                on_result(result)
            if result.risk_detected:
                break
    results.sort(key=lambda item: bvids.index(item.bvid))
    return results
