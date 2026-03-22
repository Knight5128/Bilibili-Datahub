from __future__ import annotations

import csv
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from bili_pipeline.collect import crawl_latest_comments as _crawl_latest_comments
from bili_pipeline.collect import crawl_stat_snapshot as _crawl_stat_snapshot
from bili_pipeline.collect import crawl_video_meta as _crawl_video_meta
from bili_pipeline.media import stream_media_to_store as _stream_media_to_store
from bili_pipeline.models import (
    BatchCrawlReport,
    CommentSnapshot,
    FullCrawlSummary,
    GCPStorageConfig,
    MediaDownloadStrategy,
    MediaResult,
    MetaResult,
    StatSnapshot,
)
from bili_pipeline.storage import BigQueryCrawlerStore


def _resolve_gcp_config(
    gcp_config: GCPStorageConfig | None = None,
    strategy: MediaDownloadStrategy | None = None,
) -> GCPStorageConfig:
    resolved = gcp_config or (strategy.gcp_config if strategy is not None else None)
    if resolved is None or not resolved.is_enabled():
        raise ValueError("缺少可用的 GCP 配置。请至少提供 BigQuery Dataset 与 GCS Bucket。")
    return resolved


def _store(gcp_config: GCPStorageConfig) -> BigQueryCrawlerStore:
    return BigQueryCrawlerStore(gcp_config)


def crawl_video_meta(
    bvid: str,
    gcp_config: GCPStorageConfig,
    credential: Any | None = None,
) -> MetaResult:
    result = _crawl_video_meta(bvid, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_video_meta(result)
    return result


def crawl_stat_snapshot(
    bvid: str,
    gcp_config: GCPStorageConfig,
    credential: Any | None = None,
) -> StatSnapshot:
    snapshot = _crawl_stat_snapshot(bvid, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_stat_snapshot(snapshot)
    return snapshot


def crawl_latest_comments(
    bvid: str,
    limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    credential: Any | None = None,
) -> CommentSnapshot:
    snapshot = _crawl_latest_comments(bvid, limit=limit, credential=credential)
    _store(_resolve_gcp_config(gcp_config)).save_comment_snapshot(snapshot)
    return snapshot


def stream_media_to_store(
    bvid: str,
    strategy: MediaDownloadStrategy,
    credential: Any | None = None,
) -> MediaResult:
    gcp_config = _resolve_gcp_config(strategy=strategy)
    return _stream_media_to_store(bvid=bvid, strategy=strategy, store=_store(gcp_config), credential=credential)


def crawl_media_assets(
    bvid: str,
    strategy: MediaDownloadStrategy | None = None,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    credential: Any | None = None,
) -> MediaResult:
    resolved_config = _resolve_gcp_config(gcp_config, strategy)
    active_strategy = strategy or MediaDownloadStrategy(
        max_height=max_height,
        chunk_size_mb=chunk_size_mb,
        storage_backend="gcs",
        gcp_config=resolved_config,
    )
    return stream_media_to_store(bvid=bvid, strategy=active_strategy, credential=credential)


def crawl_full_video_bundle(
    bvid: str,
    *,
    enable_media: bool = True,
    comment_limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    media_strategy: MediaDownloadStrategy | None = None,
    credential: Any | None = None,
) -> FullCrawlSummary:
    resolved_config = _resolve_gcp_config(gcp_config, media_strategy)
    errors: list[str] = []
    meta_result = None
    stat_snapshot = None
    comment_snapshot = None
    media_result = None

    try:
        meta_result = crawl_video_meta(bvid, gcp_config=resolved_config, credential=credential)
        meta_ok = True
    except Exception as exc:  # noqa: BLE001
        meta_ok = False
        errors.append(f"meta: {exc}")

    try:
        stat_snapshot = crawl_stat_snapshot(bvid, gcp_config=resolved_config, credential=credential)
        stat_ok = True
    except Exception as exc:  # noqa: BLE001
        stat_ok = False
        errors.append(f"stat: {exc}")

    try:
        comment_snapshot = crawl_latest_comments(
            bvid,
            limit=comment_limit,
            gcp_config=resolved_config,
            credential=credential,
        )
        comment_ok = True
    except Exception as exc:  # noqa: BLE001
        comment_ok = False
        errors.append(f"comment: {exc}")

    media_ok = not enable_media
    if enable_media:
        try:
            media_result = crawl_media_assets(
                bvid,
                strategy=media_strategy,
                gcp_config=resolved_config,
                max_height=max_height,
                chunk_size_mb=chunk_size_mb,
                credential=credential,
            )
            media_ok = True
        except Exception as exc:  # noqa: BLE001
            media_ok = False
            errors.append(f"media: {exc}")

    return FullCrawlSummary(
        bvid=bvid,
        meta_ok=meta_ok,
        stat_ok=stat_ok,
        comment_ok=comment_ok,
        media_ok=media_ok,
        snapshot_time=datetime.now(),
        errors=errors,
        meta_result=meta_result,
        stat_snapshot=stat_snapshot,
        comment_snapshot=comment_snapshot,
        media_result=media_result,
    )


def _read_bvids_from_csv(csv_path: Path | str) -> list[str]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "bvid" not in (reader.fieldnames or []):
            raise ValueError("CSV must contain a 'bvid' column.")
        items = []
        for row in reader:
            bvid = (row.get("bvid") or "").strip()
            if bvid and bvid not in items:
                items.append(bvid)
        return items


def crawl_bvid_list_from_csv(
    csv_path: Path | str,
    *,
    parallelism: int = 4,
    enable_media: bool = True,
    comment_limit: int = 10,
    gcp_config: GCPStorageConfig | None = None,
    max_height: int = 1080,
    chunk_size_mb: int = 4,
    media_strategy: MediaDownloadStrategy | None = None,
    credential: Any | None = None,
) -> BatchCrawlReport:
    resolved_config = _resolve_gcp_config(gcp_config, media_strategy)
    bvids = _read_bvids_from_csv(csv_path)
    run_id = uuid.uuid4().hex
    started_at = datetime.now()
    store = _store(resolved_config)
    store.save_run_start(
        run_id,
        mode="batch_csv",
        notes={
            "csv_path": str(csv_path),
            "parallelism": parallelism,
            "enable_media": enable_media,
            "comment_limit": comment_limit,
            "bigquery_dataset": resolved_config.bigquery_dataset,
            "gcs_bucket_name": resolved_config.gcs_bucket_name,
            "max_height": max_height,
            "storage_backend": (media_strategy.storage_backend if media_strategy is not None else "gcs"),
        },
    )

    summaries: list[FullCrawlSummary] = []

    def _worker(item_bvid: str) -> FullCrawlSummary:
        return crawl_full_video_bundle(
            item_bvid,
            enable_media=enable_media,
            comment_limit=comment_limit,
            gcp_config=resolved_config,
            max_height=max_height,
            chunk_size_mb=chunk_size_mb,
            media_strategy=media_strategy,
            credential=credential,
        )

    if parallelism <= 1:
        for bvid in bvids:
            summary = _worker(bvid)
            summaries.append(summary)
            store.save_run_item(run_id, summary)
    else:
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = {executor.submit(_worker, bvid): bvid for bvid in bvids}
            for future in as_completed(futures):
                summary = future.result()
                summaries.append(summary)
                store.save_run_item(run_id, summary)

        summaries.sort(key=lambda item: bvids.index(item.bvid))

    success_count = sum(1 for item in summaries if item.meta_ok and item.stat_ok and item.comment_ok and item.media_ok)
    failed_count = len(summaries) - success_count
    store.finalize_run(run_id, total_bvids=len(bvids), success_count=success_count, failed_count=failed_count)

    return BatchCrawlReport(
        run_id=run_id,
        total_bvids=len(bvids),
        success_count=success_count,
        failed_count=failed_count,
        started_at=started_at,
        finished_at=datetime.now(),
        summaries=summaries,
    )
