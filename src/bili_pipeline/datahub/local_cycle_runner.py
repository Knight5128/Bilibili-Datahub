from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from bilibili_api import Credential

from bili_pipeline.cloud_tracker.runner import TrackerRunner
from bili_pipeline.cloud_tracker.settings import TrackerSettings
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode, GCPStorageConfig, MediaDownloadStrategy

from ..crawl_api import DEFAULT_VIDEO_DATA_OUTPUT_DIR, crawl_bvid_list_from_csv


DEFAULT_AUTO_CONFIG: dict[str, Any] = {
    "crawl_interval_hours": 2,
    "tracking_window_days": 14,
    "comment_limit": 10,
    "author_bootstrap_days": 14,
    "max_videos_per_cycle": 2000,
    "snapshot_workers": 1,
    "author_fetch_page_size": 30,
    "author_fetch_max_pages": 10,
    "author_overlap_minutes": 180,
    "lock_ttl_minutes": 150,
    "risk_pause_minutes": 60,
    "risk_pause_max_minutes": 360,
    "request_pause_seconds": 0.0,
    "status_history_limit": 20,
    "table_prefix": "tracker",
    "rankboard_csv_path": "",
    "pending_once_batch_parallelism": 1,
}


@dataclass(slots=True)
class LocalCycleRunResult:
    tracker_report: dict[str, Any]
    queue_total: int
    queue_pending: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "tracker_report": self.tracker_report,
            "queue_total": self.queue_total,
            "queue_pending": self.queue_pending,
        }


class DataHubLocalCycleRunner:
    def __init__(
        self,
        *,
        gcp_config: GCPStorageConfig,
        auto_config: dict[str, Any] | None = None,
        credential: Credential | None = None,
    ) -> None:
        self.gcp_config = gcp_config
        self.auto_config = {**DEFAULT_AUTO_CONFIG, **(auto_config or {})}
        rankboard_csv_path = str(self.auto_config.get("rankboard_csv_path") or "").strip()
        settings = TrackerSettings(
            gcp_config=gcp_config,
            crawl_interval_hours=int(self.auto_config["crawl_interval_hours"]),
            tracking_window_days=int(self.auto_config["tracking_window_days"]),
            comment_limit=int(self.auto_config["comment_limit"]),
            author_bootstrap_days=int(self.auto_config["author_bootstrap_days"]),
            max_videos_per_cycle=int(self.auto_config["max_videos_per_cycle"]),
            snapshot_workers=int(self.auto_config["snapshot_workers"]),
            author_fetch_page_size=int(self.auto_config["author_fetch_page_size"]),
            author_fetch_max_pages=int(self.auto_config["author_fetch_max_pages"]),
            author_overlap_minutes=int(self.auto_config["author_overlap_minutes"]),
            lock_ttl_minutes=int(self.auto_config["lock_ttl_minutes"]),
            risk_pause_minutes=int(self.auto_config["risk_pause_minutes"]),
            risk_pause_max_minutes=int(self.auto_config["risk_pause_max_minutes"]),
            request_pause_seconds=float(self.auto_config["request_pause_seconds"]),
            status_history_limit=int(self.auto_config["status_history_limit"]),
            table_prefix=str(self.auto_config["table_prefix"] or "tracker").strip() or "tracker",
            rankboard_csv_path=Path(rankboard_csv_path) if rankboard_csv_path else TrackerSettings.from_env().rankboard_csv_path,
        )
        self.runner = TrackerRunner(settings)
        if credential is not None:
            self.runner.credential = credential

    @property
    def tracker_store(self):
        return self.runner.tracker_store

    def run_cycle(self, *, force: bool = False) -> LocalCycleRunResult:
        report = self.runner.run_cycle(force=force)
        metrics = self.tracker_store.dashboard_metrics()
        return LocalCycleRunResult(
            tracker_report=report.to_dict(),
            queue_total=int(metrics.get("meta_media_queue_total") or 0),
            queue_pending=int(metrics.get("meta_media_queue_pending") or 0),
        )

    def status(self) -> dict[str, Any]:
        payload = self.runner.status()
        payload["metrics"] = self.tracker_store.dashboard_metrics()
        payload["meta_media_queue_rows"] = self.tracker_store.export_meta_media_queue_rows()
        return payload

    def replace_author_sources(
        self,
        *,
        owner_mids: list[int],
        source_name: str,
        payload: dict[str, Any] | None = None,
    ) -> int:
        return self.tracker_store.replace_author_sources(owner_mids=owner_mids, source_name=source_name, payload=payload)

    def pending_once_rows(self, *, limit: int | None = None, include_media: bool = True) -> list[dict[str, Any]]:
        rows = self.tracker_store.export_meta_media_queue_rows()
        pending_rows = []
        for row in rows:
            meta_pending = not bool(row.get("meta_crawled"))
            media_pending = include_media and not bool(row.get("media_crawled"))
            if meta_pending or media_pending:
                pending_rows.append(row)
        if limit is not None and limit > 0:
            return pending_rows[:limit]
        return pending_rows

    def crawl_pending_once_data(
        self,
        *,
        include_media: bool = True,
        limit: int | None = None,
        parallelism: int | None = None,
        output_root_dir: Path | str | None = None,
        credential: Credential | None = None,
        media_strategy: MediaDownloadStrategy | None = None,
        max_height: int = 1080,
        chunk_size_mb: int = 4,
    ) -> BatchCrawlReport | None:
        pending_rows = self.pending_once_rows(limit=limit, include_media=include_media)
        if not pending_rows:
            return None
        with NamedTemporaryFile("w", encoding="utf-8-sig", newline="", suffix=".csv", delete=False) as handle:
            writer = csv.DictWriter(handle, fieldnames=["bvid"])
            writer.writeheader()
            for row in pending_rows:
                writer.writerow({"bvid": str(row["bvid"])})
            temp_path = Path(handle.name)
        try:
            return crawl_bvid_list_from_csv(
                temp_path,
                parallelism=int(parallelism or self.auto_config["pending_once_batch_parallelism"]),
                enable_media=include_media,
                task_mode=CrawlTaskMode.ONCE_ONLY,
                comment_limit=int(self.auto_config["comment_limit"]),
                gcp_config=self.gcp_config,
                max_height=max_height,
                chunk_size_mb=chunk_size_mb,
                media_strategy=media_strategy,
                credential=credential or self.runner.credential,
                output_root_dir=output_root_dir or DEFAULT_VIDEO_DATA_OUTPUT_DIR,
                source_csv_name=f"pending_once_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )
        finally:
            temp_path.unlink(missing_ok=True)
