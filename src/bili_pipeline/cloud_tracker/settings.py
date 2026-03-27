from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from bilibili_api import Credential

from bili_pipeline.models import GCPStorageConfig


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env_str(name)
    if not raw:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = _env_str(name)
    if not raw:
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_str(name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


@dataclass(slots=True)
class TrackerSettings:
    gcp_config: GCPStorageConfig
    host: str = "0.0.0.0"
    port: int = 8080
    admin_token: str = ""
    log_level: str = "INFO"
    crawl_interval_hours: int = 2
    tracking_window_days: int = 14
    comment_limit: int = 10
    author_bootstrap_days: int = 14
    max_videos_per_cycle: int = 2000
    snapshot_workers: int = 1
    author_fetch_page_size: int = 30
    author_fetch_max_pages: int = 10
    author_overlap_minutes: int = 180
    lock_ttl_minutes: int = 150
    risk_pause_minutes: int = 60
    risk_pause_max_minutes: int = 360
    request_pause_seconds: float = 0.0
    status_history_limit: int = 20
    table_prefix: str = "tracker"
    rankboard_csv_path: Path = _project_root() / "rankboard_valid_rids.csv"

    @classmethod
    def from_env(cls) -> TrackerSettings:
        gcp_config = GCPStorageConfig(
            project_id=_env_str("GCP_PROJECT_ID"),
            bigquery_dataset=_env_str("BQ_DATASET", "bili_video_data_crawler"),
            gcs_bucket_name=_env_str("GCS_BUCKET"),
            gcp_region=_env_str("GCP_REGION"),
            credentials_path=_env_str("GOOGLE_APPLICATION_CREDENTIALS"),
            object_prefix=_env_str("GCS_OBJECT_PREFIX", "bilibili-media"),
            public_base_url=_env_str("GCS_PUBLIC_BASE_URL"),
        )
        return cls(
            gcp_config=gcp_config,
            host=_env_str("TRACKER_HOST", "0.0.0.0"),
            port=_env_int("PORT", _env_int("TRACKER_PORT", 8080)),
            admin_token=_env_str("TRACKER_ADMIN_TOKEN"),
            log_level=_env_str("TRACKER_LOG_LEVEL", "INFO"),
            crawl_interval_hours=_env_int("TRACKER_CRAWL_INTERVAL_HOURS", 2),
            tracking_window_days=_env_int("TRACKER_TRACKING_WINDOW_DAYS", 14),
            comment_limit=_env_int("TRACKER_COMMENT_LIMIT", 10),
            author_bootstrap_days=_env_int("TRACKER_AUTHOR_BOOTSTRAP_DAYS", 14),
            max_videos_per_cycle=_env_int("TRACKER_MAX_VIDEOS_PER_CYCLE", 2000),
            snapshot_workers=_env_int("TRACKER_SNAPSHOT_WORKERS", 1),
            author_fetch_page_size=_env_int("TRACKER_AUTHOR_FETCH_PAGE_SIZE", 30),
            author_fetch_max_pages=_env_int("TRACKER_AUTHOR_FETCH_MAX_PAGES", 10),
            author_overlap_minutes=_env_int("TRACKER_AUTHOR_OVERLAP_MINUTES", 180),
            lock_ttl_minutes=_env_int("TRACKER_LOCK_TTL_MINUTES", 150),
            risk_pause_minutes=_env_int("TRACKER_RISK_PAUSE_MINUTES", 60),
            risk_pause_max_minutes=_env_int("TRACKER_RISK_PAUSE_MAX_MINUTES", 360),
            request_pause_seconds=_env_float("TRACKER_REQUEST_PAUSE_SECONDS", 0.0),
            status_history_limit=_env_int("TRACKER_STATUS_HISTORY_LIMIT", 20),
            table_prefix=_env_str("TRACKER_TABLE_PREFIX", "tracker"),
            rankboard_csv_path=Path(
                _env_str("TRACKER_RANKBOARD_CSV", str(_project_root() / "rankboard_valid_rids.csv"))
            ),
        )

    def build_credential(self) -> Credential | None:
        sessdata = _env_str("BILI_SESSDATA")
        bili_jct = _env_str("BILI_BILI_JCT")
        buvid3 = _env_str("BILI_BUVID3")
        if not sessdata and not bili_jct and not buvid3:
            return None
        return Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3)

    def require_gcp(self) -> None:
        if not self.gcp_config.is_enabled():
            raise ValueError("缺少完整的 GCP 配置。请至少提供 BigQuery Dataset 与 GCS Bucket。")

    def control_defaults(self) -> dict[str, int | str | None]:
        return {
            "crawl_interval_hours": self.crawl_interval_hours,
            "tracking_window_days": self.tracking_window_days,
            "comment_limit": self.comment_limit,
            "author_bootstrap_days": self.author_bootstrap_days,
            "max_videos_per_cycle": self.max_videos_per_cycle,
            "paused_until": None,
            "pause_reason": "",
            "consecutive_risk_hits": 0,
            "last_risk_at": None,
        }

    def to_safe_dict(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "log_level": self.log_level,
            "crawl_interval_hours": self.crawl_interval_hours,
            "tracking_window_days": self.tracking_window_days,
            "comment_limit": self.comment_limit,
            "author_bootstrap_days": self.author_bootstrap_days,
            "max_videos_per_cycle": self.max_videos_per_cycle,
            "snapshot_workers": self.snapshot_workers,
            "author_fetch_page_size": self.author_fetch_page_size,
            "author_fetch_max_pages": self.author_fetch_max_pages,
            "author_overlap_minutes": self.author_overlap_minutes,
            "lock_ttl_minutes": self.lock_ttl_minutes,
            "risk_pause_minutes": self.risk_pause_minutes,
            "risk_pause_max_minutes": self.risk_pause_max_minutes,
            "request_pause_seconds": self.request_pause_seconds,
            "status_history_limit": self.status_history_limit,
            "table_prefix": self.table_prefix,
            "rankboard_csv_path": str(self.rankboard_csv_path),
            "gcp_config": self.gcp_config.to_safe_dict(),
            "has_admin_token": bool(self.admin_token),
        }
