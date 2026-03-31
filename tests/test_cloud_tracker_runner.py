from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_auth = types.ModuleType("google.auth")
    google_auth.default = lambda scopes=None: (None, "stub-project")
    google_cloud = types.ModuleType("google.cloud")
    google_bigquery = types.ModuleType("google.cloud.bigquery")
    google_storage = types.ModuleType("google.cloud.storage")
    google_oauth2 = types.ModuleType("google.oauth2")
    google_service_account = types.ModuleType("google.oauth2.service_account")

    class _Dummy:
        def __init__(self, *args, **kwargs) -> None:
            return None

    google_bigquery.Client = _Dummy
    google_bigquery.DatasetReference = _Dummy
    google_bigquery.Dataset = _Dummy
    google_bigquery.Table = _Dummy
    google_bigquery.QueryJobConfig = _Dummy
    google_bigquery.ScalarQueryParameter = _Dummy
    google_bigquery.ArrayQueryParameter = _Dummy
    google_bigquery.SchemaField = _Dummy
    google_storage.Client = _Dummy

    class _Credentials:
        project_id = "stub-project"

        @classmethod
        def from_service_account_file(cls, *_args, **_kwargs):
            return cls()

    google_service_account.Credentials = _Credentials

    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.auth", google_auth)
    sys.modules.setdefault("google.cloud", google_cloud)
    sys.modules.setdefault("google.cloud.bigquery", google_bigquery)
    sys.modules.setdefault("google.cloud.storage", google_storage)
    sys.modules.setdefault("google.oauth2", google_oauth2)
    sys.modules.setdefault("google.oauth2.service_account", google_service_account)
    google_module.auth = google_auth
    google_module.cloud = google_cloud
    google_module.oauth2 = google_oauth2
    google_cloud.bigquery = google_bigquery
    google_cloud.storage = google_storage
    google_oauth2.service_account = google_service_account


def _install_bilibili_stubs() -> None:
    bilibili_api = types.ModuleType("bilibili_api")
    bilibili_hot = types.ModuleType("bilibili_api.hot")
    bilibili_video_zone = types.ModuleType("bilibili_api.video_zone")
    bilibili_comment = types.ModuleType("bilibili_api.comment")
    bilibili_user = types.ModuleType("bilibili_api.user")
    bilibili_video = types.ModuleType("bilibili_api.video")

    class _Credential:
        def __init__(self, *args, **kwargs) -> None:
            return None

    class _User:
        def __init__(self, *args, **kwargs) -> None:
            return None

    class _Video:
        def __init__(self, *args, **kwargs) -> None:
            return None

    class _AudioStreamDownloadURL:
        def __init__(self, *args, **kwargs) -> None:
            return None

    class _VideoStreamDownloadURL:
        def __init__(self, *args, **kwargs) -> None:
            return None

    class _VideoDownloadURLDataDetecter:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def detect_all(self) -> list[object]:
            return []

    class _VideoOrder:
        PUBDATE = "pubdate"

    class _CommentResourceType:
        VIDEO = "video"

    class _OrderType:
        LIKE = "like"

    bilibili_api.Credential = _Credential
    bilibili_api.hot = bilibili_hot
    bilibili_api.video_zone = bilibili_video_zone
    bilibili_api.comment = bilibili_comment
    bilibili_user.User = _User
    bilibili_user.VideoOrder = _VideoOrder
    bilibili_video.Video = _Video
    bilibili_video.AudioStreamDownloadURL = _AudioStreamDownloadURL
    bilibili_video.VideoStreamDownloadURL = _VideoStreamDownloadURL
    bilibili_video.VideoDownloadURLDataDetecter = _VideoDownloadURLDataDetecter
    bilibili_comment.CommentResourceType = _CommentResourceType
    bilibili_comment.OrderType = _OrderType
    sys.modules.setdefault("bilibili_api", bilibili_api)
    sys.modules.setdefault("bilibili_api.hot", bilibili_hot)
    sys.modules.setdefault("bilibili_api.video_zone", bilibili_video_zone)
    sys.modules.setdefault("bilibili_api.comment", bilibili_comment)
    sys.modules.setdefault("bilibili_api.user", bilibili_user)
    sys.modules.setdefault("bilibili_api.video", bilibili_video)


_install_google_stubs()
_install_bilibili_stubs()

from bili_pipeline.cloud_tracker.runner import TrackerRunner
from bili_pipeline.cloud_tracker.settings import TrackerSettings
from bili_pipeline.cloud_tracker.store import DiscoveredVideoRow
from bili_pipeline.models import GCPStorageConfig


class _FakeTrackerStore:
    def __init__(self, *_args, **_kwargs) -> None:
        self.upserted_rows: list[DiscoveredVideoRow] = []
        self.author_checked: list[tuple[int, bool, str]] = []
        self.persisted_logs: list[dict[str, object]] = []

    def ensure_control_row(self, defaults):
        return dict(defaults)

    def acquire_lock(self, run_id: str, ttl_minutes: int) -> bool:
        return True

    def get_control(self) -> dict[str, object]:
        return {
            "tracking_window_days": 14,
            "comment_limit": 10,
            "max_videos_per_cycle": 20,
            "author_bootstrap_days": 14,
            "paused_until": None,
            "pause_reason": "",
        }

    def list_author_sources(self) -> list[dict[str, object]]:
        return [{"owner_mid": 123}]

    def mark_author_checked(self, owner_mid: int, success: bool, error: str = "") -> None:
        self.author_checked.append((owner_mid, success, error))

    def upsert_discovered_videos(self, videos: list[DiscoveredVideoRow]) -> int:
        self.upserted_rows = list(videos)
        return len(videos)

    def expire_watchlist(self) -> int:
        return 0

    def list_active_watch_videos(self, *, limit: int) -> list[dict[str, object]]:
        return []

    def clear_risk_backoff(self) -> dict[str, object]:
        return {}

    def release_lock(self, run_id: str) -> None:
        return None

    def insert_run_log(self, **kwargs) -> None:
        self.persisted_logs.append(kwargs)


class _FakeCrawlerStore:
    def __init__(self, *_args, **_kwargs) -> None:
        return None


class TrackerRunnerDedupTest(unittest.TestCase):
    @patch("bili_pipeline.cloud_tracker.runner.snapshot_videos", return_value=[])
    @patch("bili_pipeline.cloud_tracker.runner.discover_author_videos")
    @patch("bili_pipeline.cloud_tracker.runner.discover_rankboard_videos")
    @patch("bili_pipeline.cloud_tracker.runner.BigQueryCrawlerStore", _FakeCrawlerStore)
    @patch("bili_pipeline.cloud_tracker.runner.TrackerStore", _FakeTrackerStore)
    def test_run_cycle_deduplicates_discovered_rows_before_upsert(
        self,
        mock_rankboard,
        mock_author,
        _mock_snapshot,
    ) -> None:
        discovered_at = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        shared_bvid = "BV_DUPLICATED"
        mock_rankboard.return_value = [
            DiscoveredVideoRow(
                bvid=shared_bvid,
                owner_mid=123,
                pubdate=discovered_at,
                discovered_at=discovered_at,
                tracking_deadline=discovered_at,
                status="active",
                discovery_sources=["rankboard:all"],
            )
        ]
        mock_author.return_value = (
            [
                DiscoveredVideoRow(
                    bvid=shared_bvid,
                    owner_mid=123,
                    pubdate=discovered_at,
                    discovered_at=discovered_at,
                    tracking_deadline=discovered_at,
                    status="active",
                    discovery_sources=["owner:123"],
                )
            ],
            [],
        )

        runner = TrackerRunner(
            TrackerSettings(
                gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
            )
        )

        report = runner.run_cycle()

        self.assertEqual("success", report.status)
        self.assertEqual(1, report.discovered_count)
        self.assertEqual(1, len(runner.tracker_store.upserted_rows))
        merged = runner.tracker_store.upserted_rows[0]
        self.assertEqual(shared_bvid, merged.bvid)
        self.assertEqual(["rankboard:all", "owner:123"], merged.discovery_sources)


if __name__ == "__main__":
    unittest.main()
