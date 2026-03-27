from __future__ import annotations

import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

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


_install_google_stubs()

from bili_pipeline.datahub.local_cycle_runner import DataHubLocalCycleRunner
from bili_pipeline.models import CrawlTaskMode, GCPStorageConfig


class _FakeTrackerStore:
    def __init__(self) -> None:
        self._metrics = {
            "meta_media_queue_total": 5,
            "meta_media_queue_pending": 3,
        }
        self._queue_rows = [
            {"bvid": "BV_META", "meta_crawled": False, "media_crawled": False},
            {"bvid": "BV_MEDIA", "meta_crawled": True, "media_crawled": False},
            {"bvid": "BV_DONE", "meta_crawled": True, "media_crawled": True},
        ]
        self.replace_author_sources = Mock(return_value=2)

    def dashboard_metrics(self) -> dict:
        return dict(self._metrics)

    def export_meta_media_queue_rows(self) -> list[dict]:
        return list(self._queue_rows)


class _FakeTrackerRunner:
    def __init__(self, _settings) -> None:
        self.tracker_store = _FakeTrackerStore()
        self.credential = None

    def run_cycle(self, *, force: bool = False):
        return SimpleNamespace(to_dict=lambda: {"status": "success", "force": force})

    def status(self) -> dict:
        return {"recent_runs": [], "author_source_count": 2}


class DataHubLocalCycleRunnerTest(unittest.TestCase):
    @patch("bili_pipeline.datahub.local_cycle_runner.TrackerRunner", _FakeTrackerRunner)
    def test_run_cycle_reports_queue_metrics(self) -> None:
        runner = DataHubLocalCycleRunner(
            gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
            auto_config={},
        )

        result = runner.run_cycle()

        self.assertEqual("success", result.tracker_report["status"])
        self.assertEqual(5, result.queue_total)
        self.assertEqual(3, result.queue_pending)

    @patch("bili_pipeline.datahub.local_cycle_runner.TrackerRunner", _FakeTrackerRunner)
    def test_pending_once_rows_respects_include_media(self) -> None:
        runner = DataHubLocalCycleRunner(
            gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
            auto_config={},
        )

        without_media = runner.pending_once_rows(include_media=False)
        with_media = runner.pending_once_rows(include_media=True)

        self.assertEqual(["BV_META"], [row["bvid"] for row in without_media])
        self.assertEqual(["BV_META", "BV_MEDIA"], [row["bvid"] for row in with_media])

    @patch("bili_pipeline.datahub.local_cycle_runner.crawl_bvid_list_from_csv")
    @patch("bili_pipeline.datahub.local_cycle_runner.TrackerRunner", _FakeTrackerRunner)
    def test_crawl_pending_once_data_uses_once_only_mode(self, mock_batch: Mock) -> None:
        mock_batch.return_value = SimpleNamespace(task_mode=CrawlTaskMode.ONCE_ONLY.value)
        runner = DataHubLocalCycleRunner(
            gcp_config=GCPStorageConfig(project_id="p", bigquery_dataset="d", gcs_bucket_name="b"),
            auto_config={},
        )

        runner.crawl_pending_once_data(include_media=False, limit=1)

        self.assertTrue(mock_batch.called)
        _, kwargs = mock_batch.call_args
        self.assertEqual(CrawlTaskMode.ONCE_ONLY, kwargs["task_mode"])
        self.assertFalse(kwargs["enable_media"])


if __name__ == "__main__":
    unittest.main()
