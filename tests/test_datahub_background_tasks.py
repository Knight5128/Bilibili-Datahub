from __future__ import annotations

import csv
import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path


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

from bili_pipeline.datahub.background_tasks import (
    create_background_task_dir,
    load_background_task_status,
    load_cookie_text,
    register_active_background_task,
    run_batched_crawl_from_csv,
    save_cookie_text,
    update_background_task_status,
)
from bili_pipeline.models import BatchCrawlReport, CrawlTaskMode


class DataHubBackgroundTasksTest(unittest.TestCase):
    def test_cookie_text_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cookie_path = Path(tmp_dir) / "cookie.txt"
            save_cookie_text("SESSDATA=a; bili_jct=b;", path=cookie_path)
            self.assertEqual("SESSDATA=a; bili_jct=b;", load_cookie_text(cookie_path))

    def test_background_task_status_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = create_background_task_dir(
                "manual_dynamic_batch",
                root_dir=Path(tmp_dir),
                started_at=datetime(2026, 4, 1, 12, 0, 0),
            )
            status_path = update_background_task_status(
                task_dir,
                {
                    "status": "running",
                    "task_kind": "manual_dynamic_batch",
                },
            )
            self.assertTrue(status_path.exists())
            self.assertEqual("running", load_background_task_status(task_dir)["status"])

    def test_register_active_background_task_writes_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            task_dir = Path(tmp_dir) / "task"
            task_dir.mkdir()
            registry_path = register_active_background_task(
                "manual_media",
                task_dir=task_dir,
                registry_root=Path(tmp_dir),
                pid=12345,
            )
            self.assertTrue(registry_path.exists())
            self.assertIn("manual_media", registry_path.name)

    def test_run_batched_crawl_from_csv_refreshes_cookie_per_batch_and_remaining_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "input.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV1"})
                writer.writerow({"bvid": "BV2"})
                writer.writerow({"bvid": "BV3"})

            remaining_csv = Path(tmp_dir) / "remaining.csv"
            with remaining_csv.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["bvid"])
                writer.writeheader()
                writer.writerow({"bvid": "BV2"})

            provider_calls: list[int] = []
            crawl_inputs: list[str] = []

            def provider():
                provider_calls.append(1)
                return object()

            def fake_crawl(target_csv, **kwargs):
                crawl_inputs.append(str(target_csv))
                if len(crawl_inputs) == 1:
                    return BatchCrawlReport(
                        run_id="run-1",
                        total_bvids=2,
                        processed_count=1,
                        success_count=1,
                        failed_count=1,
                        remaining_count=1,
                        started_at=datetime(2026, 4, 1, 12, 0, 0),
                        finished_at=datetime(2026, 4, 1, 12, 1, 0),
                        task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                        completed_all=False,
                        stop_reason="HTTP 412 precondition failed",
                        remaining_csv_path=str(remaining_csv),
                        session_dir=str(Path(tmp_dir) / "session"),
                        logs_dir=str(Path(tmp_dir) / "logs"),
                    )
                return BatchCrawlReport(
                    run_id=f"run-{len(crawl_inputs)}",
                    total_bvids=1,
                    processed_count=1,
                    success_count=1,
                    failed_count=0,
                    remaining_count=0,
                    started_at=datetime(2026, 4, 1, 12, 2, 0),
                    finished_at=datetime(2026, 4, 1, 12, 3, 0),
                    task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                    completed_all=True,
                    stop_reason="done",
                    session_dir=str(Path(tmp_dir) / "session"),
                    logs_dir=str(Path(tmp_dir) / "logs"),
                )

            outcome = run_batched_crawl_from_csv(
                csv_path,
                batch_size=2,
                credential_provider=provider,
                crawl_fn=fake_crawl,
                should_retry_remaining_fn=lambda report: "412" in str(report.stop_reason),
                parallelism=1,
                enable_media=False,
                task_mode=CrawlTaskMode.REALTIME_ONLY,
                session_dir=Path(tmp_dir) / "session",
                output_root_dir=Path(tmp_dir),
            )

            self.assertEqual(3, len(outcome.reports))
            self.assertEqual(3, outcome.credential_refresh_count)
            self.assertEqual(3, len(provider_calls))
            self.assertEqual(str(remaining_csv), crawl_inputs[1])


if __name__ == "__main__":
    unittest.main()
