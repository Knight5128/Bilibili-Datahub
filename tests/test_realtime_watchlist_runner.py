from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import bili_pipeline.datahub.realtime_watchlist_runner as realtime_watchlist_runner_mod

from bili_pipeline.datahub.realtime_watchlist_runner import (
    LocalAuthorsPersistedState,
    REALTIME_WATCHLIST_CURRENT_FILENAME,
    is_risk_error,
    load_active_watchlist,
    load_local_author_list,
    load_local_authors_state,
    load_watchlist_csv,
    parse_owner_mid_cell,
    load_watchlist_state,
    realtime_watchlist_authors_csv_path,
    realtime_watchlist_authors_state_path,
    realtime_watchlist_current_csv_path,
    realtime_watchlist_state_json_path,
    run_realtime_watchlist_cycle,
    save_local_author_list,
    save_local_authors_state,
    save_watchlist_csv,
    save_watchlist_state,
    RealtimeWatchlistRootState,
)
from bili_pipeline.models import BatchCrawlReport, CandidateVideo, CrawlTaskMode, RankboardEntry


def _video_data_root(tmp: str) -> Path:
    return Path(tmp) / "outputs" / "video_data"


_FIX_NOW = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)


def _author_candidate(bvid: str, pub: datetime, owner: int = 1) -> CandidateVideo:
    return CandidateVideo(
        bvid=bvid,
        source_type="author_expand",
        source_ref="test",
        discovered_at=pub,
        owner_mid=owner,
        pubdate=pub,
    )


def _hot_candidate(bvid: str, pub: datetime, owner: int = 1) -> CandidateVideo:
    return CandidateVideo(
        bvid=bvid,
        source_type="hot",
        source_ref="test",
        discovered_at=pub,
        owner_mid=owner,
        pubdate=pub,
    )


def _rank_entry(bvid: str, pub: datetime, owner: int = 1) -> RankboardEntry:
    return RankboardEntry(
        board_rid=1,
        board_name="b",
        board_rank=1,
        bvid=bvid,
        source_type="rankboard",
        source_ref="r",
        discovered_at=pub,
        owner_mid=owner,
        pubdate=pub,
    )


def _write_minimal_watchlist_state(root: Path) -> None:
    manual = root / "manual_crawls"
    manual.mkdir(parents=True, exist_ok=True)
    csv_path = realtime_watchlist_current_csv_path(root)
    state = RealtimeWatchlistRootState(
        status="idle",
        current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
        time_window_hours=168,
        current_bvid_count=0,
        last_run_started_at=None,
        last_run_finished_at=None,
        last_run_session_dir=None,
        last_run_status=None,
        updated_at="2026-03-29T12:00:00+00:00",
    )
    save_watchlist_state(state, video_data_root=root)
    save_watchlist_csv([], csv_path)


class RealtimeWatchlistRunnerPersistenceTest(unittest.TestCase):
    def test_parse_owner_mid_cell_accepts_excel_style_numeric_text(self) -> None:
        self.assertEqual(101, parse_owner_mid_cell("101.0"))
        self.assertEqual(101, parse_owner_mid_cell(101.0))

    def test_save_and_load_local_author_csv_deduplicates_owner_mids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            rows = [
                {"owner_mid": "101.0", "note": "a"},
                {"owner_mid": 202, "note": "b"},
                {"owner_mid": "101", "note": "dup"},
                {"owner_mid": "", "note": "blank"},
                {"owner_mid": "bad", "note": "x"},
            ]
            result = save_local_author_list(rows, video_data_root=root)
            loaded = load_local_author_list(video_data_root=root)
            self.assertEqual([101, 202], [row["owner_mid"] for row in loaded])
            self.assertEqual(2, result.author_count)
            authors_path = realtime_watchlist_authors_csv_path(root)
            self.assertEqual(str(authors_path.resolve()), result.authors_csv_path)

    def test_load_active_watchlist_prunes_rows_outside_time_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            manual = root / "manual_crawls"
            manual.mkdir(parents=True)
            csv_path = realtime_watchlist_current_csv_path(root)
            save_watchlist_csv(
                [
                    {
                        "bvid": "BV_RECENT",
                        "owner_mid": 1,
                        "pubdate": "2026-03-28T12:00:00+00:00",
                        "title": "recent",
                    },
                    {
                        "bvid": "BV_OLD",
                        "owner_mid": 2,
                        "pubdate": "2026-03-01T12:00:00+00:00",
                        "title": "old",
                    },
                ],
                csv_path,
            )
            state = RealtimeWatchlistRootState(
                status="idle",
                current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
                time_window_hours=168,
                current_bvid_count=2,
                last_run_started_at=None,
                last_run_finished_at=None,
                last_run_session_dir=None,
                last_run_status=None,
                updated_at="2026-03-29T12:00:00+00:00",
            )
            save_watchlist_state(state, video_data_root=root)
            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            active = load_active_watchlist(video_data_root=root, now=now, time_window_hours=168)
            self.assertEqual(["BV_RECENT"], [row["bvid"] for row in active])

    def test_load_active_watchlist_uses_caller_window_not_persisted_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            manual = root / "manual_crawls"
            manual.mkdir(parents=True)
            csv_path = realtime_watchlist_current_csv_path(root)
            save_watchlist_csv(
                [
                    {
                        "bvid": "BV_WITHIN_CALLER_WINDOW",
                        "owner_mid": 1,
                        "pubdate": "2026-03-27T12:00:00+00:00",
                        "title": "within caller window",
                    },
                    {
                        "bvid": "BV_OUTSIDE_CALLER_WINDOW",
                        "owner_mid": 2,
                        "pubdate": "2026-03-20T12:00:00+00:00",
                        "title": "outside caller window",
                    },
                ],
                csv_path,
            )
            state = RealtimeWatchlistRootState(
                status="idle",
                current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
                time_window_hours=336,
                current_bvid_count=2,
                last_run_started_at=None,
                last_run_finished_at=None,
                last_run_session_dir=None,
                last_run_status=None,
                updated_at="2026-03-29T12:00:00+00:00",
            )
            save_watchlist_state(state, video_data_root=root)

            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            active = load_active_watchlist(video_data_root=root, now=now, time_window_hours=48)

            self.assertEqual(["BV_WITHIN_CALLER_WINDOW"], [row["bvid"] for row in active])

    def test_load_active_watchlist_excludes_rows_after_now(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            manual = root / "manual_crawls"
            manual.mkdir(parents=True)
            csv_path = realtime_watchlist_current_csv_path(root)
            save_watchlist_csv(
                [
                    {
                        "bvid": "BV_VALID",
                        "owner_mid": 1,
                        "pubdate": "2026-03-29T11:00:00+00:00",
                        "title": "valid",
                    },
                    {
                        "bvid": "BV_FUTURE",
                        "owner_mid": 2,
                        "pubdate": "2026-03-29T13:00:00+00:00",
                        "title": "future",
                    },
                ],
                csv_path,
            )
            state = RealtimeWatchlistRootState(
                status="idle",
                current_csv_path=REALTIME_WATCHLIST_CURRENT_FILENAME,
                time_window_hours=168,
                current_bvid_count=2,
                last_run_started_at=None,
                last_run_finished_at=None,
                last_run_session_dir=None,
                last_run_status=None,
                updated_at="2026-03-29T12:00:00+00:00",
            )
            save_watchlist_state(state, video_data_root=root)

            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            active = load_active_watchlist(video_data_root=root, now=now, time_window_hours=168)

            self.assertEqual(["BV_VALID"], [row["bvid"] for row in active])

    def test_load_local_author_list_raises_when_no_valid_owner_mid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            path = realtime_watchlist_authors_csv_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("owner_mid\n\nx\n", encoding="utf-8-sig")
            with self.assertRaises(ValueError) as ctx:
                load_local_author_list(video_data_root=root)
            self.assertIn("owner_mid", str(ctx.exception).lower())

    def test_save_and_load_local_authors_state_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            state = LocalAuthorsPersistedState(
                status="ready",
                authors_csv_path=str(realtime_watchlist_authors_csv_path(root)),
                source_filename="authors_upload.csv",
                author_count=2,
                uploaded_at="2026-03-29T10:00:00+00:00",
                last_used_at="2026-03-30T11:30:00+00:00",
            )
            saved_path = save_local_authors_state(state, video_data_root=root)
            loaded = load_local_authors_state(video_data_root=root)

            self.assertEqual(realtime_watchlist_authors_state_path(root), saved_path)
            self.assertEqual(state, loaded)

    def test_load_local_authors_state_raises_on_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            path = realtime_watchlist_authors_state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{", encoding="utf-8")

            with self.assertRaises(ValueError):
                load_local_authors_state(video_data_root=root)

    def test_load_local_authors_state_raises_on_missing_required_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            path = realtime_watchlist_authors_state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "authors_csv_path": "outputs/video_data/manual_crawls/realtime_watchlist_authors.csv",
                        "author_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError) as ctx:
                load_local_authors_state(video_data_root=root)
            self.assertIn("missing required field", str(ctx.exception))

    def test_load_local_authors_state_raises_when_uploaded_or_last_used_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            path = realtime_watchlist_authors_state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            base_payload = {
                "status": "ready",
                "authors_csv_path": "outputs/video_data/manual_crawls/realtime_watchlist_authors.csv",
                "source_filename": "authors_upload.csv",
                "author_count": 2,
                "uploaded_at": "2026-03-29T10:00:00+00:00",
                "last_used_at": "2026-03-30T11:30:00+00:00",
            }

            for missing_key in ("uploaded_at", "last_used_at"):
                payload = dict(base_payload)
                payload.pop(missing_key)
                path.write_text(json.dumps(payload), encoding="utf-8")

                with self.subTest(missing_key=missing_key):
                    with self.assertRaises(ValueError) as ctx:
                        load_local_authors_state(video_data_root=root)
                    self.assertIn(missing_key, str(ctx.exception))

    def test_load_active_watchlist_raises_on_invalid_state_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            state_path = realtime_watchlist_state_json_path(root)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text("not json", encoding="utf-8")
            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            with self.assertRaises(ValueError):
                load_active_watchlist(video_data_root=root, now=now, time_window_hours=168)

    def test_load_active_watchlist_raises_on_watchlist_state_missing_required_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            state_path = realtime_watchlist_state_json_path(root)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps({"status": "idle"}), encoding="utf-8")
            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            with self.assertRaises(ValueError):
                load_active_watchlist(video_data_root=root, now=now, time_window_hours=168)

    def test_load_watchlist_state_raises_on_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            path = realtime_watchlist_state_json_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_watchlist_state(video_data_root=root)

    def test_load_active_watchlist_empty_when_state_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            (root / "manual_crawls").mkdir(parents=True)
            now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
            active = load_active_watchlist(video_data_root=root, now=now, time_window_hours=168)
            self.assertEqual([], active)


class RealtimeWatchlistOrchestrationTest(unittest.TestCase):
    def test_run_cycle_merges_history_and_new_candidates_then_calls_realtime_crawler(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 101}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            csv_path = realtime_watchlist_current_csv_path(root)
            save_watchlist_csv(
                [
                    {
                        "bvid": "BV_HIST",
                        "owner_mid": 101,
                        "pubdate": "2026-03-29T10:00:00+00:00",
                        "title": "h",
                        "source_types": "hot",
                        "first_discovered_at": "2026-03-29T10:00:00+00:00",
                        "last_discovered_at": "2026-03-29T10:00:00+00:00",
                        "last_selected_at": "",
                    }
                ],
                csv_path,
            )
            state = load_watchlist_state(video_data_root=root)
            state = RealtimeWatchlistRootState(
                status=state.status,
                current_csv_path=state.current_csv_path,
                time_window_hours=state.time_window_hours,
                current_bvid_count=1,
                last_run_started_at=state.last_run_started_at,
                last_run_finished_at=state.last_run_finished_at,
                last_run_session_dir=state.last_run_session_dir,
                last_run_status=state.last_run_status,
                updated_at=state.updated_at,
            )
            save_watchlist_state(state, video_data_root=root)

            in_window = _FIX_NOW - timedelta(hours=1)

            def fake_hot() -> list[CandidateVideo]:
                return [
                    _hot_candidate("BV_A", in_window),
                    _hot_candidate("BV_B", in_window),
                ]

            mock_crawl = MagicMock(return_value=MagicMock())

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=5.0,
                hot_fetch_fn=fake_hot,
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=mock_crawl,
                sleep_fn=lambda _s: None,
            )

            self.assertEqual(3, result.filtered_bvid_count)
            self.assertEqual("completed", result.status)
            mock_crawl.assert_called_once()
            self.assertEqual(CrawlTaskMode.REALTIME_ONLY, mock_crawl.call_args.kwargs["task_mode"])

    def test_stage7_crawl_failure_keeps_root_state_count_aligned_with_root_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 112}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)

            def fail_crawl(*_args, **_kwargs):
                raise RuntimeError("crawl failed after handoff")

            with self.assertRaises(RuntimeError):
                run_realtime_watchlist_cycle(
                    video_data_root=root,
                    now=_FIX_NOW,
                    time_window_hours=48,
                    hot_fetch_fn=lambda: [_hot_candidate("BV_KEEP", t)],
                    rankboard_fetch_fn=lambda: [],
                    author_fetch_fn=lambda *_: [],
                    crawl_fn=fail_crawl,
                    sleep_fn=lambda _s: None,
                )

            root_rows = load_watchlist_csv(realtime_watchlist_current_csv_path(root))
            root_state = load_watchlist_state(video_data_root=root)
            self.assertEqual(1, len(root_rows))
            self.assertEqual(len(root_rows), root_state.current_bvid_count)
            self.assertEqual("failed", root_state.last_run_status)

    def test_manual_crawl_state_includes_crawl_summary_and_stage7_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 111}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)
            report = BatchCrawlReport(
                run_id="run-1",
                total_bvids=1,
                processed_count=1,
                success_count=1,
                failed_count=0,
                remaining_count=0,
                started_at=_FIX_NOW,
                finished_at=_FIX_NOW,
                task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                completed_all=True,
                session_dir="session-dir",
                logs_dir="logs-dir",
            )

            with self.assertLogs(realtime_watchlist_runner_mod.__name__, level="INFO") as logs:
                result = run_realtime_watchlist_cycle(
                    video_data_root=root,
                    now=_FIX_NOW,
                    time_window_hours=48,
                    hot_fetch_fn=lambda: [_hot_candidate("BV_LOG", t)],
                    rankboard_fetch_fn=lambda: [],
                    author_fetch_fn=lambda *_: [],
                    crawl_fn=MagicMock(return_value=report),
                    sleep_fn=lambda _s: None,
                )

            state_path = Path(result.session_dir) / "manual_crawl_state.json"
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual("completed", payload["status"])
            self.assertEqual("completed", payload["crawl_result"]["status"])
            self.assertEqual(CrawlTaskMode.REALTIME_ONLY.value, payload["crawl_result"]["task_mode"])
            self.assertEqual(1, payload["crawl_result"]["processed_count"])
            self.assertFalse(payload["author_discovery_contract"]["failed_owner_count_counts_abandoned_owners"])
            self.assertIn("当前实现对风控错误会持续睡眠重试", payload["author_discovery_contract"]["author_failed_owner_count_note"])
            joined_logs = "\n".join(logs.output)
            self.assertIn("Stage 7 start: run realtime crawl", joined_logs)
            self.assertIn("Stage 7 end: run realtime crawl", joined_logs)

    def test_stage7_risk_stop_sleeps_and_retries_remaining_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 114}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)
            remaining_csv = str(root / "manual_crawls" / "manual_crawl_20260329_120000" / "remaining_bvids_part_1.csv")
            crawl_inputs: list[str] = []

            def fake_crawl(csv_path, **_kwargs):
                crawl_inputs.append(str(csv_path))
                if len(crawl_inputs) == 1:
                    return BatchCrawlReport(
                        run_id="run-risk",
                        total_bvids=2,
                        processed_count=1,
                        success_count=0,
                        failed_count=1,
                        remaining_count=1,
                        started_at=_FIX_NOW,
                        finished_at=_FIX_NOW,
                        task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                        completed_all=False,
                        stop_reason="检测到风控错误 code=352，已导出 remaining CSV 供继续执行。",
                        session_dir="session-dir",
                        logs_dir="logs-dir",
                        remaining_csv_path=remaining_csv,
                    )
                return BatchCrawlReport(
                    run_id="run-final",
                    total_bvids=1,
                    processed_count=1,
                    success_count=1,
                    failed_count=0,
                    remaining_count=0,
                    started_at=_FIX_NOW,
                    finished_at=_FIX_NOW,
                    task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                    completed_all=True,
                    stop_reason="当前 batch_crawl 已完成，所有视频均已成功抓取。",
                    session_dir="session-dir",
                    logs_dir="logs-dir",
                )

            sleep_mock = MagicMock()
            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=lambda: [_hot_candidate("BV_STAGE7", t)],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=fake_crawl,
                sleep_minutes=7.0,
                sleep_fn=sleep_mock,
            )

            self.assertEqual(2, len(crawl_inputs))
            self.assertTrue(crawl_inputs[0].endswith("filtered_video_list.csv"))
            self.assertEqual(remaining_csv, crawl_inputs[1])
            sleep_mock.assert_called_once_with(420.0)
            self.assertTrue(result.crawl_report is not None)
            self.assertTrue(result.crawl_report.completed_all)

    def test_manual_crawl_state_records_stage7_sleep_resume_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 115}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)
            remaining_csv = str(root / "manual_crawls" / "manual_crawl_20260329_120001" / "remaining_bvids_part_1.csv")
            call_count = {"count": 0}

            def fake_crawl(_csv_path, **_kwargs):
                call_count["count"] += 1
                if call_count["count"] == 1:
                    return BatchCrawlReport(
                        run_id="run-risk",
                        total_bvids=2,
                        processed_count=1,
                        success_count=0,
                        failed_count=1,
                        remaining_count=1,
                        started_at=_FIX_NOW,
                        finished_at=_FIX_NOW,
                        task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                        completed_all=False,
                        stop_reason="bilibili 412 precondition failed",
                        session_dir="session-dir",
                        logs_dir="logs-dir",
                        remaining_csv_path=remaining_csv,
                    )
                return BatchCrawlReport(
                    run_id="run-final",
                    total_bvids=1,
                    processed_count=1,
                    success_count=1,
                    failed_count=0,
                    remaining_count=0,
                    started_at=_FIX_NOW,
                    finished_at=_FIX_NOW,
                    task_mode=CrawlTaskMode.REALTIME_ONLY.value,
                    completed_all=True,
                    stop_reason="当前 batch_crawl 已完成，所有视频均已成功抓取。",
                    session_dir="session-dir",
                    logs_dir="logs-dir",
                )

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=lambda: [_hot_candidate("BV_STATE", t)],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=fake_crawl,
                sleep_minutes=5.0,
                sleep_fn=lambda _s: None,
            )

            payload = json.loads((Path(result.session_dir) / "manual_crawl_state.json").read_text(encoding="utf-8"))
            self.assertEqual(2, payload["stage7_sleep_resume"]["task_count"])
            self.assertEqual(1, payload["stage7_sleep_resume"]["sleep_count"])
            self.assertEqual(0, payload["stage7_sleep_resume"]["final_remaining_count"])
            self.assertEqual(2, len(payload["stage7_sleep_resume"]["crawl_reports"]))

    def test_hot_stage_risk_error_sleeps_then_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 113}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)
            hot_calls: list[int] = []

            def fake_hot() -> list[CandidateVideo]:
                hot_calls.append(1)
                if len(hot_calls) == 1:
                    raise RuntimeError("status 429 too many requests")
                return [_hot_candidate("BV_HOT", t)]

            sleep_mock = MagicMock()

            run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=2.0,
                hot_fetch_fn=fake_hot,
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=sleep_mock,
            )

            sleep_mock.assert_called_once_with(120.0)
            self.assertEqual(2, len(hot_calls))

    def test_failure_writes_manual_crawl_state_before_reraising(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 121}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            with self.assertRaises(RuntimeError):
                run_realtime_watchlist_cycle(
                    video_data_root=root,
                    now=_FIX_NOW,
                    time_window_hours=48,
                    hot_fetch_fn=lambda: (_ for _ in ()).throw(RuntimeError("hot failed")),
                    rankboard_fetch_fn=lambda: [],
                    author_fetch_fn=lambda *_: [],
                    crawl_fn=MagicMock(),
                    sleep_fn=lambda _s: None,
                )

            session_dirs = sorted((root / "manual_crawls").glob("manual_crawl_*"))
            self.assertTrue(session_dirs)
            payload = json.loads((session_dirs[-1] / "manual_crawl_state.json").read_text(encoding="utf-8"))
            self.assertEqual("failed", payload["status"])
            self.assertEqual(1, payload["failed_stage"]["stage_id"])
            self.assertEqual("RuntimeError", payload["error"]["type"])
            self.assertIn("hot failed", payload["error"]["message"])

    def test_run_cycle_aggregates_source_types_for_duplicate_bvid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 201}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            same = _FIX_NOW - timedelta(hours=2)

            def fake_hot() -> list[CandidateVideo]:
                return [_hot_candidate("BV_X", same)]

            def fake_rank() -> list[RankboardEntry]:
                return [_rank_entry("BV_X", same)]

            def fake_author(_mid: int, _s: datetime, _e: datetime) -> list[CandidateVideo]:
                return [_author_candidate("BV_X", same, owner=201)]

            run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=5.0,
                hot_fetch_fn=fake_hot,
                rankboard_fetch_fn=fake_rank,
                author_fetch_fn=fake_author,
                crawl_fn=MagicMock(),
                sleep_fn=lambda _s: None,
            )

            rows = load_watchlist_csv(realtime_watchlist_current_csv_path(root))
            self.assertEqual(1, len(rows))
            self.assertEqual("author_recent|hot|rankboard", rows[0]["source_types"])

    def test_filtered_video_list_places_bvid_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 301}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            t = _FIX_NOW - timedelta(hours=1)

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=lambda: [_hot_candidate("BV_COL", t)],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=lambda _s: None,
            )

            with Path(result.filtered_csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
                header = next(csv.reader(handle))
            self.assertEqual("bvid", header[0])

    def test_run_cycle_skips_realtime_crawl_when_final_list_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 401}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            mock_crawl = MagicMock()

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=lambda: [],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=mock_crawl,
                sleep_fn=lambda _s: None,
            )

            self.assertEqual("skipped", result.status)
            self.assertEqual(0, result.filtered_bvid_count)
            mock_crawl.assert_not_called()
            payload = json.loads((Path(result.session_dir) / "manual_crawl_state.json").read_text(encoding="utf-8"))
            self.assertEqual("not_run", payload["crawl_result"]["status"])
            self.assertEqual(CrawlTaskMode.REALTIME_ONLY.value, payload["crawl_result"]["task_mode"])

    def test_run_cycle_filters_discovery_rows_then_merges_pruned_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 501}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            csv_path = realtime_watchlist_current_csv_path(root)
            save_watchlist_csv(
                [
                    {
                        "bvid": "BV_RECENT",
                        "owner_mid": 501,
                        "pubdate": "2026-03-29T10:00:00+00:00",
                        "title": "r",
                        "source_types": "hot",
                        "first_discovered_at": "2026-03-29T10:00:00+00:00",
                        "last_discovered_at": "2026-03-29T10:00:00+00:00",
                        "last_selected_at": "",
                    },
                    {
                        "bvid": "BV_OLD",
                        "owner_mid": 501,
                        "pubdate": "2026-03-01T10:00:00+00:00",
                        "title": "o",
                        "source_types": "hot",
                        "first_discovered_at": "2026-03-01T10:00:00+00:00",
                        "last_discovered_at": "2026-03-01T10:00:00+00:00",
                        "last_selected_at": "",
                    },
                ],
                csv_path,
            )
            st = load_watchlist_state(video_data_root=root)
            save_watchlist_state(
                RealtimeWatchlistRootState(
                    status=st.status,
                    current_csv_path=st.current_csv_path,
                    time_window_hours=st.time_window_hours,
                    current_bvid_count=2,
                    last_run_started_at=st.last_run_started_at,
                    last_run_finished_at=st.last_run_finished_at,
                    last_run_session_dir=st.last_run_session_dir,
                    last_run_status=st.last_run_status,
                    updated_at=st.updated_at,
                ),
                video_data_root=root,
            )

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=lambda: [],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=lambda _s: None,
            )

            self.assertEqual(2, result.history_input_count)
            self.assertEqual(1, result.history_active_count)
            self.assertEqual(1, result.filtered_bvid_count)

    def test_discovery_respects_caller_time_window_for_pubdate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 601}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            inside = _FIX_NOW - timedelta(hours=47)
            outside = _FIX_NOW - timedelta(hours=49)

            def fake_hot() -> list[CandidateVideo]:
                return [
                    _hot_candidate("BV_IN", inside),
                    _hot_candidate("BV_OUT", outside),
                ]

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                hot_fetch_fn=fake_hot,
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=lambda _s: None,
            )

            self.assertEqual(2, result.hot_raw_count)
            self.assertEqual(1, result.hot_window_count)
            self.assertEqual(1, result.filtered_bvid_count)

    def test_future_pubdate_rows_are_excluded_from_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 701}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            future = _FIX_NOW + timedelta(hours=1)

            def fake_hot() -> list[CandidateVideo]:
                return [_hot_candidate("BV_FUT", future)]

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=168,
                hot_fetch_fn=fake_hot,
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=lambda _s: None,
            )

            self.assertEqual(0, result.hot_window_count)
            self.assertEqual("skipped", result.status)

    def test_rankboard_risk_error_sleeps_then_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 801}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            t = _FIX_NOW - timedelta(hours=1)
            rank_calls: list[int] = []

            def fake_rank() -> list[RankboardEntry]:
                rank_calls.append(1)
                if len(rank_calls) == 1:
                    raise RuntimeError("HTTP 412 precondition failed")
                return [_rank_entry("BV_R", t)]

            sleep_mock = MagicMock()

            run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=5.0,
                hot_fetch_fn=lambda: [],
                rankboard_fetch_fn=fake_rank,
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=sleep_mock,
            )

            sleep_mock.assert_called_once_with(300.0)
            self.assertEqual(2, len(rank_calls))

    def test_rankboard_negative_352_error_sleeps_then_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 802}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            t = _FIX_NOW - timedelta(hours=1)
            rank_calls: list[int] = []

            def fake_rank() -> list[RankboardEntry]:
                rank_calls.append(1)
                if len(rank_calls) == 1:
                    raise ValueError("排行榜接口返回异常: -352")
                return [_rank_entry("BV_R352", t)]

            sleep_mock = MagicMock()

            run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=5.0,
                hot_fetch_fn=lambda: [],
                rankboard_fetch_fn=fake_rank,
                author_fetch_fn=lambda *_: [],
                crawl_fn=MagicMock(),
                sleep_fn=sleep_mock,
            )

            sleep_mock.assert_called_once_with(300.0)
            self.assertEqual(2, len(rank_calls))

    def test_author_discovery_risk_error_sleeps_then_resumes_remaining_owners(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list(
                [{"owner_mid": 901}, {"owner_mid": 902}],
                video_data_root=root,
            )
            _write_minimal_watchlist_state(root)

            t = _FIX_NOW - timedelta(hours=1)
            attempts: dict[int, int] = {901: 0, 902: 0}

            def fake_author(mid: int, _s: datetime, _e: datetime) -> list[CandidateVideo]:
                attempts[mid] = attempts.get(mid, 0) + 1
                if mid == 901 and attempts[mid] == 1:
                    raise RuntimeError("bilibili 412")
                if mid == 901:
                    return [_author_candidate("BV_901", t, owner=901)]
                return [_author_candidate("BV_902", t, owner=902)]

            sleep_mock = MagicMock()

            result = run_realtime_watchlist_cycle(
                video_data_root=root,
                now=_FIX_NOW,
                time_window_hours=48,
                sleep_minutes=3.0,
                hot_fetch_fn=lambda: [],
                rankboard_fetch_fn=lambda: [],
                author_fetch_fn=fake_author,
                crawl_fn=MagicMock(),
                sleep_fn=sleep_mock,
            )

            self.assertEqual(0, result.author_failed_owner_count)
            self.assertEqual(2, result.filtered_bvid_count)
            sleep_mock.assert_called_once_with(180.0)

    def test_author_stage3_log_makes_failure_contract_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 903}], video_data_root=root)
            _write_minimal_watchlist_state(root)
            t = _FIX_NOW - timedelta(hours=1)
            attempts = {"count": 0}

            def fake_author(_mid: int, _s: datetime, _e: datetime) -> list[CandidateVideo]:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("bilibili 412")
                return [_author_candidate("BV_AU", t, owner=903)]

            with self.assertLogs(realtime_watchlist_runner_mod.__name__, level="INFO") as logs:
                result = run_realtime_watchlist_cycle(
                    video_data_root=root,
                    now=_FIX_NOW,
                    time_window_hours=48,
                    sleep_minutes=3.0,
                    hot_fetch_fn=lambda: [],
                    rankboard_fetch_fn=lambda: [],
                    author_fetch_fn=fake_author,
                    crawl_fn=MagicMock(),
                    sleep_fn=lambda _s: None,
                )

            self.assertEqual(0, result.author_failed_owner_count)
            joined_logs = "\n".join(logs.output)
            self.assertIn("Stage 3 end: author recent videos", joined_logs)
            self.assertIn("failed_owners=0", joined_logs)
            self.assertIn("risk_retries=1", joined_logs)
            self.assertIn("non_risk_errors_abort_run=True", joined_logs)

    def test_is_risk_error_may_be_patched_to_disable_sleep_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _video_data_root(tmp)
            save_local_author_list([{"owner_mid": 811}], video_data_root=root)
            _write_minimal_watchlist_state(root)

            def fake_rank() -> list[RankboardEntry]:
                raise RuntimeError("HTTP 412")

            sleep_mock = MagicMock()
            with patch.object(realtime_watchlist_runner_mod, "is_risk_error", return_value=False):
                with self.assertRaises(RuntimeError):
                    run_realtime_watchlist_cycle(
                        video_data_root=root,
                        now=_FIX_NOW,
                        time_window_hours=48,
                        sleep_minutes=5.0,
                        hot_fetch_fn=lambda: [],
                        rankboard_fetch_fn=fake_rank,
                        author_fetch_fn=lambda *_: [],
                        crawl_fn=MagicMock(),
                        sleep_fn=sleep_mock,
                    )
            sleep_mock.assert_not_called()

    def test_is_risk_error_avoids_generic_number_false_positive(self) -> None:
        self.assertFalse(is_risk_error(RuntimeError("processed 429 rows successfully")))
        self.assertTrue(is_risk_error(RuntimeError("HTTP 429 too many requests")))
        self.assertTrue(is_risk_error(RuntimeError("排行榜接口返回异常: -352")))


if __name__ == "__main__":
    unittest.main()
