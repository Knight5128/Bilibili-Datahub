from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from bili_pipeline.datahub.shared import append_live_log, save_timestamped_task_log


class _BrokenPlaceholder:
    def code(self, *_args, **_kwargs) -> None:
        raise RuntimeError("websocket closed")


class DataHubSharedLoggingTest(unittest.TestCase):
    def test_append_live_log_ignores_ui_failures_and_mirrors_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            logs: list[str] = []
            mirror_path = Path(tmp_dir) / "running.log"

            append_live_log(
                logs,
                _BrokenPlaceholder(),
                "[INFO]: still running",
                mirror_path=mirror_path,
            )

            self.assertTrue(logs)
            self.assertTrue(mirror_path.exists())
            self.assertIn("[INFO]: still running", mirror_path.read_text(encoding="utf-8"))

    def test_save_timestamped_task_log_adds_end_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            logs = ["[TIMESTAMP][BEGIN] 2026-03-28T12:00:00", "[INFO]: line"]

            saved_path = save_timestamped_task_log("unit_test", logs, log_dir=Path(tmp_dir))

            self.assertIsNotNone(saved_path)
            saved_text = saved_path.read_text(encoding="utf-8")
            self.assertIn("[TIMESTAMP][BEGIN] 2026-03-28T12:00:00", saved_text)
            self.assertIn("[TIMESTAMP][END] ", saved_text)


if __name__ == "__main__":
    unittest.main()
