from __future__ import annotations

from datetime import datetime
import unittest

from bili_pipeline.utils.log_files import build_timestamp_marker, wrap_log_lines, wrap_log_text


class LogFilesTest(unittest.TestCase):
    def test_build_timestamp_marker_formats_iso_datetime(self) -> None:
        marker = build_timestamp_marker("begin", datetime(2026, 3, 25, 10, 30, 45))
        self.assertEqual("[TIMESTAMP][BEGIN] 2026-03-25T10:30:45", marker)

    def test_wrap_log_text_adds_begin_and_end_markers(self) -> None:
        wrapped = wrap_log_text(
            "line-1\nline-2",
            started_at="2026-03-25T10:00:00",
            finished_at="2026-03-25T10:05:00",
        )

        self.assertEqual(
            "\n".join(
                [
                    "[TIMESTAMP][BEGIN] 2026-03-25T10:00:00",
                    "line-1",
                    "line-2",
                    "[TIMESTAMP][END] 2026-03-25T10:05:00",
                    "",
                ]
            ),
            wrapped,
        )

    def test_wrap_log_lines_preserves_existing_body_lines(self) -> None:
        wrapped = wrap_log_lines(
            ["first line", "", "third line"],
            started_at="2026-03-25T11:00:00",
            finished_at="2026-03-25T11:10:00",
        )

        self.assertIn("[TIMESTAMP][BEGIN] 2026-03-25T11:00:00", wrapped)
        self.assertIn("first line", wrapped)
        self.assertIn("third line", wrapped)
        self.assertIn("[TIMESTAMP][END] 2026-03-25T11:10:00", wrapped)


if __name__ == "__main__":
    unittest.main()
