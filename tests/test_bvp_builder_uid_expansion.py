from __future__ import annotations

from datetime import datetime
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

from bili_pipeline.models import DiscoverResult, VideoPoolEntry


MODULE_PATH = Path(__file__).resolve().parent.parent / "bvp-builder.py"
SPEC = importlib.util.spec_from_file_location("bvp_builder_module", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
bvp_builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bvp_builder
SPEC.loader.exec_module(bvp_builder)


class BvpBuilderUidExpansionTest(unittest.TestCase):
    def _write_original_uids(self, session_dir: Path, owner_mids: list[int]) -> None:
        session_dir.mkdir(parents=True, exist_ok=True)
        bvp_builder._save_owner_mid_csv(owner_mids, session_dir / bvp_builder.UID_EXPANSION_ORIGINAL_UIDS_FILENAME)

    def _write_state(self, session_dir: Path, requested_window_end_at: datetime) -> None:
        bvp_builder._save_uid_expansion_state(
            session_dir,
            {"requested_window_end_at": requested_window_end_at.isoformat()},
        )

    def test_load_owner_history_cutoffs_excludes_current_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            uid_expansions_root = root_dir / bvp_builder.UID_EXPANSION_DIRNAME
            previous_session = uid_expansions_root / "uid_expansion_previous"
            current_session = uid_expansions_root / "uid_expansion_current"

            self._write_original_uids(previous_session, [101, 202])
            self._write_state(previous_session, datetime(2026, 3, 1, 10, 0, 0))
            self._write_original_uids(current_session, [101])
            self._write_state(current_session, datetime(2026, 3, 20, 10, 0, 0))

            checkpoints = bvp_builder._load_owner_history_cutoffs(
                root_dir,
                excluded_session_dirs=[current_session],
            )

            self.assertEqual(datetime(2026, 3, 1, 10, 0, 0), checkpoints[101])
            self.assertEqual(datetime(2026, 3, 1, 10, 0, 0), checkpoints[202])

    def test_drop_existing_uid_expansion_duplicates_filters_existing_bvids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            session_dir = root_dir / bvp_builder.UID_EXPANSION_DIRNAME / "uid_expansion_previous"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "videolist_part_1.csv").write_text(
                "bvid,owner_mid\nBV_DUPLICATED,101\nBV_OLD,202\n",
                encoding="utf-8",
            )
            result = DiscoverResult(
                entries=[
                    VideoPoolEntry(
                        bvid="BV_DUPLICATED",
                        source_type="author_expand",
                        source_ref="owner:101",
                        discovered_at=datetime(2026, 3, 25, 12, 0, 0),
                        last_seen_at=datetime(2026, 3, 25, 12, 0, 0),
                        owner_mid=101,
                    ),
                    VideoPoolEntry(
                        bvid="BV_NEW",
                        source_type="author_expand",
                        source_ref="owner:303",
                        discovered_at=datetime(2026, 3, 25, 12, 0, 0),
                        last_seen_at=datetime(2026, 3, 25, 12, 0, 0),
                        owner_mid=303,
                    ),
                ],
                owner_mids=[101, 303],
            )

            filtered_result, removed_count = bvp_builder._drop_existing_uid_expansion_duplicates(
                result,
                root_dir,
            )

            self.assertEqual(1, removed_count)
            self.assertEqual(["BV_NEW"], [entry.bvid for entry in filtered_result.entries])


if __name__ == "__main__":
    unittest.main()
