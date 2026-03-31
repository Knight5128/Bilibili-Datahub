from __future__ import annotations

import io
import unittest

import pandas as pd

from bili_pipeline.utils.file_merge import deduplicate_dataframe, merge_dataframes, read_uploaded_dataframe


class _UploadedFileStub:
    def __init__(self, name: str, raw: bytes) -> None:
        self.name = name
        self._raw = raw

    def getvalue(self) -> bytes:
        return self._raw

    def read(self, *_args, **_kwargs) -> bytes:
        return self._raw

    def seek(self, *_args, **_kwargs) -> int:
        return 0


class FileMergeUtilsTest(unittest.TestCase):
    def test_merge_uses_default_bvid_desc_when_sort_keys_empty(self) -> None:
        merged = merge_dataframes(
            [
                pd.DataFrame([{"bvid": "BV1"}, {"bvid": "BV3"}]),
                pd.DataFrame([{"bvid": "BV2"}]),
            ],
            sort_keys=[],
        )
        self.assertEqual(["BV3", "BV2", "BV1"], merged["bvid"].tolist())

    def test_deduplicate_prefers_latest_time_and_keeps_first_on_equal_time(self) -> None:
        df = pd.DataFrame(
            [
                {"bvid": "BV1", "title": "older", "last_seen_at": "2026-03-10T10:00:00"},
                {"bvid": "BV1", "title": "newer", "last_seen_at": "2026-03-11T10:00:00"},
                {"bvid": "BV2", "title": "first-tie", "last_seen_at": "2026-03-12T10:00:00"},
                {"bvid": "BV2", "title": "second-tie", "last_seen_at": "2026-03-12T10:00:00"},
            ]
        )

        deduplicated, dedupe_time_column = deduplicate_dataframe(
            df,
            dedupe_keys=["bvid"],
            keep_keys=["bvid", "title"],
        )

        self.assertEqual("last_seen_at", dedupe_time_column)
        self.assertEqual(
            [
                {"bvid": "BV1", "title": "newer"},
                {"bvid": "BV2", "title": "first-tie"},
            ],
            deduplicated.to_dict("records"),
        )

    def test_read_uploaded_dataframe_supports_gb18030_csv(self) -> None:
        raw = "owner_mid,备注\n123,测试\n".encode("gb18030")
        uploaded = _UploadedFileStub("owner_mid.csv", raw)

        loaded = read_uploaded_dataframe(uploaded)

        self.assertEqual(["owner_mid", "备注"], loaded.columns.tolist())
        self.assertEqual("123", str(loaded.iloc[0]["owner_mid"]))
        self.assertEqual("测试", loaded.iloc[0]["备注"])


if __name__ == "__main__":
    unittest.main()
