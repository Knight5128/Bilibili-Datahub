from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Callable

import pandas as pd

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.models import DiscoverResult

from .bilibili_sources import BilibiliHotSource, BilibiliWeeklyHotSource, BilibiliZoneRecentVideosSource
from .builder import VideoPoolBuilder


@dataclass(slots=True)
class _NoopAuthorVideoSource:
    def fetch_recent_videos(self, owner_mid: int, since: datetime) -> list:
        return []


def load_valid_partition_tids(csv_path: str | Path) -> list[int]:
    csv_path = Path(csv_path)
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    else:
        raise ValueError(f"无法读取有效分区列表：{csv_path}") from last_error

    if "tid" not in df.columns:
        raise ValueError("all_valid_tags.csv 中缺少 tid 列。")

    tids: list[int] = []
    seen: set[int] = set()
    for value in pd.to_numeric(df["tid"], errors="coerce").dropna().astype(int):
        if value in seen:
            continue
        seen.add(value)
        tids.append(value)
    return tids


def build_full_site_result(
    lookback_days: int,
    valid_tids: list[int],
    logger: Callable[[str], None] | None = None,
    *,
    hot_page_size: int = 20,
    hot_max_pages: int = 20,
    partition_page_size: int = 30,
    partition_max_pages: int = 200,
    request_interval_seconds: float = 0.0,
    request_jitter_seconds: float = 0.0,
    max_retries: int = 0,
    retry_backoff_seconds: float = 3.0,
    partition_batch_size: int = 0,
    partition_batch_pause_seconds: float = 0.0,
) -> DiscoverResult:
    now = datetime.now()
    since = now - timedelta(days=lookback_days)
    weeks_to_fetch = lookback_days // 7 + 1
    candidates = []
    skipped_partition_tids: list[int] = []

    hot_source = BilibiliHotSource(
        ps=hot_page_size,
        fetch_all_pages=True,
        max_pages=hot_max_pages,
        request_interval_seconds=request_interval_seconds,
        request_jitter_seconds=request_jitter_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    _collect_source_candidates("全站热门榜单", hot_source, candidates, logger)

    for week in range(1, weeks_to_fetch + 1):
        _collect_source_candidates(
            f"第 {week} 期每周必看",
            BilibiliWeeklyHotSource(
                week=week,
                request_interval_seconds=request_interval_seconds,
                request_jitter_seconds=request_jitter_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            ),
            candidates,
            logger,
        )

    tid_batches = _chunk_list(valid_tids, partition_batch_size)
    for batch_index, tid_batch in enumerate(tid_batches, start=1):
        if logger is not None and len(tid_batches) > 1:
            logger(
                f"[INFO]: 开始处理第 {batch_index}/{len(tid_batches)} 批分区，共 {len(tid_batch)} 个 tid。"
            )

        for tid in tid_batch:
            try:
                _collect_source_candidates(
                    f"分区 tid={tid}",
                    BilibiliZoneRecentVideosSource(
                        tid=tid,
                        since=since,
                        page_size=partition_page_size,
                        max_pages=partition_max_pages,
                        request_interval_seconds=request_interval_seconds,
                        request_jitter_seconds=request_jitter_seconds,
                        max_retries=max_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                    ),
                    candidates,
                    logger,
                )
            except Exception as exc:  # noqa: BLE001
                if not _is_skippable_partition_error(exc):
                    raise
                skipped_partition_tids.append(tid)
                if logger is not None:
                    logger(
                        "[WARN]: 分区 "
                        f"tid={tid} 抓取失败，已自动跳过。原因：{_summarize_exception(exc)}"
                    )

        if batch_index < len(tid_batches) and partition_batch_pause_seconds > 0:
            if logger is not None:
                logger(
                    f"[INFO]: 分区批次间暂停 {int(partition_batch_pause_seconds)} 秒，降低请求频率。"
                )
            time.sleep(partition_batch_pause_seconds)

    if logger is not None:
        logger(f"[INFO]: 种子抓取完成，共获得 {len(candidates)} 条候选视频，开始按 bvid 去重")

    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=lookback_days, enable_author_backfill=False),
        hot_sources=[],
        partition_sources=[],
        author_source=_NoopAuthorVideoSource(),
    )
    result = builder.build_from_seed_candidates(candidates, now=now)

    if logger is not None:
        logger(f"[INFO]: 去重完成，最终保留 {len(result.entries)} 条视频")
        if skipped_partition_tids:
            skipped_text = ", ".join(str(tid) for tid in skipped_partition_tids)
            logger(f"[INFO]: 本次全量抓取中已略过以下分区 tid：{skipped_text}")
    return result


def _collect_source_candidates(
    label: str,
    source,
    collector: list,
    logger: Callable[[str], None] | None,
) -> None:
    if logger is not None:
        logger(f"[INFO]: 正在抓取{label}中的视频")
    fetched = source.fetch()
    collector.extend(fetched)
    if logger is not None:
        logger(f"[INFO]: {label}抓取完成，新增 {len(fetched)} 条候选视频")


def _chunk_list(values: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        return [values]
    return [values[start : start + chunk_size] for start in range(0, len(values), chunk_size)]


def _is_skippable_partition_error(exc: Exception) -> bool:
    message = " ".join(str(exc).split())
    lowered = message.lower()
    return "-404" in lowered and "啥都木有" in message


def _summarize_exception(exc: Exception, limit: int = 160) -> str:
    summary = " ".join(str(exc).split())
    if not summary:
        return exc.__class__.__name__
    if len(summary) <= limit:
        return summary
    return f"{summary[: limit - 3]}..."
