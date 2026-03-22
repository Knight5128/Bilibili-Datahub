from __future__ import annotations

from datetime import datetime

from bili_pipeline.config import DiscoverConfig
from bili_pipeline.discover import (
    BilibiliHotSource,
    BilibiliUserRecentVideoSource,
    BilibiliZoneTop10Source,
    VideoPoolBuilder,
)
from bili_pipeline.models import DiscoverResult


def _summarize_exception(exc: Exception, limit: int = 160) -> str:
    summary = " ".join(str(exc).split())
    if not summary:
        return exc.__class__.__name__
    if len(summary) <= limit:
        return summary
    return f"{summary[: limit - 3]}..."


def build_real_result(
    tid: int = 17,
    lookback_days: int = 90,
    logger=None,
) -> DiscoverResult:
    hot_source = BilibiliHotSource(ps=5)
    partition_source = BilibiliZoneTop10Source(tid=tid, day=7)
    author_source = BilibiliUserRecentVideoSource(page_size=10, max_pages=5)
    candidates = []

    if logger is not None:
        logger("[INFO]: 正在抓取全站热门榜单中的视频")
    hot_candidates = hot_source.fetch()
    candidates.extend(hot_candidates)
    if logger is not None:
        logger(f"[INFO]: 全站热门榜单抓取完成，新增 {len(hot_candidates)} 条候选视频")

    if logger is not None:
        logger(f"[INFO]: 正在抓取分区 tid={tid} 的近期热门视频")
    partition_candidates = partition_source.fetch()
    candidates.extend(partition_candidates)
    if logger is not None:
        logger(f"[INFO]: 分区 tid={tid} 抓取完成，新增 {len(partition_candidates)} 条候选视频")

    builder = VideoPoolBuilder(
        config=DiscoverConfig(lookback_days=lookback_days, partition_tid_whitelist={tid}),
        hot_sources=[],
        partition_sources=[],
        author_source=author_source,
    )
    if logger is not None:
        logger(f"[INFO]: 种子抓取完成，共获得 {len(candidates)} 条候选视频，开始抓取作者扩展视频")

    def _on_author_progress(owner_mid: int, index: int, total: int, fetched_count: int) -> None:
        if logger is not None and (index == 1 or index == total or index % 10 == 0):
            logger(
                f"[INFO]: 作者扩展进度 {index}/{total}，owner_mid={owner_mid}，本次抓取 {fetched_count} 条视频。"
            )

    def _on_author_error(owner_mid: int, index: int, total: int, exc: Exception) -> None:
        if logger is not None:
            logger(
                f"[WARN]: 作者扩展失败 {index}/{total}，owner_mid={owner_mid}。原因：{_summarize_exception(exc)}"
            )

    result = builder.build_from_seed_candidates(
        candidates,
        now=datetime.now(),
        progress_callback=_on_author_progress,
        error_callback=_on_author_error,
    )
    if logger is not None:
        logger(f"[INFO]: 视频池构建完成，最终保留 {len(result.entries)} 条视频")
    return result


def print_real_result(tid: int = 17, lookback_days: int = 90, limit: int = 20) -> None:
    result = build_real_result(tid=tid, lookback_days=lookback_days)
    for entry in result.entries[:limit]:
        print(
            entry.bvid,
            entry.source_type,
            entry.owner_mid,
            entry.tid,
            entry.pubdate.isoformat() if entry.pubdate else None,
            entry.source_refs,
        )


if __name__ == "__main__":
    print_real_result()
