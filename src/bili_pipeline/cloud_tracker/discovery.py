from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from bili_pipeline.discover import BilibiliRankboardSource, BilibiliUserRecentVideoSource
from bili_pipeline.models import CandidateVideo

from .settings import TrackerSettings
from .store import DiscoveredVideoRow


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(slots=True)
class RankboardBoard:
    rid: int
    name: str
    slug: str
    url: str


def load_rankboard_boards(csv_path: Path) -> list[RankboardBoard]:
    boards: list[RankboardBoard] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            boards.append(
                RankboardBoard(
                    rid=int(row["rid"]),
                    name=row["board_name"].strip(),
                    slug=row["board_slug"].strip(),
                    url=row["board_url"].strip(),
                )
            )
    return boards


def _to_discovered_row(candidate: CandidateVideo, *, now: datetime, tracking_window_days: int) -> DiscoveredVideoRow:
    tracking_deadline = None
    status = "active"
    if candidate.pubdate is not None:
        tracking_deadline = _as_utc(candidate.pubdate) + timedelta(days=tracking_window_days)
        if tracking_deadline < now:
            status = "expired"
    return DiscoveredVideoRow(
        bvid=candidate.bvid,
        owner_mid=candidate.owner_mid,
        pubdate=_as_utc(candidate.pubdate),
        discovered_at=now,
        tracking_deadline=tracking_deadline,
        status=status,
        discovery_sources=[candidate.source_ref],
    )


def _merge_candidates(candidates: list[CandidateVideo], *, now: datetime, tracking_window_days: int) -> list[DiscoveredVideoRow]:
    merged: dict[str, DiscoveredVideoRow] = {}
    for candidate in candidates:
        row = _to_discovered_row(candidate, now=now, tracking_window_days=tracking_window_days)
        existing = merged.get(row.bvid)
        if existing is None:
            merged[row.bvid] = row
            continue
        if row.owner_mid is not None and existing.owner_mid is None:
            existing.owner_mid = row.owner_mid
        if row.pubdate is not None and existing.pubdate is None:
            existing.pubdate = row.pubdate
        if row.tracking_deadline is not None:
            existing.tracking_deadline = row.tracking_deadline
            if row.status == "active":
                existing.status = "active"
        for source in row.discovery_sources:
            if source not in existing.discovery_sources:
                existing.discovery_sources.append(source)
    return list(merged.values())


def discover_rankboard_videos(
    settings: TrackerSettings,
    *,
    logger: Callable[[str], None] | None = None,
) -> list[DiscoveredVideoRow]:
    boards = load_rankboard_boards(settings.rankboard_csv_path)
    candidates: list[CandidateVideo] = []
    for index, board in enumerate(boards, start=1):
        if logger is not None:
            logger(f"[DISCOVERY] 拉取实时排行榜 {index}/{len(boards)}：{board.name}。")
        entries = BilibiliRankboardSource(
            board_rid=board.rid,
            board_name=board.name,
            board_url=board.url,
            request_interval_seconds=max(settings.request_pause_seconds, 0.0),
            request_jitter_seconds=0.5,
            max_retries=3,
            retry_backoff_seconds=5.0,
        ).fetch()
        for entry in entries:
            candidates.append(
                CandidateVideo(
                    bvid=entry.bvid,
                    source_type="rankboard",
                    source_ref=entry.source_ref,
                    discovered_at=entry.discovered_at,
                    owner_mid=entry.owner_mid,
                    tid=entry.tid,
                    pubdate=entry.pubdate,
                    duration_seconds=entry.duration_seconds,
                    seed_score=entry.seed_score,
                )
            )
    return _merge_candidates(candidates, now=_utcnow(), tracking_window_days=settings.tracking_window_days)


def discover_author_videos(
    settings: TrackerSettings,
    *,
    author_rows: list[dict[str, Any]],
    tracking_window_days: int,
    credential: Any | None,
    logger: Callable[[str], None] | None = None,
) -> tuple[list[DiscoveredVideoRow], list[tuple[int, str]]]:
    now = _utcnow()
    source = BilibiliUserRecentVideoSource(
        page_size=settings.author_fetch_page_size,
        max_pages=settings.author_fetch_max_pages,
        request_interval_seconds=max(settings.request_pause_seconds, 0.0),
        request_jitter_seconds=0.5,
        max_retries=3,
        retry_backoff_seconds=5.0,
    )
    candidates: list[CandidateVideo] = []
    failures: list[tuple[int, str]] = []
    for index, row in enumerate(author_rows, start=1):
        owner_mid = int(row["owner_mid"])
        last_checked_at_raw = row.get("last_checked_at")
        last_checked_at = None
        if last_checked_at_raw:
            try:
                last_checked_at = datetime.fromisoformat(str(last_checked_at_raw))
                if last_checked_at.tzinfo is None:
                    last_checked_at = last_checked_at.replace(tzinfo=timezone.utc)
            except ValueError:
                last_checked_at = None
        since = now - timedelta(days=settings.author_bootstrap_days)
        if last_checked_at is not None:
            since = last_checked_at - timedelta(minutes=max(0, settings.author_overlap_minutes))
        try:
            if logger is not None:
                logger(f"[DISCOVERY] 拉取作者 {owner_mid} 的最近投稿（{index}/{len(author_rows)}）。")
            author_candidates = source.fetch_recent_videos(owner_mid, since, now)
            candidates.extend(author_candidates)
        except Exception as exc:  # noqa: BLE001
            failures.append((owner_mid, str(exc)))
    return _merge_candidates(candidates, now=now, tracking_window_days=tracking_window_days), failures
