from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(slots=True)
class DiscoverConfig:
    """Runtime parameters for building the video pool."""

    lookback_days: int = 90
    start_date: datetime | None = None
    end_date: datetime | None = None
    partition_tid_whitelist: set[int] = field(default_factory=set)
    partition_tid_blacklist: set[int] = field(default_factory=set)
    enable_author_backfill: bool = True
    enable_duration_filter: bool = False
    min_duration_seconds: int = 15

    def allows_tid(self, tid: int | None) -> bool:
        if tid is None:
            return True
        if tid in self.partition_tid_blacklist:
            return False
        if self.partition_tid_whitelist and tid not in self.partition_tid_whitelist:
            return False
        return True

    def allows_duration(self, duration_seconds: int | None) -> bool:
        if not self.enable_duration_filter or duration_seconds is None:
            return True
        return duration_seconds >= self.min_duration_seconds

    def resolve_time_window(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        current_time = now or datetime.now()
        start = self.start_date or (current_time - timedelta(days=self.lookback_days))
        end = self.end_date or current_time
        if start > end:
            raise ValueError("start_date 不能晚于 end_date。")
        return start, end

    def allows_pubdate(self, pubdate: datetime | None, now: datetime | None = None) -> bool:
        if pubdate is None:
            return True
        start, end = self.resolve_time_window(now)
        return start <= pubdate <= end
