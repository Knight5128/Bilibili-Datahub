"""Cloud Run tracker service for long-running Bilibili collection."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["TrackerRunner", "TrackerSettings", "create_app"]


def __getattr__(name: str) -> Any:
    if name == "create_app":
        return import_module(".app", __name__).create_app
    if name == "TrackerRunner":
        return import_module(".runner", __name__).TrackerRunner
    if name == "TrackerSettings":
        return import_module(".settings", __name__).TrackerSettings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
