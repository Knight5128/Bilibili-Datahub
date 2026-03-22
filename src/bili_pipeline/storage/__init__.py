"""Storage backends for crawler outputs."""

from .gcp_store import BigQueryCrawlerStore, GcsMediaStore

__all__ = ["BigQueryCrawlerStore", "GcsMediaStore"]
