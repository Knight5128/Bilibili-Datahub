from __future__ import annotations

from pathlib import Path

from bili_pipeline.models import GCPStorageConfig

from .local_cycle_runner import DEFAULT_AUTO_CONFIG


APP_DIR = Path(__file__).resolve().parents[3]
LOCAL_GCP_CONFIG_PATH = APP_DIR / ".local" / "bilibili-datahub.gcp.config.json"
LOCAL_AUTO_CONFIG_PATH = APP_DIR / ".local" / "bilibili-datahub.auto.config.json"
DEFAULT_GCP_CONFIG = {
    "gcp_project_id": "",
    "bigquery_dataset": "bili_video_data_crawler",
    "gcs_bucket_name": "",
    "gcp_region": "",
    "credentials_path": "",
    "gcs_object_prefix": "bilibili-media",
    "gcs_public_base_url": "",
}


def build_gcp_config(payload: dict[str, str]) -> GCPStorageConfig:
    return GCPStorageConfig(
        project_id=str(payload.get("gcp_project_id", "")).strip(),
        bigquery_dataset=str(payload.get("bigquery_dataset", "")).strip(),
        gcs_bucket_name=str(payload.get("gcs_bucket_name", "")).strip(),
        gcp_region=str(payload.get("gcp_region", "")).strip(),
        credentials_path=str(payload.get("credentials_path", "")).strip(),
        object_prefix=str(payload.get("gcs_object_prefix", "")).strip(),
        public_base_url=str(payload.get("gcs_public_base_url", "")).strip(),
    )


def gcp_config_to_dict(config: GCPStorageConfig) -> dict[str, str]:
    return {
        "gcp_project_id": config.project_id,
        "bigquery_dataset": config.bigquery_dataset,
        "gcs_bucket_name": config.gcs_bucket_name,
        "gcp_region": config.gcp_region,
        "credentials_path": config.credentials_path,
        "gcs_object_prefix": config.object_prefix,
        "gcs_public_base_url": config.public_base_url,
    }
