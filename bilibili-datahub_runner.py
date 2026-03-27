from __future__ import annotations

import argparse
import json

from bili_pipeline.datahub.config import DEFAULT_AUTO_CONFIG, DEFAULT_GCP_CONFIG, LOCAL_AUTO_CONFIG_PATH, LOCAL_GCP_CONFIG_PATH, build_gcp_config
from bili_pipeline.datahub.local_cycle_runner import DataHubLocalCycleRunner
from bili_pipeline.datahub.shared import load_json_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one local Bilibili DataHub tracking cycle.")
    parser.add_argument("--force", action="store_true", help="Ignore current pause window and force one run.")
    args = parser.parse_args()

    gcp_payload = load_json_config(LOCAL_GCP_CONFIG_PATH, DEFAULT_GCP_CONFIG)
    auto_payload = load_json_config(LOCAL_AUTO_CONFIG_PATH, DEFAULT_AUTO_CONFIG)
    runner = DataHubLocalCycleRunner(gcp_config=build_gcp_config(gcp_payload), auto_config=auto_payload)
    result = runner.run_cycle(force=args.force)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    report_status = str(result.tracker_report.get("status") or "")
    return 0 if report_status in {"success", "paused", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
