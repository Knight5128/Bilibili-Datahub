from __future__ import annotations

import base64
import json
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any

from bilibili_api import Credential


def build_logo_data_uri(logo_path: Path) -> str | None:
    if not logo_path.exists():
        return None
    mime_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }
    mime_type = mime_types.get(logo_path.suffix.lower())
    if mime_type is None:
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def render_centered_header(st, title: str, logo_path: Path) -> None:
    safe_title = escape(title)
    logo_uri = build_logo_data_uri(logo_path)
    if logo_uri is None:
        st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.25rem;'>{safe_title}</h1>", unsafe_allow_html=True)
        return
    st.markdown(
        f"""
        <div style="display: flex; justify-content: center; align-items: center; gap: 0.75rem; margin-bottom: 0.25rem;">
            <img src="{logo_uri}" alt="{safe_title} logo" style="height: 3.5rem; width: 3.5rem; object-fit: contain;" />
            <h1 style="margin: 0;">{safe_title}</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_credential_from_cookie(cookie_text: str) -> Credential | None:
    text = (cookie_text or "").strip()
    if not text:
        return None
    parts = [item.strip() for item in text.split(";") if item.strip()]
    cookies: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        cookies[key.strip()] = value.strip()
    sessdata = cookies.get("SESSDATA")
    bili_jct = cookies.get("bili_jct")
    buvid3 = cookies.get("buvid3")
    if not (sessdata and bili_jct):
        return None
    return Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3 or "")


def load_json_config(path: Path, defaults: dict[str, Any]) -> dict[str, Any]:
    config = dict(defaults)
    if not path.exists():
        return config
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return config
    if not isinstance(payload, dict):
        return config
    for key, default_value in defaults.items():
        value = payload.get(key, default_value)
        if isinstance(default_value, bool):
            config[key] = bool(value)
        elif isinstance(default_value, int):
            config[key] = int(value)
        elif isinstance(default_value, float):
            config[key] = float(value)
        else:
            config[key] = value if isinstance(value, str) else str(value)
    return config


def save_json_config(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def display_path(path: Path, base_dir: Path | None = None) -> str:
    if base_dir is not None:
        try:
            return path.relative_to(base_dir).as_posix()
        except ValueError:
            pass
    return path.as_posix()


def append_live_log(logs: list[str], placeholder, message: str) -> None:
    logs.append(message)
    placeholder.code("\n".join(logs), language=None)


def save_timestamped_task_log(task_name: str, logs: list[str], *, log_dir: Path) -> Path | None:
    if not logs:
        return None
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_task_name = re.sub(r"[^A-Za-z0-9._-]+", "_", task_name).strip("_") or "task"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_path = log_dir / f"{timestamp}_{safe_task_name}.log"
    log_path.write_text("\n".join(logs).strip() + "\n", encoding="utf-8")
    return log_path
