from __future__ import annotations

import csv
import io
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from flask import Response, current_app, jsonify, request

if TYPE_CHECKING:
    from .runner import TrackerRunner


def require_admin(view: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        runner: TrackerRunner = current_app.config["tracker_runner"]
        token = runner.settings.admin_token.strip()
        if not token:
            return view(*args, **kwargs)
        auth_header = request.headers.get("Authorization", "").strip()
        if auth_header == f"Bearer {token}":
            return view(*args, **kwargs)
        return jsonify({"error": "Unauthorized"}), 401

    return wrapped


def parse_owner_mid_upload(file_storage) -> list[int]:
    content = file_storage.read()
    text = ""
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if not text:
        raise ValueError("无法识别上传文件编码。")
    reader = csv.DictReader(io.StringIO(text))
    if "owner_mid" not in (reader.fieldnames or []):
        raise ValueError("上传的 CSV 必须包含 owner_mid 列。")
    owner_mids: list[int] = []
    seen: set[int] = set()
    for row in reader:
        raw = (row.get("owner_mid") or "").strip()
        if not raw:
            continue
        owner_mid = int(raw)
        if owner_mid not in seen:
            owner_mids.append(owner_mid)
            seen.add(owner_mid)
    if not owner_mids:
        raise ValueError("上传的 CSV 中未解析到有效的 owner_mid。")
    return owner_mids


def csv_response(filename: str, content: str) -> Response:
    return Response(
        content,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
