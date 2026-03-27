FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY rankboard_valid_rids.csv ./rankboard_valid_rids.csv
COPY all_valid_tags.csv ./all_valid_tags.csv
COPY Bilibili_Cloud_Tracker_Deployment.md ./Bilibili_Cloud_Tracker_Deployment.md

RUN pip install --upgrade pip && pip install .

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 --timeout 1800 bili_pipeline.cloud_tracker.app:app"]
