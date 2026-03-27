from __future__ import annotations

from .app import create_app
from .settings import TrackerSettings


def main() -> None:
    settings = TrackerSettings.from_env()
    app = create_app(settings)
    app.run(host=settings.host, port=settings.port, debug=False)


if __name__ == "__main__":
    main()
