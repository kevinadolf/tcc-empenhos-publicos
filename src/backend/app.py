"""Flask application factory for the anomaly detection backend."""

from __future__ import annotations

from flask import Flask

from src.backend.routes.api import api_bp
from src.frontend.views import frontend_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(frontend_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    return app


if __name__ == "__main__":  # pragma: no cover
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=True)
