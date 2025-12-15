"""Flask application factory for the anomaly detection backend."""

from __future__ import annotations

import json
import logging
import os

from flask import Flask, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.backend.routes.api import api_bp
from src.frontend.views import frontend_bp


def create_app() -> Flask:
    if os.getenv("LOG_JSON", "true").lower() == "true":
        logging.basicConfig(
            level=logging.INFO,
            format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
        )
    else:
        logging.basicConfig(level=logging.INFO)

    app = Flask(__name__)
    app.register_blueprint(frontend_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.get("/metrics")
    def metrics():  # pragma: no cover - integration
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    return app


if __name__ == "__main__":  # pragma: no cover
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=True)
