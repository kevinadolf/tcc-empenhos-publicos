"""Flask blueprint exposing simple frontend pages."""

from __future__ import annotations

from flask import Blueprint, render_template

frontend_bp = Blueprint(
    "frontend",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/assets",
)


@frontend_bp.get("/")
def graph_explorer():
    return render_template("graph.html")


@frontend_bp.get("/anomalies")
def anomalies_view():
    return render_template("anomalies.html")


@frontend_bp.get("/nodes")
def nodes_view():
    return render_template("nodes.html")
