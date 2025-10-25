from __future__ import annotations

from flask import Blueprint, jsonify

from src.backend.services.graph_service import GraphService

api_bp = Blueprint("api", __name__)
service = GraphService()


@api_bp.get("/health")
def health_check():
    return jsonify({"status": "ok"})


@api_bp.get("/graph/summary")
def graph_summary():
    summary = service.get_graph_summary()
    return jsonify(summary)


@api_bp.get("/graph/snapshot")
def graph_snapshot():
    data = service.get_graph_snapshot()
    return jsonify(data)


@api_bp.get("/graph/nodes")
def graph_nodes():
    nodes = service.list_nodes()
    return jsonify(nodes)


@api_bp.get("/anomalies")
def anomalies():
    data = service.get_anomalies()
    return jsonify(data)
