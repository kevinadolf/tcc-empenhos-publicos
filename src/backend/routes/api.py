from __future__ import annotations

import os

from flask import Blueprint, abort, jsonify, request

from src.backend.services.graph_service import GraphService

api_bp = Blueprint("api", __name__)
service = GraphService(auto_prefetch=True)


def _require_admin() -> None:
    expected = os.getenv("ADMIN_API_KEY")
    if not expected:
        return
    provided = request.headers.get("X-API-KEY")
    if not provided or provided != expected:
        abort(401, description="API key inválida para operações administrativas.")


@api_bp.get("/health")
def health_check():
    return jsonify({"status": "ok"})


@api_bp.get("/graph/summary")
def graph_summary():
    summary = service.get_graph_summary(source=request.args.get("source"))
    return jsonify(summary)


@api_bp.get("/graph/snapshot")
def graph_snapshot():
    data = service.get_graph_snapshot(source=request.args.get("source"))
    return jsonify(data)


@api_bp.get("/graph/nodes")
def graph_nodes():
    nodes = service.list_nodes(source=request.args.get("source"))
    return jsonify(nodes)


@api_bp.get("/graph/fetch/status")
def graph_fetch_status():
    status = service.get_fetch_status()
    return jsonify(status)


@api_bp.get("/anomalies")
def anomalies():
    data = service.get_anomalies(source=request.args.get("source"))
    return jsonify(data)


@api_bp.get("/risk")
def risk_scores():
    data = service.get_risk_scores(source=request.args.get("source"))
    return jsonify(data)


@api_bp.post("/admin/refresh")
def admin_refresh():
    _require_admin()
    source = request.json.get("source") if request.is_json else None
    graph, graph_data = service.refresh_cache(source=source)
    return jsonify(
        {
            "status": "ok",
            "nodes": graph.num_vertices(),
            "edges": graph.num_edges(),
            "empenhos": len(graph_data.empenhos),
            "fornecedores": len(graph_data.fornecedores),
            "orgaos": len(graph_data.orgaos),
        },
    )


@api_bp.get("/admin/health")
def admin_health():
    _require_admin()
    status = service.get_fetch_status()
    summary = service.get_graph_summary()
    return jsonify(
        {
            "fetch_status": status,
            "default_source": service._default_source,
            "summary": summary,
        },
    )
