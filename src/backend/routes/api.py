from __future__ import annotations

import os

from flask import Blueprint, abort, jsonify, request

from src.backend.services import ai_service
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


@api_bp.post("/ai/summary")
def ai_summary():
    payload = request.get_json(silent=True) or {}
    try:
        text = ai_service.summarize_anomalies(payload)
    except RuntimeError as exc:
        abort(503, description=f"IA indisponível: {exc}")
    return jsonify({"summary": text})


@api_bp.post("/ai/prioritize")
def ai_prioritize():
    payload = request.get_json(silent=True) or {}
    cases = payload.get("cases") or []
    try:
        text = ai_service.prioritize_cases(cases)
    except RuntimeError as exc:
        abort(503, description=f"IA indisponível: {exc}")
    return jsonify({"prioritized": text})


@api_bp.post("/ai/insights")
def ai_insights():
    context = request.get_json(silent=True) or {}
    try:
        text = ai_service.suggest_insights(context)
    except RuntimeError as exc:
        abort(503, description=f"IA indisponível: {exc}")
    return jsonify({"insights": text})


@api_bp.post("/ai/assistant")
def ai_assistant():
    payload = request.get_json(silent=True) or {}
    question = payload.get("question")
    context = payload.get("context")
    if not question:
        abort(400, description="Campo 'question' é obrigatório.")
    try:
        text = ai_service.assistant_answer(question, context=context)
    except RuntimeError as exc:
        abort(503, description=f"IA indisponível: {exc}")
    return jsonify({"answer": text})
