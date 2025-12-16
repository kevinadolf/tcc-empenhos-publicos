"""Risk scoring utilities to combine anomaly signals."""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

SeverityWeight = {"baixa": 0.3, "media": 0.7, "alta": 1.2}

DETECTOR_WEIGHT = {
    "centralidade_fornecedores": 1.0,
    "centralidade_orgaos": 1.0,
    "alta_entropia_fornecedores": 0.9,
    "comunidades_isoladas": 0.8,
}


def _severity_weight(severity: str) -> float:
    return SeverityWeight.get(severity.lower(), 0.4)


def compute_node_risk(anomalies: Dict[str, List[Dict]]) -> Dict[str, Dict[str, float]]:
    """Aggregate anomaly signals into a per-node risk score.

    Returns a dict keyed by node_id with fields:
    - score: aggregated risk score
    - signals: count of contributing anomalies
    """
    node_scores: Dict[str, List[float]] = {}

    for detector, items in anomalies.items():
        for item in items:
            severity = item.get("severity", "media")
            weight = _severity_weight(severity)
            detector_weight = DETECTOR_WEIGHT.get(detector, 1.0)
            raw_score = item.get("score")
            try:
                numeric_score = float(raw_score) if raw_score is not None else 1.0
            except (TypeError, ValueError):
                numeric_score = 1.0
            numeric_score = max(0.0, numeric_score)
            score = numeric_score * weight * detector_weight
            context = item.get("context") or {}
            node_ids: List[str] = []
            if context.get("node_id"):
                node_ids.append(str(context["node_id"]))
            if isinstance(context.get("sample_nodes"), Iterable):
                node_ids.extend(str(n) for n in context["sample_nodes"])
            if isinstance(context.get("nodes"), Iterable):
                node_ids.extend(str(n) for n in context["nodes"])
            if context.get("subgraph") and isinstance(context["subgraph"].get("nodes"), Iterable):
                node_ids.extend(str(node.get("id")) for node in context["subgraph"]["nodes"] if node)

            for node_id in set(node_ids):
                node_scores.setdefault(node_id, []).append(score)

    aggregated: Dict[str, Dict[str, float]] = {}
    for node_id, scores in node_scores.items():
        if not scores:
            continue
        total = sum(scores)
        signals = max(len(scores), 1)
        aggregated[node_id] = {
            "score": float(total / signals),
            "signals": signals,
        }

    return aggregated
