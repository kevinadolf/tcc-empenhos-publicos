"""Anomaly detection helpers built on top of Spark-based graph metrics."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, functions as F

from src.analysis.metrics import (
    community_detection_louvain,
    shannon_entropy_by_neighbor,
    weighted_degree_centrality,
)
from src.common.graph_serialization import normalize_node_id
from src.common.spark_graph import SparkGraph
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO

MAX_SUBGRAPH_NODES = 30


@dataclass
class AnomalyResult:
    score: float
    reason: str
    context: Dict[str, Any]
    severity: str = "media"


def _ensure_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


class RobustStats:
    def __init__(self, values: Iterable[float]) -> None:
        cleaned = [float(value) for value in values if math.isfinite(value)]
        if not cleaned:
            cleaned = [0.0]
        self._values = cleaned
        self.median = statistics.median(cleaned)
        self.mad = statistics.median(abs(value - self.median) for value in cleaned) or 1e-9
        self.mean = statistics.mean(cleaned)
        self.stdev = statistics.pstdev(cleaned) or 1e-9
        self.p80 = statistics.quantiles(cleaned, n=5)[3] if len(cleaned) >= 5 else max(cleaned)

    def zscore(self, value: float) -> float:
        return 0.6745 * (value - self.median) / self.mad if self.mad else 0.0


def _severity_from_zscore(zscore: float) -> str:
    if zscore >= 3.5:
        return "alta"
    if zscore >= 2.5:
        return "media"
    return "baixa"


def _collect_vertex_metadata(graph: SparkGraph, node_ids: Sequence[Any]) -> Dict[str, Dict[str, Any]]:
    unique_ids: List[Any] = [node_id for node_id in dict.fromkeys(node_ids) if node_id is not None]
    if not unique_ids:
        return {}

    selection = graph.vertices.filter(F.col("id").isin(*unique_ids)).select(
        "id",
        "node_type",
        "original_id",
        "nome",
        "descricao",
        "numero",
        "valor",
    )
    rows = selection.collect()

    metadata: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        data = row.asDict()
        raw_id = data.get("id")
        normalized = normalize_node_id(raw_id)
        label = (
            data.get("nome")
            or data.get("descricao")
            or data.get("numero")
            or data.get("original_id")
            or normalized
        )
        valor = _ensure_float(data.get("valor"))
        metadata[normalized] = {
            "id": normalized,
            "type": data.get("node_type"),
            "label": label,
            **({"valor": valor} if valor is not None else {}),
        }
    return metadata


def _build_subgraph(
    graph: SparkGraph,
    node_ids: Sequence[Any],
    focus_id: Any,
    *,
    metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    max_nodes: int = MAX_SUBGRAPH_NODES,
) -> Dict[str, Any]:
    if not node_ids:
        focus = normalize_node_id(focus_id) if focus_id is not None else None
        return {"nodes": [], "links": [], "focus": focus}

    trimmed_ids = list(dict.fromkeys(node_ids))[:max_nodes]
    if metadata is None:
        metadata = _collect_vertex_metadata(graph, trimmed_ids)

    nodes_payload: List[Dict[str, Any]] = []
    for raw_id in trimmed_ids:
        normalized = normalize_node_id(raw_id)
        node_payload = metadata.get(normalized, {"id": normalized, "label": normalized, "type": None})
        nodes_payload.append(node_payload)

    edges_df = (
        graph.edges.filter(F.col("src").isin(*trimmed_ids) & F.col("dst").isin(*trimmed_ids)).select(
            "src",
            "dst",
            "edge_type",
            "valor",
        )
    )
    links: List[Dict[str, Any]] = []
    for row in edges_df.collect():
        weight = _ensure_float(row["valor"])
        links.append(
            {
                "source": normalize_node_id(row["src"]),
                "target": normalize_node_id(row["dst"]),
                "type": row["edge_type"],
                **({"valor": weight} if weight is not None else {}),
            },
        )

    return {
        "nodes": nodes_payload,
        "links": links,
        "focus": normalize_node_id(focus_id) if focus_id is not None else None,
    }


def _extract_local_view(
    graph: SparkGraph,
    node_id: Any,
    *,
    neighbor_limit: int = 6,
    extra_nodes: Optional[Sequence[Any]] = None,
) -> Dict[str, Any]:
    edges = (
        graph.edges.alias("e")
        .filter((F.col("e.src") == node_id) | (F.col("e.dst") == node_id))
        .select(
            F.when(F.col("e.src") == node_id, F.col("e.dst")).otherwise(F.col("e.src")).alias("neighbor_id"),
            "edge_type",
            "valor",
        )
    )

    rows = edges.collect()
    neighbor_entries: List[Dict[str, Any]] = []
    for row in rows:
        weight = _ensure_float(row["valor"])
        neighbor_entries.append(
            {
                "raw_id": row["neighbor_id"],
                "id": normalize_node_id(row["neighbor_id"]),
                "edge_type": row["edge_type"],
                **({"valor": weight} if weight is not None else {}),
            },
        )

    neighbor_entries.sort(
        key=lambda entry: entry["valor"] if entry.get("valor") is not None else float("-inf"),
        reverse=True,
    )
    trimmed_neighbors = neighbor_entries[:neighbor_limit]

    raw_ids = [node_id]
    raw_ids.extend(entry["raw_id"] for entry in trimmed_neighbors)
    if extra_nodes:
        raw_ids.extend(extra_nodes)

    metadata = _collect_vertex_metadata(graph, raw_ids)
    center_id = normalize_node_id(node_id)
    center_metadata = metadata.get(center_id, {"id": center_id, "label": center_id, "type": None})

    enriched_neighbors: List[Dict[str, Any]] = []
    for entry in trimmed_neighbors:
        meta = metadata.get(entry["id"], {"id": entry["id"], "label": entry["id"], "type": None})
        enriched_neighbors.append(
            {
                "id": meta["id"],
                "label": meta.get("label"),
                "type": meta.get("type"),
                "edge_type": entry.get("edge_type"),
                **({"valor": entry["valor"]} if "valor" in entry else {}),
                **({"node_valor": meta.get("valor")} if meta.get("valor") is not None else {}),
            },
        )

    subgraph = _build_subgraph(graph, raw_ids, focus_id=node_id, metadata=metadata)

    return {
        "center": center_metadata,
        "neighbors": enriched_neighbors,
        "neighbor_count": len(rows),
        "subgraph": subgraph,
    }


def detect_centrality_outliers(
    graph: SparkGraph,
    node_type: str,
    *,
    top_k: int = 8,
    zscore_threshold: float = 2.0,
) -> List[AnomalyResult]:
    centrality = weighted_degree_centrality(graph, node_type)
    if not centrality:
        return []

    stats = RobustStats(centrality.values())
    ranked = sorted(centrality.items(), key=lambda item: item[1], reverse=True)

    candidates: List[Tuple[int, Any, float, float]] = []
    for rank, (node_id, value) in enumerate(ranked, start=1):
        zscore = stats.zscore(value)
        if zscore >= zscore_threshold or rank <= top_k:
            candidates.append((rank, node_id, value, zscore))

    if not candidates:
        return []

    candidates.sort(key=lambda entry: entry[3], reverse=True)
    selected = candidates[:top_k]

    anomalies: List[AnomalyResult] = []
    for rank, node_id, value, zscore in selected:
        local_view = _extract_local_view(graph, node_id)
        severity = _severity_from_zscore(zscore)
        anomalies.append(
            AnomalyResult(
                score=float(zscore),
                severity=severity,
                reason="Centralidade ponderada acima do esperado",
                context={
                    "node_id": local_view["center"]["id"],
                    "node_label": local_view["center"]["label"],
                    "node_type": node_type,
                    "centrality": float(value),
                    "rank": rank,
                    "zscore": float(zscore),
                    "neighbor_count": local_view["neighbor_count"],
                    "top_neighbors": local_view["neighbors"],
                    "subgraph": local_view["subgraph"],
                },
            ),
        )
    return anomalies


def detect_entropy_outliers(
    graph: SparkGraph,
    focus_type: str,
    neighbor_type: str,
    threshold: Optional[float] = None,
    *,
    top_k: int = 6,
    zscore_threshold: float = 1.5,
) -> List[AnomalyResult]:
    entropy_scores = shannon_entropy_by_neighbor(graph, focus_type, neighbor_type)
    if not entropy_scores:
        return []

    min_entropy = threshold if threshold is not None else 0.0
    filtered_values = [value for value in entropy_scores.values() if value >= min_entropy]
    stats = RobustStats(filtered_values or entropy_scores.values())

    candidates: List[Tuple[Any, float, float]] = []
    for node_id, entropy_value in entropy_scores.items():
        if entropy_value < min_entropy:
            continue
        zscore = stats.zscore(entropy_value)
        if zscore >= zscore_threshold:
            candidates.append((node_id, entropy_value, zscore))

    if not candidates:
        fallback = sorted(entropy_scores.items(), key=lambda item: item[1], reverse=True)[:top_k]
        candidates = [(node_id, value, stats.zscore(value)) for node_id, value in fallback]

    candidates.sort(key=lambda entry: entry[2], reverse=True)
    selected = candidates[:top_k]

    anomalies: List[AnomalyResult] = []
    for node_id, entropy_value, zscore in selected:
        local_view = _extract_local_view(graph, node_id)
        severity = _severity_from_zscore(zscore + 0.5)
        anomalies.append(
            AnomalyResult(
                score=float(zscore),
                severity=severity,
                reason=f"Alta entropia de relação com {neighbor_type}",
                context={
                    "node_id": local_view["center"]["id"],
                    "node_label": local_view["center"]["label"],
                    "focus_type": focus_type,
                    "entropy": float(entropy_value),
                    "zscore": float(zscore),
                    "neighbor_count": local_view["neighbor_count"],
                    "top_neighbors": local_view["neighbors"],
                    "subgraph": local_view["subgraph"],
                },
            ),
        )
    return anomalies


def detect_isolated_communities(
    graph: SparkGraph,
    source_type: str,
    target_type: str,
    min_size: int = 2,
) -> List[AnomalyResult]:
    relevant_types = {source_type, target_type}
    vertices = graph.vertices.filter(F.col("node_type").isin(*relevant_types)).cache()

    if vertices.rdd.isEmpty():
        vertices.unpersist(False)
        return []

    fallback_rows = vertices.select("id", "node_type").limit(6).collect()

    edges = (
        graph.edges.alias("e")
        .join(vertices.select(F.col("id").alias("src_id")), F.col("e.src") == F.col("src_id"))
        .join(vertices.select(F.col("id").alias("dst_id")), F.col("e.dst") == F.col("dst_id"))
        .select("e.src", "e.dst", "e.edge_type", "e.valor")
    )

    if edges.rdd.isEmpty():
        vertices.unpersist(False)
        return []

    subgraph = SparkGraph(graph.spark, vertices, edges)
    components = community_detection_louvain(subgraph)
    if not components:
        vertices.unpersist(False)
        return []

    component_df = graph.spark.createDataFrame(
        [{"id": node_id, "component": component_id} for node_id, component_id in components.items()],
    )

    enriched = component_df.join(vertices, on="id", how="inner")
    grouped = (
        enriched.groupBy("component")
        .agg(
            F.count("*").alias("size"),
            F.collect_list("id").alias("nodes"),
            F.collect_set("node_type").alias("node_types"),
        )
        .collect()
    )
    vertices.unpersist(False)

    if not grouped:
        if not fallback_rows:
            return []

        fallback_ids = [normalize_node_id(row["id"]) for row in fallback_rows]
        focus_row = next((row for row in fallback_rows if row["node_type"] == source_type), fallback_rows[0])
        subgraph_payload = _build_subgraph(graph, fallback_ids, focus_id=focus_row["id"])
        anomalies = [
            AnomalyResult(
                score=0.0,
                severity="baixa",
                reason="Comunidade aproximada gerada por fallback de centralidade",
                context={
                    "node_count": len(subgraph_payload["nodes"]),
                    "fallback_selected": True,
                    "node_types": list({node.get("type") for node in subgraph_payload["nodes"] if node.get("type")}),
                    "subgraph": subgraph_payload,
                },
            ),
        ]
        return anomalies

    def _build_anomaly(row, *, note: Optional[str] = None) -> Tuple[int, float, AnomalyResult]:
        size = int(row["size"])
        component_nodes: Sequence[Any] = row["nodes"]
        node_types = sorted(row["node_types"])
        focus_id = component_nodes[0]

        local_view = _extract_local_view(graph, focus_id, neighbor_limit=4, extra_nodes=component_nodes)
        subgraph_payload = local_view["subgraph"]
        internal_edges = len(subgraph_payload["links"])

        neighbor_edges_df = (
            graph.edges.filter(
                F.col("src").isin(*component_nodes) | F.col("dst").isin(*component_nodes),
            ).select("src", "dst")
        )
        edge_rows = neighbor_edges_df.collect()
        normalized_component = {normalize_node_id(node_id) for node_id in component_nodes}

        total_touches = len(edge_rows)
        external_edges = sum(
            1
            for edge in edge_rows
            if not (
                normalize_node_id(edge["src"]) in normalized_component
                and normalize_node_id(edge["dst"]) in normalized_component
            )
        )

        isolation_score = 1.0
        if total_touches:
            isolation_score = 1.0 - (external_edges / total_touches)

        severity = "alta" if isolation_score >= 0.85 else "media" if isolation_score >= 0.6 else "baixa"
        possible_edges = size * (size - 1) / 2
        density = (internal_edges / possible_edges) if possible_edges else 0.0

        context = {
            "community_id": int(row["component"]),
            "node_count": size,
            "node_types": list(node_types),
            "isolation_score": float(isolation_score),
            "internal_edges": internal_edges,
            "external_edges": external_edges,
            "edge_density": float(density),
            "sample_nodes": [normalize_node_id(node_id) for node_id in component_nodes[:MAX_SUBGRAPH_NODES]],
            "subgraph": subgraph_payload,
        }
        if note:
            context["fallback_reason"] = note

        reason = "Comunidade com poucas ligações externas"
        if note:
            reason = f"{reason} ({note})"

        anomaly = AnomalyResult(
            score=float(isolation_score),
            severity=severity,
            reason=reason,
            context=context,
        )
        return size, isolation_score, anomaly

    anomalies: List[AnomalyResult] = []
    fallback_candidates: List[Tuple[int, float, AnomalyResult]] = []
    for row in grouped:
        size, isolation_score, anomaly = _build_anomaly(row)
        if size < min_size:
            fallback_candidates.append((size, isolation_score, anomaly))
            continue
        anomalies.append(anomaly)

    if not anomalies and fallback_candidates:
        fallback_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        _, _, best_anomaly = fallback_candidates[0]
        best_anomaly.severity = best_anomaly.severity or "baixa"
        best_anomaly.context["fallback_selected"] = True
        best_anomaly.context["min_size_threshold"] = min_size
        best_anomaly.reason = (
            f"{best_anomaly.reason} (componente abaixo do limiar mínimo considerado: {min_size})"
        )
        anomalies.append(best_anomaly)

    if not anomalies:
        ordered = sorted(
            fallback_rows,
            key=lambda row: (row["node_type"] != source_type, str(row["id"])),
        )
        if ordered:
            fallback_ids = [normalize_node_id(row["id"]) for row in ordered]
            focus_id = next((row["id"] for row in ordered if row["node_type"] == source_type), ordered[0]["id"])
            subgraph_payload = _build_subgraph(graph, fallback_ids, focus_id=focus_id)
            anomalies.append(
                AnomalyResult(
                    score=0.0,
                    severity="baixa",
                    reason="Subgrafo reduzido gerado via fallback (dados insuficientes para componentes reais)",
                    context={
                        "fallback_selected": True,
                        "node_count": len(subgraph_payload["nodes"]),
                        "node_types": list(
                            {node.get("type") for node in subgraph_payload["nodes"] if node.get("type")}
                        ),
                        "subgraph": subgraph_payload,
                    },
                ),
            )

    return sorted(anomalies, key=lambda anomaly: anomaly.score, reverse=True)


def run_default_anomaly_suite(graph: SparkGraph) -> Dict[str, List[AnomalyResult]]:
    return {
        "centralidade_fornecedores": detect_centrality_outliers(graph, NODE_FORNECEDOR),
        "centralidade_orgaos": detect_centrality_outliers(graph, NODE_ORGAO),
        "alta_entropia_fornecedores": detect_entropy_outliers(
            graph,
            focus_type=NODE_FORNECEDOR,
            neighbor_type=NODE_ORGAO,
        ),
        "comunidades_isoladas": detect_isolated_communities(
            graph,
            source_type=NODE_ORGAO,
            target_type=NODE_FORNECEDOR,
        ),
    }
