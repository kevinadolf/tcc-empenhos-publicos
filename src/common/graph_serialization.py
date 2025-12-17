"""Utilities to serialize Spark-based graphs for persistence and frontend consumption."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from src.common.spark_graph import SparkGraph


def normalize_node_id(node: Any) -> str:
    """Return a stable string representation for heterogeneous node keys."""
    if isinstance(node, (list, tuple)):
        return "::".join(str(part) for part in node)
    return str(node)


def _collect_nodes(graph: SparkGraph) -> List[Dict[str, Any]]:
    available_cols = set(graph.vertices.columns)
    select_cols = [
        "id",
        "node_type",
        "original_id",
        "nome",
        "sigla",
        "municipio",
        "uf",
        "documento",
        "tipo_documento",
        "numero",
        "descricao",
        "valor",
        "data",
    ]
    optional_cols = ["fonte_origem", "data_ingestao", "payload_hash"]
    select_cols.extend([col for col in optional_cols if col in available_cols])

    rows = graph.vertices.select(*select_cols).toLocalIterator()

    nodes: List[Dict[str, Any]] = []
    for row in rows:
        data = row.asDict()
        node_id = normalize_node_id(data.pop("id"))
        node_type = data.pop("node_type", None)
        valor = data.get("valor")
        if valor is not None:
            try:
                numeric = float(valor)
            except (TypeError, ValueError):
                numeric = None
            if numeric is None or not math.isfinite(numeric):
                data.pop("valor", None)
            else:
                data["valor"] = numeric
        if data.get("data") is not None:
            data["data"] = str(data["data"])
        nodes.append(
            {
                "id": node_id,
                "type": node_type,
                **{key: value for key, value in data.items() if value is not None},
            },
        )
    return nodes


def _collect_edges(graph: SparkGraph) -> List[Dict[str, Any]]:
    rows = graph.edges.select("src", "dst", "edge_type", "valor").toLocalIterator()
    edges: List[Dict[str, Any]] = []
    for row in rows:
        data = row.asDict()
        valor = data.get("valor")
        if valor is not None:
            try:
                numeric = float(valor)
            except (TypeError, ValueError):
                numeric = None
            if numeric is None or not math.isfinite(numeric):
                valor = None
            else:
                valor = numeric
        edges.append(
            {
                "source": normalize_node_id(data["src"]),
                "target": normalize_node_id(data["dst"]),
                "type": data.get("edge_type"),
                **({"valor": valor} if valor is not None else {}),
            },
        )
    return edges


def _compute_layout(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Dict[str, Tuple[float, float]]:
    if len(nodes) == 0 or len(nodes) > 1000:
        return {}
    try:
        import networkx as nx  # type: ignore
    except ImportError:  # pragma: no cover - layout is optional
        return {}

    graph = nx.Graph()
    for node in nodes:
        graph.add_node(node["id"])
    for edge in edges:
        graph.add_edge(edge["source"], edge["target"])

    positions = nx.spring_layout(graph, seed=42, dim=2, k=None)
    return {node_id: (float(x), float(y)) for node_id, (x, y) in positions.items()}


def to_node_link_data(graph: SparkGraph) -> Dict[str, Any]:
    """Serialize the graph to a JSON-compatible node-link structure."""
    nodes = _collect_nodes(graph)
    edges = _collect_edges(graph)

    layout = _compute_layout(nodes, edges)
    for node in nodes:
        coords = layout.get(node["id"])
        if coords:
            node["x"], node["y"] = coords

    return {
        "nodes": nodes,
        "links": edges,
    }


def export_graph_snapshot(
    graph: SparkGraph,
    graphml_path: Path,
    json_path: Path,
) -> None:
    """Persist the graph to GraphML and node-link JSON files."""
    nodes = _collect_nodes(graph)
    edges = _collect_edges(graph)

    try:
        import networkx as nx  # type: ignore
    except ImportError:  # pragma: no cover - optional dependency
        nx = None

    if nx is not None:
        serializable = nx.MultiDiGraph()
        for node in nodes:
            serializable.add_node(node["id"], **{k: v for k, v in node.items() if k != "id"})
        for edge in edges:
            attrs = {k: v for k, v in edge.items() if k not in {"source", "target"}}
            serializable.add_edge(edge["source"], edge["target"], **attrs)
        nx.write_graphml(serializable, graphml_path)

    json_path.write_text(
        json.dumps({"nodes": nodes, "links": edges}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def iter_node_summaries(graph: SparkGraph) -> Iterable[Dict[str, Any]]:
    """Yield lightweight node summaries suitable for list views."""
    rows = graph.vertices.select(
        "id",
        "node_type",
        "nome",
        "descricao",
        "numero",
        "original_id",
        "valor",
    ).toLocalIterator()

    for row in rows:
        data = row.asDict()
        node_id = normalize_node_id(data["id"])
        label = (
            data.get("nome")
            or data.get("descricao")
            or data.get("numero")
            or data.get("original_id")
            or node_id
        )
        summary = {
            "id": node_id,
            "label": label,
            "type": data.get("node_type"),
            "original_id": data.get("original_id") or node_id,
        }
        valor = data.get("valor")
        if valor is not None:
            try:
                numeric = float(valor)
            except (TypeError, ValueError):
                numeric = None
            if numeric is not None and math.isfinite(numeric):
                summary["valor"] = numeric
        yield summary
