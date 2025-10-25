"""Utilities to serialize NetworkX graphs for persistence and frontend consumption."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable

import networkx as nx
from networkx.readwrite import json_graph


def normalize_node_id(node: Any) -> str:
    """Return a stable string representation for heterogeneous node keys."""
    if isinstance(node, (list, tuple)):
        return "::".join(str(part) for part in node)
    return str(node)


def prepare_serializable_graph(graph: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Copy the graph with flattened node identifiers and original metadata."""
    serializable = nx.MultiDiGraph()
    for node, data in graph.nodes(data=True):
        node_id = normalize_node_id(node)
        serializable.add_node(node_id, original_id=node_id, **data)

    for source, target, key, data in graph.edges(keys=True, data=True):
        serializable.add_edge(
            normalize_node_id(source),
            normalize_node_id(target),
            key=key,
            **data,
        )

    return serializable


def to_node_link_data(graph: nx.MultiDiGraph) -> Dict[str, Any]:
    """Serialize the graph to a JSON-compatible node-link structure."""
    serializable = prepare_serializable_graph(graph)
    # Pre-compute positions to support frontend rendering.
    if serializable.number_of_nodes() > 0:
        positions = nx.spring_layout(serializable, seed=42, dim=2, k=None)
        for node_id, (x, y) in positions.items():
            serializable.nodes[node_id]["x"] = float(x)
            serializable.nodes[node_id]["y"] = float(y)
    return json_graph.node_link_data(serializable)


def export_graph_snapshot(
    graph: nx.MultiDiGraph,
    graphml_path: Path,
    json_path: Path,
) -> None:
    """Persist the graph to GraphML and node-link JSON files."""
    serializable = prepare_serializable_graph(graph)
    nx.write_graphml(serializable, graphml_path)
    json_path.write_text(
        json.dumps(json_graph.node_link_data(serializable), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def iter_node_summaries(graph: nx.MultiDiGraph) -> Iterable[Dict[str, Any]]:
    """Yield lightweight node summaries suitable for list views."""
    for node_id, attrs in graph.nodes(data=True):
        summary = {
            "id": normalize_node_id(node_id),
            "label": attrs.get("nome")
            or attrs.get("descricao")
            or attrs.get("numero")
            or attrs.get("original_id")
            or normalize_node_id(node_id),
            "type": attrs.get("type"),
            "original_id": attrs.get("original_id") or normalize_node_id(node_id),
        }
        if "valor" in attrs:
            try:
                summary["valor"] = float(attrs["valor"])
            except (TypeError, ValueError):
                summary["valor"] = attrs["valor"]
        yield summary
