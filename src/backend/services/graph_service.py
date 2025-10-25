"""Domain service orchestrating graph loading and analysis."""

from __future__ import annotations

from typing import Dict, Optional

from src.analysis.pipeline import analyze_graph
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.settings import get_settings
from src.db.repository import GraphRepository
from src.common.graph_serialization import to_node_link_data, iter_node_summaries


class GraphService:
    def __init__(self, repository: Optional[GraphRepository] = None) -> None:
        self.settings = get_settings()
        self.repository = repository or GraphRepository(self.settings)

    def load_payloads(self) -> Dict:
        if self.settings.enable_live_fetch:
            return self.repository.fetch_payloads()
        return SAMPLE_PAYLOAD

    def load_graph(self, payloads: Dict | None = None):
        if payloads is None:
            payloads = self.load_payloads()
        graph, graph_data = self.repository.load_graph(payloads=payloads)
        return graph, graph_data

    def get_graph_summary(self, payloads: Dict | None = None) -> Dict:
        graph, graph_data = self.load_graph(payloads=payloads)
        return {
            "nodes": graph.number_of_nodes(),
            "edges": graph.number_of_edges(),
            "empenhos": len(graph_data.empenhos),
            "fornecedores": len(graph_data.fornecedores),
            "orgaos": len(graph_data.orgaos),
        }

    def get_anomalies(self, payloads: Dict | None = None) -> Dict:
        graph, _ = self.load_graph(payloads=payloads)
        report = analyze_graph(graph)
        return report.as_dict()

    def get_graph_snapshot(self, payloads: Dict | None = None) -> Dict:
        graph, _ = self.load_graph(payloads=payloads)
        return to_node_link_data(graph)

    def list_nodes(self, payloads: Dict | None = None):
        graph, _ = self.load_graph(payloads=payloads)
        return list(iter_node_summaries(graph))
