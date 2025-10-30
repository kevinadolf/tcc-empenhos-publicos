"""Domain service orchestrating graph loading and analysis."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Optional, Tuple, cast

from src.analysis.pipeline import analyze_graph
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.graph_serialization import iter_node_summaries, to_node_link_data
from src.common.spark_graph import SparkGraph
from src.common.settings import get_settings
from src.db.repository import GraphData, GraphRepository

logger = logging.getLogger(__name__)


class GraphService:
    def __init__(self, repository: Optional[GraphRepository] = None) -> None:
        self.settings = get_settings()
        self.repository = repository or GraphRepository(self.settings)
        self._cache_lock = Lock()
        self._graph_cache: Dict[str, Any] = {
            "graph": None,
            "graph_data": None,
            "payloads": None,
            "timestamp": None,
        }
        ttl_seconds = self.settings.graph_cache_ttl_seconds
        self._cache_ttl = (
            timedelta(seconds=ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None
        )

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._graph_cache = {
                "graph": None,
                "graph_data": None,
                "payloads": None,
                "timestamp": None,
            }

    def refresh_cache(self):
        self.clear_cache()
        if not self.settings.enable_live_fetch:
            return self.repository.load_graph(payloads=SAMPLE_PAYLOAD)
        graph, graph_data, _ = self._load_live_graph_from_cache()
        return graph, graph_data

    def load_payloads(self) -> Dict:
        if not self.settings.enable_live_fetch:
            return SAMPLE_PAYLOAD
        _, _, payloads = self._load_live_graph_from_cache()
        return payloads

    def load_graph(self, payloads: Dict | None = None):
        if payloads is not None:
            graph, graph_data = self.repository.load_graph(payloads=payloads)
            return graph, graph_data

        if not self.settings.enable_live_fetch:
            graph, graph_data = self.repository.load_graph(payloads=SAMPLE_PAYLOAD)
            return graph, graph_data

        graph, graph_data, _ = self._load_live_graph_from_cache()
        return graph, graph_data

    def get_graph_summary(self, payloads: Dict | None = None) -> Dict:
        graph, graph_data = self.load_graph(payloads=payloads)
        return {
            "nodes": graph.num_vertices(),
            "edges": graph.num_edges(),
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

    def _cache_expired(self) -> bool:
        if self._graph_cache["graph"] is None:
            return True
        if self._cache_ttl is None:
            return False
        timestamp = self._graph_cache.get("timestamp")
        if not isinstance(timestamp, datetime):
            return True
        return datetime.utcnow() - timestamp > self._cache_ttl

    def _load_live_graph_from_cache(self) -> Tuple[SparkGraph, GraphData, Dict]:
        with self._cache_lock:
            if not self._cache_expired():
                graph = cast(SparkGraph, self._graph_cache["graph"])
                graph_data = cast(GraphData, self._graph_cache["graph_data"])
                payloads = cast(Dict, self._graph_cache["payloads"])
                return graph, graph_data, payloads

        try:
            payloads = self.repository.fetch_payloads()
        except Exception as exc:  # pragma: no cover - exercised in integration
            logger.warning(
                "Falha ao coletar dados em tempo real; usando payload de exemplo. Erro: %s",
                exc,
            )
            payloads = SAMPLE_PAYLOAD
        graph, graph_data = self.repository.load_graph(payloads=payloads)

        with self._cache_lock:
            self._graph_cache = {
                "graph": graph,
                "graph_data": graph_data,
                "payloads": payloads,
                "timestamp": datetime.utcnow(),
            }

        return graph, graph_data, payloads
