"""Domain service orchestrating graph loading and analysis."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Callable, Dict, Optional, Tuple, cast

from src.analysis.pipeline import analyze_graph
from src.backend.services.random_payloads import generate_random_payloads
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.graph_serialization import iter_node_summaries, to_node_link_data
from src.common.spark_graph import SparkGraph
from src.common.settings import get_settings
from src.db.repository import GraphData, GraphRepository

logger = logging.getLogger(__name__)

DATA_SOURCES = {"sample", "api", "random"}


class GraphService:
    def __init__(self, repository: Optional[GraphRepository] = None) -> None:
        self.settings = get_settings()
        self.repository = repository or GraphRepository(self.settings)
        self._cache_lock = Lock()
        self._graph_cache: Dict[str, Dict[str, Any]] = {}
        ttl_seconds = self.settings.graph_cache_ttl_seconds
        self._cache_ttl = (
            timedelta(seconds=ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None
        )
        self._default_source = self._normalize_default_source(self.settings.graph_data_source)

    @staticmethod
    def _normalize_source(source: Optional[str]) -> str:
        if not source:
            return "sample"
        normalized = source.strip().lower()
        if normalized not in DATA_SOURCES:
            return "sample"
        return normalized

    def _normalize_default_source(self, source: Optional[str]) -> str:
        normalized = self._normalize_source(source)
        if normalized == "api" and not self.settings.enable_live_fetch:
            return "sample"
        return normalized

    def _resolve_source(self, source: Optional[str]) -> str:
        if source is None:
            return self._default_source
        return self._normalize_source(source)

    def _cache_expired(self, mode: str) -> bool:
        entry = self._graph_cache.get(mode)
        if not entry or entry.get("graph") is None:
            return True
        if self._cache_ttl is None:
            return False
        timestamp = entry.get("timestamp")
        if not isinstance(timestamp, datetime):
            return True
        return datetime.utcnow() - timestamp > self._cache_ttl

    def _with_cache(
        self,
        mode: str,
        loader: Callable[[], Tuple[SparkGraph, GraphData, Dict]],
    ) -> Tuple[SparkGraph, GraphData, Dict]:
        with self._cache_lock:
            if not self._cache_expired(mode):
                cached = self._graph_cache[mode]
                return (
                    cast(SparkGraph, cached["graph"]),
                    cast(GraphData, cached["graph_data"]),
                    cast(Dict, cached["payloads"]),
                )

        graph, graph_data, payloads = loader()

        with self._cache_lock:
            self._graph_cache[mode] = {
                "graph": graph,
                "graph_data": graph_data,
                "payloads": payloads,
                "timestamp": datetime.utcnow(),
            }

        return graph, graph_data, payloads

    def clear_cache(self, source: Optional[str] = None) -> None:
        mode = self._resolve_source(source)
        with self._cache_lock:
            if source is None:
                self._graph_cache.clear()
            else:
                self._graph_cache.pop(mode, None)

    def refresh_cache(self, source: Optional[str] = None) -> Tuple[SparkGraph, GraphData]:
        mode = self._resolve_source(source)
        self.clear_cache(source=mode)
        graph, graph_data = self._load_graph_for_mode(mode)
        return graph, graph_data

    def _load_graph_for_mode(self, mode: str) -> Tuple[SparkGraph, GraphData]:
        if mode == "api":
            graph, data, _ = self._load_live_graph()
            return graph, data
        if mode == "random":
            graph, data, _ = self._load_random_graph()
            return graph, data
        graph, data, _ = self._load_sample_graph()
        return graph, data

    def _load_sample_graph(self) -> Tuple[SparkGraph, GraphData, Dict]:
        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            graph, graph_data = self.repository.load_graph(payloads=SAMPLE_PAYLOAD)
            return graph, graph_data, SAMPLE_PAYLOAD

        return self._with_cache("sample", loader)

    def _load_random_graph(self) -> Tuple[SparkGraph, GraphData, Dict]:
        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            payloads = generate_random_payloads()
            graph, graph_data = self.repository.load_graph(payloads=payloads)
            return graph, graph_data, payloads

        return self._with_cache("random", loader)

    def _load_live_graph(self) -> Tuple[SparkGraph, GraphData, Dict]:
        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            try:
                payloads = self.repository.fetch_payloads()
            except Exception as exc:  # pragma: no cover - exercised in integration
                logger.warning(
                    "Falha ao coletar dados em tempo real; usando payload de exemplo. Erro: %s",
                    exc,
                )
                payloads = SAMPLE_PAYLOAD
            graph, graph_data = self.repository.load_graph(payloads=payloads)
            return graph, graph_data, payloads

        return self._with_cache("api", loader)

    def load_payloads(self, source: Optional[str] = None) -> Dict:
        mode = self._resolve_source(source)
        if mode == "api":
            _, _, payloads = self._load_live_graph()
            return payloads
        if mode == "random":
            _, _, payloads = self._load_random_graph()
            return payloads
        return SAMPLE_PAYLOAD

    def load_graph(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ) -> Tuple[SparkGraph, GraphData]:
        if payloads is not None:
            graph, graph_data = self.repository.load_graph(payloads=payloads)
            return graph, graph_data

        mode = self._resolve_source(source)
        graph, graph_data = self._load_graph_for_mode(mode)
        return graph, graph_data

    def get_graph_summary(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ) -> Dict:
        graph, graph_data = self.load_graph(payloads=payloads, source=source)
        return {
            "nodes": graph.num_vertices(),
            "edges": graph.num_edges(),
            "empenhos": len(graph_data.empenhos),
            "fornecedores": len(graph_data.fornecedores),
            "orgaos": len(graph_data.orgaos),
        }

    def get_anomalies(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ) -> Dict:
        graph, _ = self.load_graph(payloads=payloads, source=source)
        report = analyze_graph(graph)
        return report.as_dict()

    def get_graph_snapshot(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ) -> Dict:
        graph, _ = self.load_graph(payloads=payloads, source=source)
        return to_node_link_data(graph)

    def list_nodes(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ):
        graph, _ = self.load_graph(payloads=payloads, source=source)
        return list(iter_node_summaries(graph))

