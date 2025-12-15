"""Domain service orchestrating graph loading and analysis."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Callable, Dict, Optional, Tuple, cast

from src.analysis.pipeline import analyze_graph
from src.analysis.scoring import compute_node_risk
from src.backend import metrics
from src.backend.services.random_payloads import generate_random_payloads
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.graph_serialization import iter_node_summaries, to_node_link_data
from src.common.progress import ProgressTracker
from src.common.spark_graph import SparkGraph
from src.common.settings import get_settings
from src.db.repository import GraphData, GraphRepository

logger = logging.getLogger(__name__)

DATA_SOURCES = {"sample", "api", "random"}


class GraphService:
    def __init__(
        self,
        repository: Optional[GraphRepository] = None,
        *,
        auto_prefetch: bool = False,
    ) -> None:
        self.settings = get_settings()
        self.repository = repository or GraphRepository(self.settings)
        self._cache_lock = Lock()
        self._graph_cache: Dict[str, Dict[str, Any]] = {}
        ttl_seconds = self.settings.graph_cache_ttl_seconds
        self._cache_ttl = (
            timedelta(seconds=ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None
        )
        self._prefetch_event = Event()
        self._prefetch_event.set()
        self._target_live_source: Optional[str] = None
        self._default_source = self._normalize_default_source(self.settings.graph_data_source)
        self._fetch_tracker = ProgressTracker("TCE-RJ fetch")
        self._fetch_tracker.subscribe(self._log_progress_update)
        self._background_thread: Optional[Thread] = None
        self._config_watch_stop = Event()
        self._config_watcher_thread = Thread(
            target=self._watch_env_file,
            name="env-config-watcher",
            daemon=True,
        )
        self._config_watcher_thread.start()

        if auto_prefetch and self.settings.enable_live_fetch:
            self.start_background_fetch()

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
        if self.settings.enable_live_fetch:
            if normalized == "random":
                self._target_live_source = None
                return "random"
            self._target_live_source = "api"
            return "sample"
        if normalized == "api" and not self.settings.enable_live_fetch:
            return "sample"
        return normalized

    def _log_progress_update(self, state: Dict[str, Any]) -> None:
        progress = int(state.get("progress", 0))
        message = str(state.get("message", ""))
        status = str(state.get("status", "running"))
        bar_length = 20
        filled = max(0, min(bar_length, progress * bar_length // 100))
        bar = "#" * filled + "-" * (bar_length - filled)
        line = f"[TCE-RJ] {progress:3d}% [{bar}] {message} ({status})"
        output = f"{line}\n" + "-" * len(line)
        if logger.isEnabledFor(logging.INFO):
            logger.info("%s", output)
        else:
            print(output, file=sys.stderr)

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
        *,
        force_refresh: bool = False,
    ) -> Tuple[SparkGraph, GraphData, Dict]:
        with self._cache_lock:
            if not force_refresh and not self._cache_expired(mode):
                cached = self._graph_cache[mode]
                return (
                    cast(SparkGraph, cached["graph"]),
                    cast(GraphData, cached["graph_data"]),
                    cast(Dict, cached["payloads"]),
                )

        graph, graph_data, payloads = loader()

        self._store_in_cache(mode, graph, graph_data, payloads)

        return graph, graph_data, payloads

    def _store_in_cache(
        self,
        mode: str,
        graph: SparkGraph,
        graph_data: GraphData,
        payloads: Dict,
    ) -> None:
        with self._cache_lock:
            self._graph_cache[mode] = {
                "graph": graph,
                "graph_data": graph_data,
                "payloads": payloads,
                "timestamp": datetime.utcnow(),
            }

    def clear_cache(self, source: Optional[str] = None) -> None:
        mode = self._resolve_source(source)
        with self._cache_lock:
            if source is None:
                self._graph_cache.clear()
            else:
                self._graph_cache.pop(mode, None)

    def refresh_cache(
        self,
        source: Optional[str] = None,
        *,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> Tuple[SparkGraph, GraphData]:
        mode = self._resolve_source(source)
        graph, graph_data = self._load_graph_for_mode(
            mode,
            force_refresh=True,
            progress_callback=progress_callback,
        )
        return graph, graph_data

    def _load_graph_for_mode(
        self,
        mode: str,
        *,
        force_refresh: bool = False,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> Tuple[SparkGraph, GraphData]:
        if mode == "api":
            graph, data, _ = self._load_live_graph(
                force_refresh=force_refresh,
                progress_callback=progress_callback,
            )
            return graph, data
        if mode == "random":
            graph, data, _ = self._load_random_graph(force_refresh=force_refresh)
            return graph, data
        graph, data, _ = self._load_sample_graph(force_refresh=force_refresh)
        return graph, data

    def _load_sample_graph(
        self,
        *,
        force_refresh: bool = False,
    ) -> Tuple[SparkGraph, GraphData, Dict]:
        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            with metrics.graph_build_duration.labels("sample").time():
                graph, graph_data = self.repository.load_graph(payloads=SAMPLE_PAYLOAD, source_label="sample")
            return graph, graph_data, SAMPLE_PAYLOAD

        return self._with_cache("sample", loader, force_refresh=force_refresh)

    def _load_random_graph(
        self,
        *,
        force_refresh: bool = False,
    ) -> Tuple[SparkGraph, GraphData, Dict]:
        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            payloads = generate_random_payloads()
            with metrics.graph_build_duration.labels("random").time():
                graph, graph_data = self.repository.load_graph(payloads=payloads, source_label="random")
            return graph, graph_data, payloads

        return self._with_cache("random", loader, force_refresh=force_refresh)

    def _load_live_graph(
        self,
        *,
        force_refresh: bool = False,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> Tuple[SparkGraph, GraphData, Dict]:
        if not force_refresh:
            self._await_prefetch_if_running(progress_callback=progress_callback)

        def loader() -> Tuple[SparkGraph, GraphData, Dict]:
            try:
                def repo_progress(progress: float, message: str) -> None:
                    if progress_callback:
                        scaled = 5.0 + (progress / 100.0) * 70.0
                        progress_callback(min(scaled, 80.0), message)

                payloads = self.repository.fetch_payloads(progress_callback=repo_progress)
            except Exception as exc:  # pragma: no cover - exercised in integration
                if progress_callback:
                    progress_callback(5.0, "Falha ao coletar dados; usando payload de exemplo")
                logger.warning(
                    "Falha ao coletar dados em tempo real; usando payload de exemplo. Erro: %s",
                    exc,
                )
                metrics.fetch_failures.labels("api").inc()
                metrics.fetch_status.labels("api").set(0)
                payloads = SAMPLE_PAYLOAD
            else:
                if progress_callback:
                    progress_callback(85.0, "Construindo grafo com dados do TCE-RJ")
            with metrics.graph_build_duration.labels("api").time():
                graph, graph_data = self.repository.load_graph(payloads=payloads, source_label="api")
            metrics.fetch_status.labels("api").set(1)
            if progress_callback:
                progress_callback(95.0, "Atualizando cache com grafo mais recente")
            return graph, graph_data, payloads

        return self._with_cache("api", loader, force_refresh=force_refresh)

    def start_background_fetch(self) -> None:
        if self._background_thread and self._background_thread.is_alive():
            return
        if not self.settings.enable_live_fetch or self._target_live_source != "api":
            self._fetch_tracker.update(0.0, "Coleta remota desativada nas configurações")
            logger.info("Live fetch disabled; skipping background prefetch.")
            self._prefetch_event.set()
            return
        self._prefetch_event.clear()
        self._background_thread = Thread(
            target=self._run_background_fetch,
            name="tce-live-fetch",
            daemon=True,
        )
        self._background_thread.start()

    def _run_background_fetch(self) -> None:
        self._fetch_tracker.start("Iniciando coleta assíncrona do TCE-RJ")

        def tracker_progress(progress: float, message: str) -> None:
            self._fetch_tracker.update(progress, message)

        success = False
        try:
            graph, graph_data, _ = self._load_live_graph(
                force_refresh=True,
                progress_callback=tracker_progress,
            )
            success = True
        except Exception as exc:  # pragma: no cover - requires integration
            self._fetch_tracker.fail(f"Falha na coleta: {exc}")
            logger.exception("Erro durante coleta assíncrona do TCE-RJ: %s", exc)
        else:
            tracker_progress(100.0, "Cache sincronizado com dados do TCE-RJ")
            self._fetch_tracker.complete("Grafo TCE-RJ disponível")
            logger.info("Coleta assíncrona do TCE-RJ concluída com sucesso.")
            if success and self._target_live_source:
                self._default_source = self._target_live_source
                self._target_live_source = None
        finally:
            self._prefetch_event.set()

    def _await_prefetch_if_running(
        self,
        *,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> None:
        thread = self._background_thread
        if thread and thread.is_alive():
            if progress_callback:
                progress_callback(3.0, "Aguardando sincronização em andamento")
            logger.info("Aguardando coleta TCE-RJ em andamento concluir antes de nova requisição.")
            self._prefetch_event.wait()

    def _watch_env_file(self) -> None:
        env_path = Path(".env")
        try:
            last_mtime = env_path.stat().st_mtime
        except FileNotFoundError:
            last_mtime = None

        while not self._config_watch_stop.wait(3.0):
            try:
                current_mtime = env_path.stat().st_mtime
            except FileNotFoundError:
                current_mtime = None

            if current_mtime != last_mtime:
                last_mtime = current_mtime
                try:
                    self._handle_env_change()
                except Exception as exc:  # pragma: no cover - defensivo
                    logger.exception("Falha ao reagir à alteração do .env: %s", exc)

    def _handle_env_change(self) -> None:
        logger.info("Arquivo .env atualizado; recarregando configurações do backend.")
        self._prefetch_event.wait()
        self._fetch_tracker.update(0.0, "Configurações atualizadas, aplicando mudanças")
        get_settings.cache_clear()
        self.settings = get_settings()
        self.repository = GraphRepository(self.settings)
        self.clear_cache()
        self._default_source = self._normalize_default_source(self.settings.graph_data_source)
        if self.settings.enable_live_fetch:
            self.start_background_fetch()
        else:
            self._target_live_source = None

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
        anomalies = report.as_dict()
        for detector, items in anomalies.items():
            for item in items or []:
                severity = str(item.get("severity") or "media").lower()
                metrics.anomalies_detected.labels(detector=detector, severity=severity).inc()
        return anomalies

    def get_risk_scores(
        self,
        payloads: Optional[Dict] = None,
        *,
        source: Optional[str] = None,
    ) -> Dict[str, Dict[str, float]]:
        anomalies = self.get_anomalies(payloads=payloads, source=source)
        return compute_node_risk(anomalies)

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

    def get_fetch_status(self) -> Dict[str, Any]:
        """Expose current background fetch status for health endpoints."""
        return self._fetch_tracker.status()
