"""Prometheus metrics for the backend."""

from prometheus_client import Counter, Gauge, Summary

graph_build_duration = Summary(
    "graph_build_duration_seconds",
    "Tempo para construir o grafo a partir dos payloads",
    ["source"],
)

fetch_failures = Counter(
    "data_fetch_failures_total",
    "Falhas ao coletar dados da API externa",
    ["source"],
)

anomalies_detected = Counter(
    "anomalies_detected_total",
    "Anomalias detectadas por tipo e severidade",
    ["detector", "severity"],
)

fetch_status = Gauge(
    "data_fetch_status",
    "Estado da coleta atual (1=sucesso, 0=falha/indisponível)",
    ["source"],
)
