from src.analysis.detectors import (
    detect_centrality_outliers,
    detect_entropy_outliers,
    run_default_anomaly_suite,
)
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO


def test_detect_centrality_outliers(sample_graph):
    anomalies = detect_centrality_outliers(sample_graph, NODE_FORNECEDOR, top_k=3)
    assert anomalies
    first = anomalies[0]
    assert first.reason
    assert first.severity in {"alta", "media", "baixa"}
    assert "subgraph" in first.context
    assert first.context["subgraph"]["nodes"]


def test_detect_entropy_outliers(sample_graph):
    anomalies = detect_entropy_outliers(
        sample_graph,
        focus_type=NODE_FORNECEDOR,
        neighbor_type="orgao",
        threshold=0.0,
    )
    assert anomalies
    first = anomalies[0]
    assert first.context["subgraph"]["nodes"]
    assert first.context["top_neighbors"]


def test_run_default_suite(sample_graph):
    report = run_default_anomaly_suite(sample_graph)
    assert "centralidade_fornecedores" in report
    assert report["centralidade_fornecedores"]
    assert report["alta_entropia_fornecedores"]
    assert "comunidades_isoladas" in report
    assert isinstance(report["comunidades_isoladas"], list)
    assert all(item.severity in {"alta", "media", "baixa"} for item in report["centralidade_orgaos"])
