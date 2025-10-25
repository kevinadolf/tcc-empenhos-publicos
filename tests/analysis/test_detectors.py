from src.analysis.detectors import (
    detect_entropy_outliers,
    run_default_anomaly_suite,
    top_weighted_centrality,
)
from src.db.graph_builder import NODE_FORNECEDOR


def test_top_weighted_centrality(sample_graph):
    ranking = top_weighted_centrality(sample_graph, NODE_FORNECEDOR)
    assert len(ranking) == 2
    assert ranking[0][1] >= ranking[1][1]


def test_detect_entropy_outliers(sample_graph):
    anomalies = detect_entropy_outliers(
        sample_graph,
        focus_type=NODE_FORNECEDOR,
        neighbor_type="orgao",
        threshold=0.0,
    )
    assert anomalies


def test_run_default_suite(sample_graph):
    report = run_default_anomaly_suite(sample_graph)
    assert "centralidade_fornecedores" in report
    assert report["centralidade_fornecedores"]
