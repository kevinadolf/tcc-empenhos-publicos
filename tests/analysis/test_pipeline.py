from src.analysis.pipeline import analyze_graph


def test_analyze_graph_returns_report(sample_graph):
    report = analyze_graph(sample_graph)
    assert report.anomalies
    as_dict = report.as_dict()
    assert isinstance(as_dict, dict)
    assert "centralidade_fornecedores" in as_dict
