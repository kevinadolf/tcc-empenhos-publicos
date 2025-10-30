from pyspark.sql import functions as F

from src.db.graph_builder import (
    EDGE_EMPENHO_FORNECEDOR,
    EDGE_ORGAO_EMPENHO,
    NODE_EMPENHO,
    NODE_FORNECEDOR,
    NODE_ORGAO,
    build_heterogeneous_graph,
    filter_empenhos_by_period,
)


def test_graph_has_expected_nodes(sample_dataframes, spark):
    empenhos, fornecedores, orgaos = sample_dataframes
    graph = build_heterogeneous_graph(
        empenhos,
        fornecedores_df=fornecedores,
        orgaos_df=orgaos,
        include_contratos=False,
        spark=spark,
    )

    vertices = graph.vertices
    assert vertices.filter(F.col("node_type") == NODE_ORGAO).count() == 2
    assert vertices.filter(F.col("node_type") == NODE_FORNECEDOR).count() == 2
    assert vertices.filter(F.col("node_type") == NODE_EMPENHO).count() == 3


def test_graph_edges_connect_entities(sample_graph):
    edges = sample_graph.edges
    assert (
        edges.filter(F.col("edge_type") == EDGE_ORGAO_EMPENHO).count() > 0
    )
    assert (
        edges.filter(F.col("edge_type") == EDGE_EMPENHO_FORNECEDOR).count() > 0
    )


def test_filter_empenhos_by_period(sample_dataframes):
    empenhos, _, _ = sample_dataframes
    filtered = filter_empenhos_by_period(empenhos, start="2023-02-01", end="2023-02-28")

    assert len(filtered) == 1
    assert filtered.iloc[0]["numero_empenho"] == "2023-0002"
