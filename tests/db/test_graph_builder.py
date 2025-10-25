from src.db.graph_builder import (
    EDGE_EMPENHO_FORNECEDOR,
    EDGE_ORGAO_EMPENHO,
    NODE_EMPENHO,
    NODE_FORNECEDOR,
    NODE_ORGAO,
    build_heterogeneous_graph,
    filter_empenhos_by_period,
)


def test_graph_has_expected_nodes(sample_dataframes):
    empenhos, fornecedores, orgaos = sample_dataframes
    graph = build_heterogeneous_graph(
        empenhos,
        fornecedores_df=fornecedores,
        orgaos_df=orgaos,
        include_contratos=False,
    )

    orgao_nodes = [n for n in graph.nodes if n[0] == NODE_ORGAO]
    fornecedor_nodes = [n for n in graph.nodes if n[0] == NODE_FORNECEDOR]
    empenho_nodes = [n for n in graph.nodes if n[0] == NODE_EMPENHO]

    assert len(orgao_nodes) == 2
    assert len(fornecedor_nodes) == 2
    assert len(empenho_nodes) == 3


def test_graph_edges_connect_entities(sample_graph):
    edges = list(sample_graph.edges(data=True, keys=True))
    has_orgao_empenho = any(edge[2] == EDGE_ORGAO_EMPENHO for edge in edges)
    has_empenho_fornecedor = any(edge[2] == EDGE_EMPENHO_FORNECEDOR for edge in edges)

    assert has_orgao_empenho
    assert has_empenho_fornecedor


def test_filter_empenhos_by_period(sample_dataframes):
    empenhos, _, _ = sample_dataframes
    filtered = filter_empenhos_by_period(empenhos, start="2023-02-01", end="2023-02-28")

    assert len(filtered) == 1
    assert filtered.iloc[0]["numero_empenho"] == "2023-0002"
