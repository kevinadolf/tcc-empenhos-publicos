"""Construct heterogeneous graphs representing the public spending flow."""

from __future__ import annotations

from typing import Optional

import networkx as nx
import pandas as pd

NODE_ORGAO = "orgao"
NODE_FORNECEDOR = "fornecedor"
NODE_EMPENHO = "empenho"
NODE_CONTRATO = "contrato"

EDGE_ORGAO_EMPENHO = "orgao_empenho"
EDGE_EMPENHO_FORNECEDOR = "empenho_fornecedor"
EDGE_EMPENHO_CONTRATO = "empenho_contrato"


def add_orgaos(graph: nx.MultiDiGraph, orgaos_df: pd.DataFrame) -> None:
    for _, row in orgaos_df.iterrows():
        graph.add_node(
            (NODE_ORGAO, row["orgao_id"]),
            type=NODE_ORGAO,
            nome=row.get("nome_orgao"),
            sigla=row.get("sigla"),
            municipio=row.get("municipio"),
            uf=row.get("uf"),
        )


def add_fornecedores(graph: nx.MultiDiGraph, fornecedores_df: pd.DataFrame) -> None:
    for _, row in fornecedores_df.iterrows():
        graph.add_node(
            (NODE_FORNECEDOR, row["fornecedor_id"]),
            type=NODE_FORNECEDOR,
            nome=row.get("nome_fornecedor"),
            documento=row.get("documento"),
            tipo_documento=row.get("tipo_documento"),
            municipio=row.get("municipio"),
            uf=row.get("uf"),
        )


def add_empenhos(
    graph: nx.MultiDiGraph,
    empenhos_df: pd.DataFrame,
    include_contratos: bool = False,
) -> None:
    for _, row in empenhos_df.iterrows():
        node_key = (NODE_EMPENHO, row["empenho_id"])
        valor = row.get("valor_empenhado", 0.0) or 0.0
        graph.add_node(
            node_key,
            type=NODE_EMPENHO,
            numero=row.get("numero_empenho"),
            descricao=row.get("descricao"),
            valor=valor,
            data=row.get("data_empenho"),
        )

        if pd.notna(row.get("orgao_id")):
            graph.add_edge(
                (NODE_ORGAO, row["orgao_id"]),
                node_key,
                key=EDGE_ORGAO_EMPENHO,
                tipo=EDGE_ORGAO_EMPENHO,
                valor=valor,
            )

        if pd.notna(row.get("fornecedor_id")):
            graph.add_edge(
                node_key,
                (NODE_FORNECEDOR, row["fornecedor_id"]),
                key=EDGE_EMPENHO_FORNECEDOR,
                tipo=EDGE_EMPENHO_FORNECEDOR,
                valor=valor,
            )

        if include_contratos and pd.notna(row.get("contrato_id")):
            graph.add_node(
                (NODE_CONTRATO, row["contrato_id"]),
                type=NODE_CONTRATO,
            )
            graph.add_edge(
                node_key,
                (NODE_CONTRATO, row["contrato_id"]),
                key=EDGE_EMPENHO_CONTRATO,
                tipo=EDGE_EMPENHO_CONTRATO,
            )


def build_heterogeneous_graph(
    empenhos_df: pd.DataFrame,
    fornecedores_df: Optional[pd.DataFrame] = None,
    orgaos_df: Optional[pd.DataFrame] = None,
    include_contratos: bool = False,
) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()

    fornecedores_df = fornecedores_df if fornecedores_df is not None else pd.DataFrame()
    orgaos_df = orgaos_df if orgaos_df is not None else pd.DataFrame()

    if not empenhos_df.empty:
        add_empenhos(graph, empenhos_df, include_contratos=include_contratos)

    if not fornecedores_df.empty:
        add_fornecedores(graph, fornecedores_df)

    if not orgaos_df.empty:
        add_orgaos(graph, orgaos_df)

    return graph


def filter_empenhos_by_period(
    empenhos_df: pd.DataFrame,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    filtered = empenhos_df.copy()
    if filtered.empty:
        return filtered

    filtered["data_empenho"] = pd.to_datetime(filtered["data_empenho"], errors="coerce")
    if start:
        filtered = filtered[filtered["data_empenho"] >= pd.to_datetime(start)]
    if end:
        filtered = filtered[filtered["data_empenho"] <= pd.to_datetime(end)]

    return filtered
