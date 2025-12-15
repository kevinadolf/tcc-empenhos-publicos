"""Construct heterogeneous graphs representing the public spending flow using Spark."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from src.common.spark_graph import SparkGraph
from src.common.spark_session import get_spark_session

NODE_ORGAO = "orgao"
NODE_FORNECEDOR = "fornecedor"
NODE_EMPENHO = "empenho"
NODE_CONTRATO = "contrato"

EDGE_ORGAO_EMPENHO = "orgao_empenho"
EDGE_EMPENHO_FORNECEDOR = "empenho_fornecedor"
EDGE_EMPENHO_CONTRATO = "empenho_contrato"

VERTEX_COLUMNS = [
    "id",
    "node_type",
    "original_id",
    "nome",
    "sigla",
    "municipio",
    "uf",
    "documento",
    "tipo_documento",
    "numero",
    "descricao",
    "valor",
    "data",
    "fonte_origem",
    "data_ingestao",
    "payload_hash",
]

VERTEX_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), False),
        T.StructField("node_type", T.StringType(), False),
        T.StructField("original_id", T.StringType(), True),
        T.StructField("nome", T.StringType(), True),
        T.StructField("sigla", T.StringType(), True),
        T.StructField("municipio", T.StringType(), True),
        T.StructField("uf", T.StringType(), True),
        T.StructField("documento", T.StringType(), True),
        T.StructField("tipo_documento", T.StringType(), True),
        T.StructField("numero", T.StringType(), True),
        T.StructField("descricao", T.StringType(), True),
        T.StructField("valor", T.DoubleType(), True),
        T.StructField("data", T.StringType(), True),
        T.StructField("fonte_origem", T.StringType(), True),
        T.StructField("data_ingestao", T.StringType(), True),
        T.StructField("payload_hash", T.StringType(), True),
    ],
)

EDGE_SCHEMA = T.StructType(
    [
        T.StructField("src", T.StringType(), False),
        T.StructField("dst", T.StringType(), False),
        T.StructField("edge_type", T.StringType(), False),
        T.StructField("valor", T.DoubleType(), True),
    ],
)


def _sanitize_float(series: Iterable) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def _sanitize_timestamp(series: Iterable) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _prepare_empenho_vertices(empenhos_df: pd.DataFrame) -> pd.DataFrame:
    if empenhos_df.empty:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    df = empenhos_df.copy()
    df["id"] = df["empenho_id"].map(lambda x: f"{NODE_EMPENHO}::{x}")
    df["node_type"] = NODE_EMPENHO
    df["original_id"] = df["empenho_id"]
    df["nome"] = df.get("descricao")
    df["sigla"] = None
    df["municipio"] = None
    df["uf"] = None
    df["documento"] = None
    df["tipo_documento"] = None
    df["numero"] = df.get("numero_empenho")
    df["descricao"] = df.get("descricao")
    df["valor"] = _sanitize_float(df.get("valor_empenhado", 0.0))
    df["data"] = _sanitize_timestamp(df.get("data_empenho")).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["data"] = df["data"].fillna(pd.NA)
    df["fonte_origem"] = df.get("fonte_origem")
    df["data_ingestao"] = df.get("data_ingestao")
    df["payload_hash"] = df.get("payload_hash")
    return df[VERTEX_COLUMNS]


def _prepare_fornecedor_vertices(fornecedores_df: pd.DataFrame) -> pd.DataFrame:
    if fornecedores_df.empty:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    df = fornecedores_df.copy()
    df["id"] = df["fornecedor_id"].map(lambda x: f"{NODE_FORNECEDOR}::{x}")
    df["node_type"] = NODE_FORNECEDOR
    df["original_id"] = df["fornecedor_id"]
    df["nome"] = df.get("nome_fornecedor")
    df["sigla"] = None
    df["municipio"] = df.get("municipio")
    df["uf"] = df.get("uf")
    df["documento"] = df.get("documento")
    df["tipo_documento"] = df.get("tipo_documento")
    df["numero"] = None
    df["descricao"] = None
    df["valor"] = None
    df["data"] = None
    df["fonte_origem"] = df.get("fonte_origem")
    df["data_ingestao"] = df.get("data_ingestao")
    df["payload_hash"] = df.get("payload_hash")
    return df[VERTEX_COLUMNS]


def _prepare_orgao_vertices(orgaos_df: pd.DataFrame) -> pd.DataFrame:
    if orgaos_df.empty:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    df = orgaos_df.copy()
    df["id"] = df["orgao_id"].map(lambda x: f"{NODE_ORGAO}::{x}")
    df["node_type"] = NODE_ORGAO
    df["original_id"] = df["orgao_id"]
    df["nome"] = df.get("nome_orgao")
    df["sigla"] = df.get("sigla")
    df["municipio"] = df.get("municipio")
    df["uf"] = df.get("uf")
    df["documento"] = None
    df["tipo_documento"] = None
    df["numero"] = None
    df["descricao"] = None
    df["valor"] = None
    df["data"] = None
    df["fonte_origem"] = df.get("fonte_origem")
    df["data_ingestao"] = df.get("data_ingestao")
    df["payload_hash"] = df.get("payload_hash")
    return df[VERTEX_COLUMNS]


def _prepare_contrato_vertices(empenhos_df: pd.DataFrame) -> pd.DataFrame:
    if empenhos_df.empty or "contrato_id" not in empenhos_df.columns:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    contratos = empenhos_df[["contrato_id"]].dropna().drop_duplicates()
    if contratos.empty:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    contratos["id"] = contratos["contrato_id"].map(lambda x: f"{NODE_CONTRATO}::{x}")
    contratos["node_type"] = NODE_CONTRATO
    contratos["original_id"] = contratos["contrato_id"]
    contratos["nome"] = None
    contratos["sigla"] = None
    contratos["municipio"] = None
    contratos["uf"] = None
    contratos["documento"] = None
    contratos["tipo_documento"] = None
    contratos["numero"] = None
    contratos["descricao"] = None
    contratos["valor"] = None
    contratos["data"] = None
    contratos["fonte_origem"] = None
    contratos["data_ingestao"] = None
    contratos["payload_hash"] = None
    return contratos[VERTEX_COLUMNS]


def _prepare_vertices(
    empenhos_df: pd.DataFrame,
    fornecedores_df: pd.DataFrame,
    orgaos_df: pd.DataFrame,
    include_contratos: bool,
) -> pd.DataFrame:
    frames = [
        _prepare_empenho_vertices(empenhos_df),
        _prepare_fornecedor_vertices(fornecedores_df),
        _prepare_orgao_vertices(orgaos_df),
    ]
    if include_contratos:
        frames.append(_prepare_contrato_vertices(empenhos_df))

    if not frames:
        return pd.DataFrame(columns=VERTEX_COLUMNS)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=VERTEX_COLUMNS)
    return combined.fillna(value=pd.NA)


def _prepare_edges(
    empenhos_df: pd.DataFrame,
    include_contratos: bool,
) -> pd.DataFrame:
    if empenhos_df.empty:
        return pd.DataFrame(columns=EDGE_SCHEMA.names)

    df = empenhos_df.copy()
    df["valor"] = _sanitize_float(df.get("valor_empenhado", 0.0))

    edges: list[pd.DataFrame] = []

    if "orgao_id" in df.columns:
        orgao_edges = df.dropna(subset=["orgao_id"])
        if not orgao_edges.empty:
            orgao_edges = pd.DataFrame(
                {
                    "src": orgao_edges["orgao_id"].map(lambda x: f"{NODE_ORGAO}::{x}"),
                    "dst": orgao_edges["empenho_id"].map(lambda x: f"{NODE_EMPENHO}::{x}"),
                    "edge_type": EDGE_ORGAO_EMPENHO,
                    "valor": orgao_edges["valor"],
                },
            )
            edges.append(orgao_edges)

    if "fornecedor_id" in df.columns:
        fornecedor_edges = df.dropna(subset=["fornecedor_id"])
        if not fornecedor_edges.empty:
            fornecedor_edges = pd.DataFrame(
                {
                    "src": fornecedor_edges["empenho_id"].map(lambda x: f"{NODE_EMPENHO}::{x}"),
                    "dst": fornecedor_edges["fornecedor_id"].map(lambda x: f"{NODE_FORNECEDOR}::{x}"),
                    "edge_type": EDGE_EMPENHO_FORNECEDOR,
                    "valor": fornecedor_edges["valor"],
                },
            )
            edges.append(fornecedor_edges)

    if include_contratos and "contrato_id" in df.columns:
        contrato_edges = df.dropna(subset=["contrato_id"])
        if not contrato_edges.empty:
            contrato_edges = pd.DataFrame(
                {
                    "src": contrato_edges["empenho_id"].map(lambda x: f"{NODE_EMPENHO}::{x}"),
                    "dst": contrato_edges["contrato_id"].map(lambda x: f"{NODE_CONTRATO}::{x}"),
                    "edge_type": EDGE_EMPENHO_CONTRATO,
                    "valor": contrato_edges["valor"],
                },
            )
            edges.append(contrato_edges)

    if not edges:
        return pd.DataFrame(columns=EDGE_SCHEMA.names)

    combined = pd.concat(edges, ignore_index=True)
    return combined.fillna(value=pd.NA)


def _create_spark_dataframe(
    spark: SparkSession,
    data: pd.DataFrame,
    schema: T.StructType,
) -> DataFrame:
    if data.empty:
        return spark.createDataFrame([], schema)
    rows = data.replace({pd.NA: None}).to_dict(orient="records")
    return spark.createDataFrame(rows, schema=schema)


def build_heterogeneous_graph(
    empenhos_df: pd.DataFrame,
    fornecedores_df: Optional[pd.DataFrame] = None,
    orgaos_df: Optional[pd.DataFrame] = None,
    include_contratos: bool = False,
    spark: Optional[SparkSession] = None,
) -> SparkGraph:
    spark_session = spark or get_spark_session()
    fornecedores_df = fornecedores_df if fornecedores_df is not None else pd.DataFrame()
    orgaos_df = orgaos_df if orgaos_df is not None else pd.DataFrame()

    vertices_pdf = _prepare_vertices(
        empenhos_df if empenhos_df is not None else pd.DataFrame(),
        fornecedores_df,
        orgaos_df,
        include_contratos,
    )
    edges_pdf = _prepare_edges(
        empenhos_df if empenhos_df is not None else pd.DataFrame(),
        include_contratos,
    )

    vertices_sdf = _create_spark_dataframe(spark_session, vertices_pdf, VERTEX_SCHEMA)
    edges_sdf = _create_spark_dataframe(spark_session, edges_pdf, EDGE_SCHEMA)

    return SparkGraph(
        spark=spark_session,
        vertices=vertices_sdf,
        edges=edges_sdf,
    )


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
