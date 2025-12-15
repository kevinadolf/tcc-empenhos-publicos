"""Utilities to transform raw API payloads into normalized DataFrames."""

from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd


def build_empenhos_df(payload: Iterable[Mapping]) -> pd.DataFrame:
    df = pd.DataFrame(payload)
    if df.empty:
        return df

    column_map = {
        "id": "empenho_id",
        "numero": "numero_empenho",
        "descricao": "descricao",
        "valor_empenhado": "valor_empenhado",
        "data_empenho": "data_empenho",
        "fornecedor_id": "fornecedor_id",
        "unidade_gestora_id": "orgao_id",
        "contrato_id": "contrato_id",
        "fonte_origem": "fonte_origem",
        "data_ingestao": "data_ingestao",
        "payload_hash": "payload_hash",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    normalized_columns = list(column_map.values())
    for column in normalized_columns:
        if column not in df.columns:
            df[column] = pd.NA

    return df[normalized_columns]


def build_fornecedores_df(payload: Iterable[Mapping]) -> pd.DataFrame:
    df = pd.DataFrame(payload)
    if df.empty:
        return df

    column_map = {
        "id": "fornecedor_id",
        "nome": "nome_fornecedor",
        "documento": "documento",
        "tipo_documento": "tipo_documento",
        "municipio": "municipio",
        "uf": "uf",
        "fonte_origem": "fonte_origem",
        "data_ingestao": "data_ingestao",
        "payload_hash": "payload_hash",
    }

    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
    normalized_columns = list(column_map.values())
    for column in normalized_columns:
        if column not in df.columns:
            df[column] = pd.NA

    return df[normalized_columns]


def build_orgaos_df(payload: Iterable[Mapping]) -> pd.DataFrame:
    df = pd.DataFrame(payload)
    if df.empty:
        return df

    column_map = {
        "id": "orgao_id",
        "nome": "nome_orgao",
        "sigla": "sigla",
        "municipio": "municipio",
        "uf": "uf",
        "fonte_origem": "fonte_origem",
        "data_ingestao": "data_ingestao",
        "payload_hash": "payload_hash",
    }

    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
    normalized_columns = list(column_map.values())
    for column in normalized_columns:
        if column not in df.columns:
            df[column] = pd.NA

    return df[normalized_columns]
