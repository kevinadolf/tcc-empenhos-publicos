import pandas as pd

from src.db.dataframes import (
    build_empenhos_df,
    build_fornecedores_df,
    build_orgaos_df,
)


def test_build_empenhos_df_maps_columns(sample_payloads):
    df = build_empenhos_df(sample_payloads["empenhos"])
    expected = {
        "empenho_id",
        "numero_empenho",
        "descricao",
        "valor_empenhado",
        "data_empenho",
        "fornecedor_id",
        "orgao_id",
        "contrato_id",
        "fonte_origem",
        "data_ingestao",
        "payload_hash",
    }
    assert expected.issubset(set(df.columns))
    assert df.iloc[0]["empenho_id"] == "E1"


def test_build_fornecedores_df_handles_empty():
    df = build_fornecedores_df([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_build_fornecedores_df_missing_fields():
    payload = [{"id": "F1"}]
    df = build_fornecedores_df(payload)
    assert pd.isna(df.iloc[0]["nome_fornecedor"])  # coluna preenchida com NA


def test_build_orgaos_df(sample_payloads):
    df = build_orgaos_df(sample_payloads["orgaos"])
    assert df.iloc[0]["nome_orgao"] == "Secretaria de Educação"


def test_build_empenhos_df_missing_fields():
    payload = [{"id": "E10"}]
    df = build_empenhos_df(payload)
    assert pd.isna(df.iloc[0]["numero_empenho"])
