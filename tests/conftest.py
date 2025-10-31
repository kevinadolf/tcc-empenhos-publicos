import os
from pathlib import Path

import pandas as pd
import pytest

import pyspark

from src.db.dataframes import (
    build_empenhos_df,
    build_fornecedores_df,
    build_orgaos_df,
)
from src.db.graph_builder import build_heterogeneous_graph
from src.common.spark_session import get_spark_session


if not os.environ.get("SPARK_HOME"):
    os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).resolve().parent)


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("TCCPytests")


@pytest.fixture
def sample_payloads():
    return {
        "empenhos": [
            {
                "id": "E1",
                "numero": "2023-0001",
                "descricao": "Aquisição de computadores",
                "valor_empenhado": 150000.0,
                "data_empenho": "2023-01-15",
                "fornecedor_id": "F1",
                "unidade_gestora_id": "O1",
                "contrato_id": "C1",
            },
            {
                "id": "E2",
                "numero": "2023-0002",
                "descricao": "Serviços de manutenção",
                "valor_empenhado": 90000.0,
                "data_empenho": "2023-02-10",
                "fornecedor_id": "F2",
                "unidade_gestora_id": "O1",
                "contrato_id": "C2",
            },
            {
                "id": "E3",
                "numero": "2023-0003",
                "descricao": "Consultoria especializada",
                "valor_empenhado": 250000.0,
                "data_empenho": "2023-03-12",
                "fornecedor_id": "F1",
                "unidade_gestora_id": "O2",
                "contrato_id": "C3",
            },
        ],
        "fornecedores": [
            {
                "id": "F1",
                "nome": "TecnoData Ltda",
                "documento": "12.345.678/0001-90",
                "tipo_documento": "CNPJ",
                "municipio": "Rio de Janeiro",
                "uf": "RJ",
            },
            {
                "id": "F2",
                "nome": "Servicos Gerais ME",
                "documento": "98.765.432/0001-10",
                "tipo_documento": "CNPJ",
                "municipio": "Niterói",
                "uf": "RJ",
            },
        ],
        "orgaos": [
            {
                "id": "O1",
                "nome": "Secretaria de Educação",
                "sigla": "SEDUC",
                "municipio": "Rio de Janeiro",
                "uf": "RJ",
            },
            {
                "id": "O2",
                "nome": "Secretaria de Saúde",
                "sigla": "SESAU",
                "municipio": "Rio de Janeiro",
                "uf": "RJ",
            },
        ],
    }


@pytest.fixture
def sample_dataframes(sample_payloads):
    empenhos = build_empenhos_df(sample_payloads["empenhos"])
    fornecedores = build_fornecedores_df(sample_payloads["fornecedores"])
    orgaos = build_orgaos_df(sample_payloads["orgaos"])
    return empenhos, fornecedores, orgaos


@pytest.fixture
def sample_graph(sample_dataframes, spark):
    empenhos, fornecedores, orgaos = sample_dataframes
    return build_heterogeneous_graph(
        empenhos,
        fornecedores_df=fornecedores,
        orgaos_df=orgaos,
        include_contratos=True,
        spark=spark,
    )
