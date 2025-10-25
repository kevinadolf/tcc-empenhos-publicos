"""Placeholder datasets for local development without live API access."""

SAMPLE_PAYLOAD = {
    "empenhos": [
        {
            "id": "E100",
            "numero": "2023-100",
            "descricao": "Contratação de serviços de TI",
            "valor_empenhado": 180000.0,
            "data_empenho": "2023-04-10",
            "fornecedor_id": "F100",
            "unidade_gestora_id": "O100",
            "contrato_id": "C100",
        },
        {
            "id": "E101",
            "numero": "2023-101",
            "descricao": "Compra de materiais médicos",
            "valor_empenhado": 95000.0,
            "data_empenho": "2023-05-05",
            "fornecedor_id": "F101",
            "unidade_gestora_id": "O101",
            "contrato_id": "C101",
        },
    ],
    "fornecedores": [
        {
            "id": "F100",
            "nome": "GigaTech Solutions",
            "documento": "11.111.111/0001-11",
            "tipo_documento": "CNPJ",
            "municipio": "Rio de Janeiro",
            "uf": "RJ",
        },
        {
            "id": "F101",
            "nome": "SaudeMais Distribuidora",
            "documento": "22.222.222/0001-22",
            "tipo_documento": "CNPJ",
            "municipio": "Niterói",
            "uf": "RJ",
        },
    ],
    "orgaos": [
        {
            "id": "O100",
            "nome": "Secretaria de Planejamento",
            "sigla": "SEPLAN",
            "municipio": "Rio de Janeiro",
            "uf": "RJ",
        },
        {
            "id": "O101",
            "nome": "Secretaria de Saúde",
            "sigla": "SESAU",
            "municipio": "Rio de Janeiro",
            "uf": "RJ",
        },
    ],
}
