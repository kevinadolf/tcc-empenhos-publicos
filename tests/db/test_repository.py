import pytest

from pydantic import ValidationError

from src.common.settings import Settings, get_settings
from src.db.repository import GraphRepository


def test_load_graph_from_payloads(sample_payloads):
    repo = GraphRepository()
    graph, graph_data = repo.load_graph(payloads=sample_payloads)

    assert graph.num_vertices() == 10  # 2 órgãos + 2 fornecedores + 3 empenhos + 3 contratos
    assert graph.num_edges() >= 6  # conexões múltiplas
    assert not graph_data.empenhos.empty
    assert not graph_data.fornecedores.empty
    assert not graph_data.orgaos.empty


def test_fetch_payloads_disabled_by_default(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    get_settings.cache_clear()
    repo = GraphRepository()
    with pytest.raises(RuntimeError):
        repo.fetch_payloads()
    get_settings.cache_clear()


def test_fetch_payloads_enabled(monkeypatch):
    settings = Settings(enable_live_fetch=True)

    class StubClient:
        def fetch_empenhos(self):
            return [{"id": "E"}]

        def fetch_fornecedores(self):
            return [{"id": "F"}]

        def fetch_unidades_gestoras(self):
            return [{"id": "O"}]

    repo = GraphRepository(settings=settings, client=StubClient())
    result = repo.fetch_payloads()
    assert set(result.keys()) == {"empenhos", "fornecedores", "orgaos"}


def test_fetch_payloads_empenho_estado(monkeypatch):
    sample = [
        {
            "Unidade": "Secretaria de Inovação",
            "Ano": 2024,
            "Mes": 5,
            "NumeroEmpenho": 42,
            "TipoPessoa": "JURÍDICA",
            "CPFCNPJ": "12345678000199",
            "Funcao": "TECNOLOGIA",
            "Empenho": 150000.50,
        },
        {
            "Unidade": "Secretaria de Educação",
            "Ano": 2024,
            "Mes": 6,
            "NumeroEmpenho": 77,
            "TipoPessoa": "JURÍDICA",
            "CPFCNPJ": "99887766000111",
            "Funcao": "EDUCAÇÃO",
            "Empenho": 999.0,
        },
    ]

    settings = Settings(enable_live_fetch=True, tce_years=(2024,), tce_max_records=1)

    class StubClient:
        def fetch_empenho_estado(self, **params):
            assert params.get("ano") == "2024"
            return sample

    repo = GraphRepository(settings=settings, client=StubClient())
    payloads = repo.fetch_payloads()

    assert len(payloads["empenhos"]) == 1
    assert len(payloads["fornecedores"]) == 1
    assert payloads["empenhos"][0]["numero"] == "42"
    fornecedor = payloads["fornecedores"][0]
    assert fornecedor["documento"] == "12345678000199"
    assert fornecedor["nome"] == "12345678000199"


def test_validate_payloads_invalid(monkeypatch):
    settings = Settings(enable_live_fetch=False)
    repo = GraphRepository(settings=settings, client=None)
    bad_payload = {"empenhos": [{"numero": "123"}]}  # falta ID obrigatório
    with pytest.raises(ValidationError):
        repo.load_graph(payloads=bad_payload)


def test_enrich_metadata_in_payloads(sample_payloads):
    settings = Settings(enable_live_fetch=False)
    repo = GraphRepository(settings=settings, client=None)
    graph, graph_data = repo.load_graph(payloads=sample_payloads, source_label="manual")

    assert "fonte_origem" in graph_data.empenhos.columns
    assert "data_ingestao" in graph_data.empenhos.columns
    assert "payload_hash" in graph_data.empenhos.columns
    assert set(graph_data.empenhos["fonte_origem"].unique()) == {"manual"}
    assert graph_data.empenhos["payload_hash"].notna().all()
