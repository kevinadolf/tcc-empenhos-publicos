import pytest

from src.common.settings import Settings
from src.db.repository import GraphRepository


def test_load_graph_from_payloads(sample_payloads):
    repo = GraphRepository()
    graph, graph_data = repo.load_graph(payloads=sample_payloads)

    assert graph.number_of_nodes() == 10  # 2 órgãos + 2 fornecedores + 3 empenhos + 3 contratos
    assert graph.number_of_edges() >= 6  # conexões múltiplas
    assert not graph_data.empenhos.empty
    assert not graph_data.fornecedores.empty
    assert not graph_data.orgaos.empty


def test_fetch_payloads_disabled_by_default():
    repo = GraphRepository()
    with pytest.raises(RuntimeError):
        repo.fetch_payloads()


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
