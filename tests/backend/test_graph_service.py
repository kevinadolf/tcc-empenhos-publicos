from src.backend.services.graph_service import GraphService
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.settings import get_settings
from src.db.repository import GraphRepository


def reset_settings_cache():
    get_settings.cache_clear()


def test_load_payloads_uses_sample(monkeypatch):
    # Force settings to disable live fetch
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    reset_settings_cache()
    service = GraphService()
    payloads = service.load_payloads()
    assert payloads == SAMPLE_PAYLOAD


def test_get_graph_summary_returns_counts(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    reset_settings_cache()
    service = GraphService()
    summary = service.get_graph_summary()
    assert summary["nodes"] > 0
    assert summary["empenhos"] == len(SAMPLE_PAYLOAD["empenhos"])


def test_get_anomalies_has_expected_keys(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    reset_settings_cache()
    service = GraphService()
    anomalies = service.get_anomalies()
    assert "centralidade_fornecedores" in anomalies
    assert isinstance(anomalies["centralidade_fornecedores"], list)


def test_load_graph_with_custom_payload(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    reset_settings_cache()
    service = GraphService()
    custom_payload = {
        "empenhos": [
            {
                "id": "E-A",
                "numero": "2025-001",
                "valor_empenhado": 1000.0,
                "fornecedor_id": "F-A",
                "unidade_gestora_id": "O-A",
                "descricao": "Teste",
                "data_empenho": "2025-01-01",
            }
        ],
        "fornecedores": [
            {"id": "F-A", "nome": "Fornecedor A"}
        ],
        "orgaos": [
            {"id": "O-A", "nome": "Órgão A"}
        ],
    }
    summary = service.get_graph_summary(payloads=custom_payload)
    assert summary["empenhos"] == 1


def test_live_fetch_uses_cache(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "true")
    monkeypatch.setenv("GRAPH_CACHE_TTL_SECONDS", "3600")
    reset_settings_cache()

    calls = {"count": 0}

    def fake_fetch(self):
        calls["count"] += 1
        return SAMPLE_PAYLOAD

    monkeypatch.setattr(GraphRepository, "fetch_payloads", fake_fetch, raising=False)

    service = GraphService()
    service.get_graph_summary()
    service.get_graph_summary()

    assert calls["count"] == 1

    reset_settings_cache()


def test_live_fetch_fallback_to_sample(monkeypatch):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "true")
    monkeypatch.setenv("GRAPH_CACHE_TTL_SECONDS", "0")
    reset_settings_cache()

    def boom(self):
        raise RuntimeError("erro de rede")

    monkeypatch.setattr(GraphRepository, "fetch_payloads", boom, raising=False)

    service = GraphService()
    summary = service.get_graph_summary()

    assert summary["empenhos"] == len(SAMPLE_PAYLOAD["empenhos"])

    reset_settings_cache()
