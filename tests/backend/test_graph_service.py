from src.backend.services.graph_service import GraphService
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.settings import get_settings


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
