import json

import pytest

from src.backend.app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        yield client


def test_health_endpoint(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"


def test_graph_summary_endpoint(client):
    response = client.get("/api/graph/summary")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodes"] > 0
    assert payload["empenhos"] == 2


def test_anomalies_endpoint(client):
    response = client.get("/api/anomalies")
    assert response.status_code == 200
    data = response.get_json()
    assert "centralidade_fornecedores" in data
    assert isinstance(data["centralidade_fornecedores"], list)


def test_graph_snapshot_endpoint(client):
    response = client.get("/api/graph/snapshot")
    assert response.status_code == 200
    data = response.get_json()
    assert "nodes" in data
    assert "links" in data
    assert len(data["nodes"]) > 0
    assert all("x" in node and "y" in node for node in data["nodes"])


def test_graph_nodes_endpoint(client):
    response = client.get("/api/graph/nodes")
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    first = data[0]
    assert {"id", "label", "type"}.issubset(first.keys())
