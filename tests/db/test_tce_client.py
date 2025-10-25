import pytest

from src.db.sources.tce_client import TCEClientConfig, TCEDataClient


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if not self.responses:
            return DummyResponse({}, status_code=404)
        return DummyResponse(self.responses.pop(0))


def test_fetch_collection_paginates():
    responses = [
        {"data": [{"id": 1}]},
        {"data": []},
    ]
    session = DummySession(responses)
    client = TCEDataClient(config=TCEClientConfig(max_pages=None), session=session)
    data = client.fetch_collection("empenhos")
    assert len(data) == 1
    assert len(session.calls) == 2


def test_fetch_collection_respects_max_pages():
    responses = [
        {"data": [{"id": 1}]},
        {"data": [{"id": 2}]},
    ]
    session = DummySession(responses)
    client = TCEDataClient(config=TCEClientConfig(max_pages=1), session=session)
    data = client.fetch_collection("empenhos")
    assert len(data) == 1


def test_fetch_collection_malformed_payload():
    session = DummySession([{"unexpected": []}])
    client = TCEDataClient(session=session)
    with pytest.raises(ValueError):
        client.fetch_collection("empenhos")
