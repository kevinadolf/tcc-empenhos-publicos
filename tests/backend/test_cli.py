import json
from pathlib import Path

import pytest

from src.backend import cli
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.common.settings import get_settings


def reset_settings_cache():
    get_settings.cache_clear()


def test_cli_summary(monkeypatch, capsys):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    monkeypatch.setenv("GRAPH_DATA_SOURCE", "sample")
    reset_settings_cache()

    exit_code = cli.main(["summary"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["empenhos"] == len(SAMPLE_PAYLOAD["empenhos"])
    reset_settings_cache()


def test_cli_anomalies_with_limit(monkeypatch, capsys):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    monkeypatch.setenv("GRAPH_DATA_SOURCE", "sample")
    reset_settings_cache()

    exit_code = cli.main(["anomalies", "--limit", "1"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert all(len(items) <= 1 for items in data.values() if isinstance(items, list))
    reset_settings_cache()


def test_cli_export_to_files(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    monkeypatch.setenv("GRAPH_DATA_SOURCE", "sample")
    reset_settings_cache()

    summary_path = tmp_path / "summary.json"
    anomalies_path = tmp_path / "anomalies.json"

    exit_code = cli.main(
        [
            "--pretty",
            "export",
            "--summary-path",
            str(summary_path),
            "--anomalies-path",
            str(anomalies_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Resumo salvo" in captured.out
    assert summary_path.exists()
    assert anomalies_path.exists()

    summary_data = json.loads(summary_path.read_text(encoding="utf-8"))
    anomalies_data = json.loads(anomalies_path.read_text(encoding="utf-8"))

    assert summary_data["fornecedores"] == len(SAMPLE_PAYLOAD["fornecedores"])
    assert "centralidade_fornecedores" in anomalies_data
    reset_settings_cache()


def test_cli_payload_from_file(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    monkeypatch.setenv("GRAPH_DATA_SOURCE", "sample")
    reset_settings_cache()

    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(SAMPLE_PAYLOAD), encoding="utf-8")

    exit_code = cli.main(["--payload", str(payload_path), "--pretty", "summary"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["empenhos"] == len(SAMPLE_PAYLOAD["empenhos"])
    reset_settings_cache()


def test_cli_snapshot(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv("ENABLE_LIVE_FETCH", "false")
    monkeypatch.setenv("GRAPH_DATA_SOURCE", "sample")
    reset_settings_cache()

    graphml_path = tmp_path / "snapshot.graphml"
    json_path = tmp_path / "snapshot.json"

    exit_code = cli.main(
        [
            "snapshot",
            "--graphml",
            str(graphml_path),
            "--json",
            str(json_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert graphml_path.exists()
    assert json_path.exists()
    assert "Snapshot GraphML" in captured.out
    reset_settings_cache()
