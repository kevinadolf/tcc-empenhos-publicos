"""Command-line interface for running graph analyses."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.backend.services.graph_service import DATA_SOURCES, GraphService
from src.common.graph_serialization import export_graph_snapshot


def load_payload_from_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Payload JSON must contain an object with data collections")
    return data


def _limit_anomalies(anomalies: Dict[str, Any], limit: Optional[int]) -> Dict[str, Any]:
    if limit is None:
        return anomalies
    return {
        key: values[:limit] if isinstance(values, list) else values
        for key, values in anomalies.items()
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ferramentas para análise do grafo de empenhos")
    parser.add_argument(
        "--payload",
        type=Path,
        help="Arquivo JSON com payloads já coletados (opcional)",
    )
    parser.add_argument(
        "--source",
        choices=sorted(DATA_SOURCES),
        help="Origem dos dados do grafo (padrão configurado pelo backend)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Imprime JSON formatado",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("summary", help="Mostra estatísticas básicas do grafo")

    anomalies_parser = subparsers.add_parser("anomalies", help="Lista anomalias detectadas")
    anomalies_parser.add_argument(
        "--limit",
        type=int,
        help="Limita a quantidade de anomalias por categoria",
    )

    export_parser = subparsers.add_parser("export", help="Exporta resumo e anomalias para arquivos")
    export_parser.add_argument(
        "--summary-path",
        type=Path,
        default=Path("graph_summary.json"),
        help="Arquivo de saída para o resumo (padrão: graph_summary.json)",
    )
    export_parser.add_argument(
        "--anomalies-path",
        type=Path,
        default=Path("anomalies.json"),
        help="Arquivo de saída para anomalias (padrão: anomalies.json)",
    )

    snapshot_parser = subparsers.add_parser("snapshot", help="Exporta estrutura do grafo para análise offline")
    snapshot_parser.add_argument(
        "--graphml",
        type=Path,
        default=Path("graph.graphml"),
        help="Arquivo GraphML de saída (padrão: graph.graphml)",
    )
    snapshot_parser.add_argument(
        "--json",
        type=Path,
        default=Path("graph.json"),
        help="Arquivo JSON (node-link) de saída (padrão: graph.json)",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    payloads = load_payload_from_file(args.payload) if args.payload else None
    service = GraphService()
    source = args.source

    if args.command == "summary":
        summary = service.get_graph_summary(payloads=payloads, source=source)
        output = json.dumps(summary, indent=2 if args.pretty else None, ensure_ascii=False)
        print(output)
        return 0

    if args.command == "anomalies":
        anomalies = service.get_anomalies(payloads=payloads, source=source)
        anomalies = _limit_anomalies(anomalies, args.limit)
        output = json.dumps(anomalies, indent=2 if args.pretty else None, ensure_ascii=False)
        print(output)
        return 0

    if args.command == "export":
        summary = service.get_graph_summary(payloads=payloads, source=source)
        anomalies = service.get_anomalies(payloads=payloads, source=source)
        args.summary_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        args.anomalies_path.write_text(
            json.dumps(anomalies, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Resumo salvo em {args.summary_path}")
        print(f"Anomalias salvas em {args.anomalies_path}")
        return 0

    if args.command == "snapshot":
        graph, _ = service.load_graph(payloads=payloads, source=source)
        export_graph_snapshot(graph, args.graphml, args.json)
        print(f"Snapshot GraphML salvo em {args.graphml}")
        print(f"Snapshot JSON salvo em {args.json}")
        return 0

    parser.error("Comando inválido")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
