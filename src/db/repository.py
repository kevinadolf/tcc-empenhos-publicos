"""High level interface to assemble the heterogeneous spending graph."""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, Optional, Sequence

import pandas as pd

from pydantic import ValidationError, parse_obj_as

from src.common.settings import Settings, get_settings
from src.db.dataframes import (
    build_empenhos_df,
    build_fornecedores_df,
    build_orgaos_df,
)
from src.db.graph_builder import build_heterogeneous_graph
from src.db.sources.tce_client import TCEClientConfig, TCEDataClient
from src.db.schemas import EmpenhoPayload, FornecedorPayload, OrgaoPayload

logger = logging.getLogger(__name__)

ProgressReporter = Callable[[float, str], None]


@dataclass
class GraphData:
    empenhos: pd.DataFrame
    fornecedores: pd.DataFrame
    orgaos: pd.DataFrame


class GraphRepository:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[TCEDataClient] = None,
    ) -> None:
        self.settings = settings or get_settings()
        client_config = TCEClientConfig(
            base_url=self.settings.tce_base_url,
            page_size=self.settings.api_page_size,
            max_pages=self.settings.api_max_pages,
        )
        self.client = client or TCEDataClient(config=client_config)

    def fetch_payloads(
        self,
        *,
        progress_callback: Optional[ProgressReporter] = None,
    ) -> Dict[str, Sequence[Dict]]:
        if not self.settings.enable_live_fetch:
            raise RuntimeError(
                "Live fetch is disabled. Set ENABLE_LIVE_FETCH=true to allow network calls.",
            )

        def report(progress: float, message: str) -> None:
            if progress_callback:
                progress_callback(progress, message)

        def scale_callback(start: float, end: float) -> ProgressReporter:
            span = max(end - start, 0.0)

            def inner(progress: float, message: str) -> None:
                scaled = start + (max(0.0, min(100.0, progress)) / 100.0) * span
                report(scaled, message)

            return inner

        report(0.0, "Iniciando coleta da API TCE-RJ")

        if hasattr(self.client, "fetch_empenho_estado"):
            report(2.0, "Coletando empenhos estaduais")
            records = self._fetch_empenho_estado_records(
                progress_callback=scale_callback(2.0, 75.0),
            )
            report(75.0, f"{len(records)} empenhos coletados")
            limit = self.settings.tce_max_records or None
            payload = self._assemble_payloads_from_empenho_estado(records, limit=limit)
            report(100.0, "Coleta finalizada")
            return payload

        report(5.0, "Coletando empenhos")
        empenhos = self.client.fetch_empenhos()
        report(35.0, f"{len(empenhos)} empenhos coletados")
        report(40.0, "Coletando fornecedores")
        fornecedores = self.client.fetch_fornecedores()
        report(60.0, f"{len(fornecedores)} fornecedores coletados")
        report(65.0, "Coletando órgãos")
        orgaos = self.client.fetch_unidades_gestoras()
        report(75.0, f"{len(orgaos)} órgãos coletados")
        payloads = {
            "empenhos": empenhos,
            "fornecedores": fornecedores,
            "orgaos": orgaos,
        }
        report(100.0, "Coleta finalizada")
        return payloads

    @staticmethod
    def build_graph_from_payloads(payloads: Dict[str, Sequence[Dict]]) -> GraphData:
        empenhos_df = build_empenhos_df(payloads.get("empenhos", []))
        fornecedores_df = build_fornecedores_df(payloads.get("fornecedores", []))
        orgaos_df = build_orgaos_df(payloads.get("orgaos", []))
        return GraphData(empenhos_df, fornecedores_df, orgaos_df)

    def load_graph(self, payloads: Optional[Dict[str, Sequence[Dict]]] = None):
        if payloads is None:
            payloads = self.fetch_payloads()

        validated = self._validate_payloads(payloads)
        graph_data = self.build_graph_from_payloads(validated)
        graph = build_heterogeneous_graph(
            graph_data.empenhos,
            fornecedores_df=graph_data.fornecedores,
            orgaos_df=graph_data.orgaos,
            include_contratos=True,
        )
        return graph, graph_data

    def _fetch_empenho_estado_records(
        self,
        *,
        progress_callback: Optional[ProgressReporter] = None,
    ) -> Sequence[Dict]:
        years = self.settings.tce_years or ()
        records: list[Dict] = []
        max_records = self.settings.tce_max_records or None
        total_hint: int
        if max_records and max_records > 0:
            total_hint = max_records
        else:
            max_pages = self.settings.api_max_pages or 10
            total_hint = max(self.settings.api_page_size * max_pages, 1)

        last_reported_progress = -5.0

        def emit_progress(message: str) -> None:
            nonlocal last_reported_progress
            if not progress_callback:
                return
            ratio = min(len(records) / total_hint, 1.0)
            progress = min(100.0, ratio * 100.0)
            if progress - last_reported_progress >= 1.0 or progress >= 100.0:
                last_reported_progress = progress
                progress_callback(progress, message)

        if progress_callback:
            progress_callback(0.0, "Iniciando coleta de empenhos estaduais")

        def extend_with_limit(batch: Sequence[Dict]) -> bool:
            if max_records is None:
                records.extend(batch)
                emit_progress(f"Carregando empenhos (total parcial: {len(records)})")
                return True
            remaining = max_records - len(records)
            if remaining <= 0:
                return False
            records.extend(list(batch)[:remaining])
            emit_progress(f"Carregando empenhos (total parcial: {len(records)})")
            return len(records) < max_records

        if not years:
            batch = self.client.fetch_empenho_estado()
            extend_with_limit(batch)
            emit_progress("Registros de empenhos carregados")
            if progress_callback:
                progress_callback(100.0, "Empenhos estaduais coletados")
            return records

        for year in years:
            fetched = self.client.fetch_empenho_estado(ano=str(year))
            should_continue = extend_with_limit(fetched)
            if not should_continue:
                break
        emit_progress("Registros de empenhos carregados")
        if progress_callback:
            progress_callback(100.0, "Empenhos estaduais coletados")
        return records

    @staticmethod
    def _slugify(value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        normalized = normalized.lower()
        normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
        return normalized.strip("-")

    @staticmethod
    def _stable_id(prefix: str, *parts: str) -> str:
        payload = "::".join(part for part in parts if part)
        if not payload:
            return f"{prefix}-desconhecido"
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
        slug = GraphRepository._slugify(payload)
        base = slug if slug else "registro"
        return f"{prefix}-{base}-{digest}"

    def _validate_payloads(self, payloads: Dict[str, Sequence[Dict]]) -> Dict[str, Sequence[Dict]]:
        try:
            empenhos = parse_obj_as(list[EmpenhoPayload], payloads.get("empenhos", []))
            fornecedores = parse_obj_as(list[FornecedorPayload], payloads.get("fornecedores", []))
            orgaos = parse_obj_as(list[OrgaoPayload], payloads.get("orgaos", []))
        except ValidationError as exc:
            logger.error("Falha na validação dos payloads do TCE: %s", exc)
            raise

        return {
            "empenhos": [item.model_dump() for item in empenhos],
            "fornecedores": [item.model_dump() for item in fornecedores],
            "orgaos": [item.model_dump() for item in orgaos],
        }

    @staticmethod
    def _parse_float(value) -> float:
        if value in (None, ""):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return 0.0
            if "," in raw and "." in raw:
                raw = raw.replace(".", "").replace(",", ".")
            elif "," in raw:
                raw = raw.replace(",", ".")
            return float(raw)
        raise TypeError(f"Unsupported numeric value: {value!r}")

    @classmethod
    def _assemble_payloads_from_empenho_estado(
        cls,
        records: Sequence[Dict],
        *,
        limit: Optional[int] = None,
    ) -> Dict[str, Sequence[Dict]]:
        empenhos: list[Dict] = []
        fornecedores: Dict[str, Dict] = {}
        orgaos: Dict[str, Dict] = {}

        limit_value = limit if limit and limit > 0 else None

        for record in records:
            if limit_value and len(empenhos) >= limit_value:
                break
            unidade = str(record.get("Unidade") or "").strip()
            ano = record.get("Ano")
            mes = record.get("Mes")
            numero_empenho = record.get("NumeroEmpenho")
            cpf_cnpj = str(record.get("CPFCNPJ") or "").strip()
            funcao = record.get("Funcao")

            fornecedor_id = cls._stable_id("fornecedor", cpf_cnpj, record.get("TipoPessoa", ""))
            orgao_id = cls._stable_id("orgao", unidade)

            try:
                mes_int = int(str(mes))
                data_empenho = date(int(ano), mes_int, 1).isoformat()
            except (TypeError, ValueError):
                data_empenho = None

            valor = cls._parse_float(
                record.get("Empenho") or record.get("Empenhado"),
            )

            empenho_id = cls._stable_id(
                "empenho",
                str(ano),
                str(mes),
                unidade,
                str(numero_empenho),
                cpf_cnpj,
            )

            empenhos.append(
                {
                    "id": empenho_id,
                    "numero": str(numero_empenho),
                    "descricao": f"Função: {funcao}" if funcao else None,
                    "valor_empenhado": valor,
                    "data_empenho": data_empenho,
                    "fornecedor_id": fornecedor_id,
                    "unidade_gestora_id": orgao_id,
                    "contrato_id": None,
                },
            )

            if fornecedor_id not in fornecedores:
                fornecedores[fornecedor_id] = {
                    "id": fornecedor_id,
                    "nome": cpf_cnpj or "Fornecedor não informado",
                    "documento": cpf_cnpj or None,
                    "tipo_documento": record.get("TipoPessoa"),
                    "municipio": None,
                    "uf": None,
                }

            if orgao_id not in orgaos:
                orgaos[orgao_id] = {
                    "id": orgao_id,
                    "nome": unidade or "Unidade não informada",
                    "sigla": None,
                    "municipio": None,
                    "uf": None,
                }

        payload = {
            "empenhos": empenhos,
            "fornecedores": list(fornecedores.values()),
            "orgaos": list(orgaos.values()),
        }
        if limit_value:
            payload["empenhos"] = payload["empenhos"][:limit_value]
        return payload
