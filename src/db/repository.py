"""High level interface to assemble the heterogeneous spending graph."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Sequence

import pandas as pd

from src.common.settings import Settings, get_settings
from src.db.dataframes import (
    build_empenhos_df,
    build_fornecedores_df,
    build_orgaos_df,
)
from src.db.graph_builder import build_heterogeneous_graph
from src.db.sources.tce_client import TCEClientConfig, TCEDataClient


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

    def fetch_payloads(self) -> Dict[str, Sequence[Dict]]:
        if not self.settings.enable_live_fetch:
            raise RuntimeError(
                "Live fetch is disabled. Set ENABLE_LIVE_FETCH=true to allow network calls.",
            )

        if hasattr(self.client, "fetch_empenho_estado"):
            records = self._fetch_empenho_estado_records()
            limit = self.settings.tce_max_records or None
            return self._assemble_payloads_from_empenho_estado(records, limit=limit)

        empenhos = self.client.fetch_empenhos()
        fornecedores = self.client.fetch_fornecedores()
        orgaos = self.client.fetch_unidades_gestoras()
        return {
            "empenhos": empenhos,
            "fornecedores": fornecedores,
            "orgaos": orgaos,
        }

    @staticmethod
    def build_graph_from_payloads(payloads: Dict[str, Sequence[Dict]]) -> GraphData:
        empenhos_df = build_empenhos_df(payloads.get("empenhos", []))
        fornecedores_df = build_fornecedores_df(payloads.get("fornecedores", []))
        orgaos_df = build_orgaos_df(payloads.get("orgaos", []))
        return GraphData(empenhos_df, fornecedores_df, orgaos_df)

    def load_graph(self, payloads: Optional[Dict[str, Sequence[Dict]]] = None):
        if payloads is None:
            payloads = self.fetch_payloads()

        graph_data = self.build_graph_from_payloads(payloads)
        graph = build_heterogeneous_graph(
            graph_data.empenhos,
            fornecedores_df=graph_data.fornecedores,
            orgaos_df=graph_data.orgaos,
            include_contratos=True,
        )
        return graph, graph_data

    def _fetch_empenho_estado_records(self) -> Sequence[Dict]:
        years = self.settings.tce_years or ()
        records: list[Dict] = []
        max_records = self.settings.tce_max_records or None

        def extend_with_limit(batch: Sequence[Dict]) -> bool:
            if max_records is None:
                records.extend(batch)
                return True
            remaining = max_records - len(records)
            if remaining <= 0:
                return False
            records.extend(list(batch)[:remaining])
            return len(records) < max_records

        if not years:
            batch = self.client.fetch_empenho_estado()
            extend_with_limit(batch)
            return records

        for year in years:
            fetched = self.client.fetch_empenho_estado(ano=str(year))
            should_continue = extend_with_limit(fetched)
            if not should_continue:
                break
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
