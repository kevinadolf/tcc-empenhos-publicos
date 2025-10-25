"""High level interface to assemble the heterogeneous spending graph."""

from __future__ import annotations

from dataclasses import dataclass
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
