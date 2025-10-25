"""Client for interacting with the TCE-RJ open data API."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class TCEClientConfig:
    """Holds configuration for the TCE data client."""

    base_url: str = "https://dados.tcerj.tc.br/api/v1"
    timeout: int = 30
    page_size: int = 100
    max_pages: Optional[int] = None  # limit for development/testing


class TCEDataClient:
    """Thin wrapper around the TCE-RJ API with resilient pagination."""

    def __init__(
        self,
        config: Optional[TCEClientConfig] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config or TCEClientConfig()
        self.session = session or requests.Session()

    def _get(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict:
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=self.config.timeout)
        response.raise_for_status()
        return response.json()

    def fetch_collection(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None,
        page_param: str = "page",
        page_size_param: str = "pageSize",
        data_key: str = "data",
    ) -> List[Dict]:
        """Retrieve a potentially paginated collection from the API."""

        records: List[Dict] = []
        page = 1
        params = dict(params or {})
        params[page_size_param] = params.get(page_size_param, self.config.page_size)

        while True:
            params[page_param] = page
            payload = self._get(endpoint, params=params)
            if data_key not in payload:
                raise ValueError(
                    f"Payload missing '{data_key}' key for endpoint '{endpoint}'",
                )

            batch = payload.get(data_key, [])

            if not isinstance(batch, Iterable):
                raise ValueError(f"Unexpected payload format for endpoint '{endpoint}'")

            records.extend(batch)

            logger.debug(
                "Fetched %s records from %s (page %s)",
                len(batch),
                endpoint,
                page,
            )

            if not batch:
                break

            if self.config.max_pages and page >= self.config.max_pages:
                break

            page += 1

        return records

    def fetch_empenhos(self, **params: str) -> List[Dict]:
        return self.fetch_collection("empenhos", params=params)

    def fetch_fornecedores(self, **params: str) -> List[Dict]:
        return self.fetch_collection("fornecedores", params=params)

    def fetch_unidades_gestoras(self, **params: str) -> List[Dict]:
        return self.fetch_collection("unidades-gestoras", params=params)
