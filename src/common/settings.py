"""Application-wide settings helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    tce_base_url: str = "https://dados.tcerj.tc.br/api/v1"
    api_page_size: int = 100
    api_max_pages: Optional[int] = None
    enable_live_fetch: bool = False  # evita chamadas externas durante desenvolvimento/testes
    tce_years: Tuple[int, ...] = ()
    tce_max_records: Optional[int] = None
    graph_cache_ttl_seconds: int = 300


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    max_pages = os.getenv("TCE_API_MAX_PAGES")
    years_raw = os.getenv("TCE_API_YEARS", "")
    max_records = os.getenv("TCE_API_MAX_RECORDS")
    cache_ttl_raw = os.getenv("GRAPH_CACHE_TTL_SECONDS")

    years: Tuple[int, ...] = tuple(
        int(value.strip())
        for value in years_raw.split(",")
        if value.strip().isdigit()
    )

    graph_cache_ttl_seconds = Settings.graph_cache_ttl_seconds
    if cache_ttl_raw is not None and cache_ttl_raw.strip():
        try:
            graph_cache_ttl_seconds = max(int(cache_ttl_raw), 0)
        except ValueError:
            graph_cache_ttl_seconds = Settings.graph_cache_ttl_seconds

    max_records_value: Optional[int]
    if max_records and max_records.strip():
        try:
            max_records_value = max(int(max_records), 0)
        except ValueError:
            max_records_value = None
    else:
        max_records_value = None

    return Settings(
        tce_base_url=os.getenv("TCE_API_BASE_URL", Settings.tce_base_url),
        api_page_size=int(os.getenv("TCE_API_PAGE_SIZE", Settings.api_page_size)),
        api_max_pages=int(max_pages) if max_pages else None,
        enable_live_fetch=os.getenv("ENABLE_LIVE_FETCH", "false").lower() == "true",
        tce_years=years,
        tce_max_records=max_records_value,
        graph_cache_ttl_seconds=graph_cache_ttl_seconds,
    )
