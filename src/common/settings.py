"""Application-wide settings helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    tce_base_url: str = "https://dados.tcerj.tc.br/api/v1"
    api_page_size: int = 100
    api_max_pages: Optional[int] = None
    enable_live_fetch: bool = False  # evita chamadas externas durante desenvolvimento/testes


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    max_pages = os.getenv("TCE_API_MAX_PAGES")
    return Settings(
        tce_base_url=os.getenv("TCE_API_BASE_URL", Settings.tce_base_url),
        api_page_size=int(os.getenv("TCE_API_PAGE_SIZE", Settings.api_page_size)),
        api_max_pages=int(max_pages) if max_pages else None,
        enable_live_fetch=os.getenv("ENABLE_LIVE_FETCH", "false").lower() == "true",
    )
