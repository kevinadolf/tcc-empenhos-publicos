"""Optional CNPJ enrichment using local Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


class CNPJEnricher:
    def __init__(self, empresas: pd.DataFrame, estabelecimentos: pd.DataFrame) -> None:
        self.empresas = empresas
        self.estabelecimentos = estabelecimentos

    @classmethod
    def from_parquet(cls, base_dir: Path | None = None) -> "CNPJEnricher":
        base = base_dir or Path("data/cnpj/parquet")
        empresas_path = base / "empresas.parquet"
        est_path = base / "estabelecimentos.parquet"
        if not empresas_path.exists() or not est_path.exists():
            # retorna enricher vazio (no-op)
            return cls(pd.DataFrame(), pd.DataFrame())

        empresas = pd.read_parquet(empresas_path)
        estabelecimentos = pd.read_parquet(est_path)
        # normalizar colunas
        for df in (empresas, estabelecimentos):
            df.columns = [col.lower() for col in df.columns]
        return cls(empresas, estabelecimentos)

    def enrich_fornecedores(self, fornecedores: Iterable[Dict]) -> List[Dict]:
        if self.empresas.empty or self.estabelecimentos.empty:
            return list(fornecedores)

        fornecedores_df = pd.DataFrame(list(fornecedores))
        if fornecedores_df.empty or "documento" not in fornecedores_df.columns:
            return list(fornecedores)

        fornecedores_df["cnpj"] = fornecedores_df["documento"].astype(str).str.replace(r"\\D", "", regex=True)
        empresas = self.empresas.copy()
        estabelecimentos = self.estabelecimentos.copy()
        empresas["cnpj"] = empresas.get("cnpj", "").astype(str)
        estabelecimentos["cnpj"] = estabelecimentos.get("cnpj", "").astype(str)

        enriched = (
            fornecedores_df.merge(empresas[["cnpj", "razao_social", "capital_social"]], on="cnpj", how="left")
            .merge(
                estabelecimentos[["cnpj", "nome_fantasia", "cnae_principal", "data_inicio_atividade"]],
                on="cnpj",
                how="left",
            )
        )

        records: List[Dict] = []
        for _, row in enriched.iterrows():
            record = row.to_dict()
            # Priorizar razao_social/nome_fantasia como nome amigável
            nome_pref = record.get("razao_social") or record.get("nome_fantasia")
            if nome_pref and not record.get("nome"):
                record["nome"] = nome_pref
            records.append(record)
        return records
