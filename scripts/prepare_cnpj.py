"""
Converter zips de CNPJ (Empresas, Estabelecimentos, Sócios) para Parquet.
Entrada: data/cnpj/raw/Empresas*.zip, Estabelecimentos*.zip, Socios*.zip
Saída: data/cnpj/parquet/empresas.parquet, estabelecimentos.parquet, socios.parquet
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd

RAW_DIR = Path("data/cnpj/raw")
OUT_DIR = Path("data/cnpj/parquet")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _collect_files(pattern: str) -> Iterable[Path]:
    return sorted(RAW_DIR.glob(pattern))


def _read_zip_csv(zip_path: Path, encoding: str = "latin-1", sep: str = ";") -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if not names:
            return pd.DataFrame()
        with zf.open(names[0]) as fh:
            return pd.read_csv(fh, sep=sep, encoding=encoding, dtype=str)


def convert_empresas():
    frames = []
    for path in _collect_files("Empresas*.zip"):
        df = _read_zip_csv(path)
        if df.empty:
            continue
        df.columns = [col.lower() for col in df.columns]
        frames.append(df)
    if not frames:
        return
    full = pd.concat(frames, ignore_index=True)
    # renomear colunas principais se existirem
    col_map = {
        "cnpj": "cnpj",
        "razaosocial": "razao_social",
        "razao_social": "razao_social",
        "capital_social": "capital_social",
        "capital_social_da_empresa": "capital_social",
        "situacao_cadastral": "situacao_cadastral",
        "data_situacao_cadastral": "data_situacao_cadastral",
    }
    full = full.rename(columns=col_map)
    full.to_parquet(OUT_DIR / "empresas.parquet", index=False)


def convert_estabelecimentos():
    frames = []
    for path in _collect_files("Estabelecimentos*.zip"):
        df = _read_zip_csv(path)
        if df.empty:
            continue
        df.columns = [col.lower() for col in df.columns]
        frames.append(df)
    if not frames:
        return
    full = pd.concat(frames, ignore_index=True)
    col_map = {
        "cnpj": "cnpj",
        "cnpj_basico": "cnpj",
        "cnpj_ordem": "cnpj_ordem",
        "cnpj_dv": "cnpj_dv",
        "nome_fantasia": "nome_fantasia",
        "cnae_fiscal": "cnae_principal",
        "cnae_fiscal_secundaria": "cnaes_secundarios",
        "data_inicio_atividade": "data_inicio_atividade",
        "situacao_cadastral": "situacao_cadastral",
    }
    full = full.rename(columns=col_map)
    full.to_parquet(OUT_DIR / "estabelecimentos.parquet", index=False)


def convert_socios():
    frames = []
    for path in _collect_files("Socios*.zip"):
        df = _read_zip_csv(path)
        if df.empty:
            continue
        df.columns = [col.lower() for col in df.columns]
        frames.append(df)
    if not frames:
        return
    full = pd.concat(frames, ignore_index=True)
    col_map = {
        "cnpj": "cnpj",
        "cnpj_basico": "cnpj",
        "nome_socio": "nome_socio",
        "qualificacao_socio": "qualificacao_socio",
        "pais": "pais",
    }
    full = full.rename(columns=col_map)
    full.to_parquet(OUT_DIR / "socios.parquet", index=False)


def main():
    convert_empresas()
    convert_estabelecimentos()
    convert_socios()
    print("Conversão concluída. Arquivos em data/cnpj/parquet/")


if __name__ == "__main__":
    main()
