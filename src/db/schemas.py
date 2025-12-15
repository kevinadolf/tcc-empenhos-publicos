"""Pydantic models to validate external payloads before graph construction."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EmpenhoPayload(BaseModel):
    id: str
    numero: Optional[str] = None
    descricao: Optional[str] = None
    valor_empenhado: float = Field(ge=0)
    data_empenho: Optional[str] = None
    fornecedor_id: Optional[str] = None
    unidade_gestora_id: Optional[str] = None
    contrato_id: Optional[str] = None

    @field_validator("valor_empenhado", mode="before")
    @classmethod
    def parse_valor(cls, value):
        if value is None:
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
        raise TypeError("valor_empenhado inválido")

    model_config = {"extra": "allow"}


class FornecedorPayload(BaseModel):
    id: str
    nome: Optional[str] = None
    documento: Optional[str] = None
    tipo_documento: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None

    model_config = {"extra": "allow"}


class OrgaoPayload(BaseModel):
    id: str
    nome: Optional[str] = None
    sigla: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None

    model_config = {"extra": "allow"}
