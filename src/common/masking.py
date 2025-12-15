"""Helpers to mask sensitive fields before sending to the frontend."""

from __future__ import annotations

import re


def mask_document(documento: str) -> str:
    if not documento:
        return documento
    digits = re.sub(r"\\D", "", documento)
    if len(digits) == 11:  # CPF
        return f"***.{digits[3:6]}.***-{digits[-2:]}"
    if len(digits) == 14:  # CNPJ
        return f"**.{digits[2:5]}.***/*{digits[8:12]}-{digits[-2:]}"
    if len(digits) >= 4:
        return f"{'*' * (len(digits) - 4)}{digits[-4:]}"
    return "***"
