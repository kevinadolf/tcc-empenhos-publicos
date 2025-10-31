"""Utilities to generate synthetic graph payloads for quick exploration."""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Dict, List, Optional


ORGAO_NAMES = [
    ("Secretaria de Educação", "SEDUC"),
    ("Secretaria de Saúde", "SESAU"),
    ("Secretaria de Fazenda", "SEFAZ"),
    ("Secretaria de Cultura", "SECULT"),
    ("Secretaria de Segurança", "SESSEG"),
    ("Secretaria de Esporte", "SEESP"),
]

FORNECEDOR_BASES = [
    "TechNova",
    "Inova Serviços",
    "Alpha Consultoria",
    "Beta Engenharia",
    "Delta TI",
    "Omega Logística",
    "Vertex Digital",
    "Aurora Saúde",
    "Atlas Construções",
    "Horizonte Eventos",
]

DESCRICOES = [
    "Aquisição de materiais de escritório",
    "Serviços de manutenção predial",
    "Contratação de consultoria especializada",
    "Fornecimento de equipamentos de informática",
    "Organização de evento institucional",
    "Campanha de comunicação social",
    "Serviços de limpeza e conservação",
    "Treinamento e capacitação de servidores",
]


def _random_cnpj(rng: random.Random) -> str:
    digits = [rng.randint(0, 9) for _ in range(14)]
    return "{0:02d}.{1:03d}.{2:03d}/{3:04d}-{4:02d}".format(
        digits[0],
        digits[1] * 100 + digits[2] * 10 + digits[3],
        digits[4] * 100 + digits[5] * 10 + digits[6],
        digits[7] * 1000 + digits[8] * 100 + digits[9] * 10 + digits[10],
        digits[11] * 10 + digits[12],
    )


def _random_date(rng: random.Random, *, days_back: int = 365) -> str:
    today = date.today()
    offset = rng.randint(0, max(days_back, 1))
    return (today - timedelta(days=offset)).isoformat()


def _build_orgaos(rng: random.Random, total: int) -> List[Dict]:
    choices = ORGAO_NAMES[:]
    rng.shuffle(choices)
    orgaos = []
    for index in range(total):
        nome, sigla = choices[index % len(choices)]
        orgaos.append(
            {
                "id": f"O{index + 1:03d}",
                "nome": nome,
                "sigla": sigla,
                "municipio": "Rio de Janeiro",
                "uf": "RJ",
            },
        )
    return orgaos


def _build_fornecedores(rng: random.Random, total: int) -> List[Dict]:
    fornecedores = []
    bases = FORNECEDOR_BASES[:]
    rng.shuffle(bases)
    for index in range(total):
        base = bases[index % len(bases)]
        fornecedores.append(
            {
                "id": f"F{index + 1:04d}",
                "nome": f"{base} LTDA",
                "documento": _random_cnpj(rng),
                "tipo_documento": "CNPJ",
                "municipio": rng.choice(["Rio de Janeiro", "Niterói", "Campos", "Volta Redonda"]),
                "uf": "RJ",
            },
        )
    return fornecedores


def _build_empenhos(
    rng: random.Random,
    total: int,
    fornecedores: List[Dict],
    orgaos: List[Dict],
    include_contratos: bool,
) -> List[Dict]:
    empenhos = []
    for index in range(total):
        fornecedor = rng.choice(fornecedores)
        orgao = rng.choice(orgaos)
        valor = round(rng.uniform(5_000, 250_000), 2)
        descricao = rng.choice(DESCRICOES)
        empenhos.append(
            {
                "id": f"E{index + 1:05d}",
                "numero": f"{date.today().year}-{index + 1:04d}",
                "descricao": descricao,
                "valor_empenhado": float(valor),
                "data_empenho": _random_date(rng, days_back=540),
                "fornecedor_id": fornecedor["id"],
                "unidade_gestora_id": orgao["id"],
                "contrato_id": f"C{index + 1:05d}" if include_contratos and rng.random() < 0.7 else None,
            },
        )
    return empenhos


def generate_random_payloads(
    *,
    num_orgaos: int = 6,
    num_fornecedores: int = 10,
    num_empenhos: int = 40,
    include_contratos: bool = True,
    seed: Optional[int] = None,
) -> Dict[str, List[Dict]]:
    """Create a synthetic dataset compatible with the graph builder."""

    rng = random.Random(seed)
    orgaos = _build_orgaos(rng, max(1, num_orgaos))
    fornecedores = _build_fornecedores(rng, max(1, num_fornecedores))
    empenhos = _build_empenhos(
        rng,
        max(1, num_empenhos),
        fornecedores,
        orgaos,
        include_contratos=include_contratos,
    )

    _inject_anomaly_patterns(
        rng=rng,
        orgaos=orgaos,
        fornecedores=fornecedores,
        empenhos=empenhos,
        include_contratos=include_contratos,
    )

    return {
        "orgaos": orgaos,
        "fornecedores": fornecedores,
        "empenhos": empenhos,
    }


def _inject_anomaly_patterns(
    *,
    rng: random.Random,
    orgaos: List[Dict],
    fornecedores: List[Dict],
    empenhos: List[Dict],
    include_contratos: bool,
) -> None:
    """Ensure synthetic graphs always have sinais fortes de anomalia."""

    if not orgaos or not fornecedores:
        return

    def _next_fornecedor_id() -> str:
        return f"F{len(fornecedores) + 1:04d}"

    def _next_orgao_id() -> str:
        return f"O{len(orgaos) + 1:03d}"

    def _next_empenho_id() -> str:
        return f"E{len(empenhos) + 1:05d}"

    current_year = date.today().year

    # 1. Fornecedor extremamente central conectando-se a múltiplos órgãos com valores elevados.
    super_fornecedor_id = _next_fornecedor_id()
    fornecedores.append(
        {
            "id": super_fornecedor_id,
            "nome": "SupraMax Serviços Integrados LTDA",
            "documento": _random_cnpj(rng),
            "tipo_documento": "CNPJ",
            "municipio": "Rio de Janeiro",
            "uf": "RJ",
        },
    )

    alvo_orgaos = orgaos[:]
    rng.shuffle(alvo_orgaos)
    alvo_orgaos = alvo_orgaos[: max(4, min(len(alvo_orgaos), 6))]

    base_valor = round(rng.uniform(380_000, 520_000), 2)
    for indice, orgao in enumerate(alvo_orgaos, start=1):
        empenhos.append(
            {
                "id": _next_empenho_id(),
                "numero": f"{current_year}-S{indice:03d}",
                "descricao": "Contrato estratégico de serviços continuados",
                "valor_empenhado": float(base_valor * rng.uniform(0.9, 1.1)),
                "data_empenho": _random_date(rng, days_back=180),
                "fornecedor_id": super_fornecedor_id,
                "unidade_gestora_id": orgao["id"],
                "contrato_id": f"CSUP-{indice:03d}" if include_contratos else None,
            },
        )

    # 2. Comunidade compacta quase isolada (dois órgãos + um fornecedor).
    iso_fornecedor_id = _next_fornecedor_id()
    fornecedores.append(
        {
            "id": iso_fornecedor_id,
            "nome": "Isolado Soluções Regionais ME",
            "documento": _random_cnpj(rng),
            "tipo_documento": "CNPJ",
            "municipio": "Petrópolis",
            "uf": "RJ",
        },
    )

    iso_orgaos: List[Dict] = []
    for idx in range(2):
        iso_orgao = {
            "id": _next_orgao_id(),
            "nome": f"Autarquia Distrital {idx + 1}",
            "sigla": f"AUD{idx + 1}",
            "municipio": "Paraty",
            "uf": "RJ",
        }
        orgaos.append(iso_orgao)
        iso_orgaos.append(iso_orgao)

    for idx, iso_orgao in enumerate(iso_orgaos, start=1):
        empenhos.append(
            {
                "id": _next_empenho_id(),
                "numero": f"{current_year}-ISO-{idx:02d}",
                "descricao": "Programa local de manutenção extraordinária",
                "valor_empenhado": float(round(rng.uniform(45_000, 60_000), 2)),
                "data_empenho": _random_date(rng, days_back=90),
                "fornecedor_id": iso_fornecedor_id,
                "unidade_gestora_id": iso_orgao["id"],
                "contrato_id": f"CISO-{idx:02d}" if include_contratos else None,
            },
        )

    # 3. Elo leve opcional para o fornecedor isolado, preservando característica de isolamento.
    outras_unidades = [org for org in orgaos if org["id"] not in {o["id"] for o in iso_orgaos}]
    if outras_unidades:
        orgao_extra = rng.choice(outras_unidades)
        empenhos.append(
            {
                "id": _next_empenho_id(),
                "numero": f"{current_year}-ISO-LINK",
                "descricao": "Contrato eventual de pequeno valor",
                "valor_empenhado": float(round(rng.uniform(8_000, 12_000), 2)),
                "data_empenho": _random_date(rng, days_back=30),
                "fornecedor_id": iso_fornecedor_id,
                "unidade_gestora_id": orgao_extra["id"],
                "contrato_id": None,
            },
        )
