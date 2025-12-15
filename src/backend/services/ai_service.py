"""Helper functions to interact with Gemini for summaries and insights."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _get_model():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não configurada.")
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Biblioteca google-generativeai não instalada. Adicione ao ambiente.",
        ) from exc

    genai.configure(api_key=api_key)
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-pro")
    try:
        return genai.GenerativeModel(model_name)
    except Exception as exc:  # pragma: no cover - external dependency
        raise RuntimeError(f"Falha ao instanciar modelo '{model_name}': {exc}") from exc


def _invoke(prompt: str, system: Optional[str] = None) -> str:
    model = _get_model()
    base_system = (
        "Você é um assistente para análise de anomalias em empenhos públicos do TCE-RJ. "
        "Stack: PySpark + GraphFrames, Flask, D3. Seja conciso, técnico e forneça próximas ações."
    )
    full_prompt = f"{base_system}\n\n{system or ''}\n\n{prompt}" if system else f"{base_system}\n\n{prompt}"
    response = model.generate_content(full_prompt)
    return response.text or ""


def summarize_anomalies(payload: Dict[str, Any]) -> str:
    prompt = f"""
Você é um auditor de dados públicos. Resuma as anomalias abaixo em linguagem clara,
trazendo 2-3 pontos de risco principais, quais nós/fornecedores/órgãos aparecem e
sugira próximos passos de revisão. Seja conciso (máx. 6 linhas).

Anomalias (JSON):
{payload}
"""
    return _invoke(prompt)


def prioritize_cases(cases: List[Dict[str, Any]]) -> str:
    prompt = f"""
Reordene e priorize os casos de anomalia abaixo (JSON), considerando severidade, valores e contexto.
Devolva um ranking com breve justificativa e uma ação recomendada por caso.

Casos:
{cases}
"""
    return _invoke(prompt)


def suggest_insights(context: Dict[str, Any]) -> str:
    prompt = f"""
Sugira filtros, cruzamentos e visualizações úteis para investigar o grafo e anomalias,
baseado no contexto a seguir (nós resumidos, anomalias e metadados).
Entregue 3-5 sugestões objetivas.

Contexto:
{context}
"""
    return _invoke(prompt)


def assistant_answer(question: str, context: Optional[Dict[str, Any]] = None) -> str:
    prompt = f"""
Pergunta de usuário sobre gastos públicos / empenhos / grafo:
"{question}"

Contexto opcional (JSON):
{context or {}}

Responda de forma curta, clara e cite termos de forma pedagógica.
"""
    return _invoke(prompt)
