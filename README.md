# Sistema de Detecção de Anomalias em Empenhos Públicos

## Visão Geral
Plataforma em desenvolvimento para identificar padrões suspeitos em transações de despesa pública (TCE-RJ) usando grafos heterogêneos. A solução integra módulos de modelagem de dados (DB Architect), análise e detecção (Data Science), API REST (Backend), infraestrutura (DevOps) e garantia de qualidade (QA).

## Pré-requisitos
- Python 3.11+
- Libs do projeto (`requirements.txt`)
- Docker & Docker Compose (opcional)

## Configuração do Ambiente
```bash
python -m venv .venv
source .venv/bin/activate 
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Variáveis de Ambiente
Copie `.env.example` para `.env` e ajuste quando necessário.
- `ENABLE_LIVE_FETCH=true` habilita coleta real na API do TCE-RJ (padrão: dados mockados).
- `TCE_API_BASE_URL` e `TCE_API_PAGE_SIZE` configuram a paginação.
- `TCE_API_MAX_PAGES` limita a quantidade de páginas durante desenvolvimento.

## Execução
- **Backend/API:** `python -m src.backend.app` (porta `8000`).
- **Interface Web:** com o backend ativo, acesse `http://127.0.0.1:8000/` e navegue pelo Explorador do Grafo, painel de Anomalias e catálogo de nós.
- **Docker:** `docker compose up` para rodar o backend em container.
- **Análises:** expostas via `/api/anomalies` ou consumo direto dos módulos em `src/analysis/`.
- **CLI:** `python -m src.backend.cli summary`, `python -m src.backend.cli anomalies --limit 5` ou `python -m src.backend.cli snapshot --graphml out.graphml --json out.json`.
- **Docker + CLI:** `docker compose run --rm cli snapshot --graphml out.graphml` (usa entrypoint inteligente).

### Endpoints Principais
- `GET /api/health`
- `GET /api/graph/summary`
- `GET /api/graph/snapshot`
- `GET /api/graph/nodes`
- `GET /api/anomalies`

Detalhes completos em `docs/backend/openapi.yaml`.

## Testes
```bash
pytest
pytest --cov=src --cov-report=term
```
Meta de cobertura: ≥ 90% (relatórios adicionais em `docs/qa/`).

## Estrutura
```
src/
  backend/
  analysis/
  db/
  frontend/
  common/
tests/
  backend/
  analysis/
  db/
docs/
  backend/
  analysis/
  db/
  infra/
  qa/
```

## Próximos Passos
1. Calibrar limiares das heurísticas com dados reais do TCE-RJ.
2. Expandir pipelines de CI com linting e verificação de segurança.
3. Evoluir o Explorador do Grafo com filtros adicionais e dashboards.
