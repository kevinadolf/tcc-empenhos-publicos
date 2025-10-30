# Sistema de Detecção de Anomalias em Empenhos Públicos

## Visão Geral
Projeto em construção para identificar padrões suspeitos em transações financeiras públicas usando grafos heterogêneos. A aplicação será composta por camadas de dados, análise, API e frontend, todas integradas e cobertas por automação de testes e CI/CD.

## Pré-requisitos
- Python 3.11+
- Pipenv ou `venv` + `pip`
- Docker & Docker Compose (opcional neste estágio, obrigatório para ambiente containerizado)

## Configuração do Ambiente
```bash
# 1. Criar e ativar o ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Atualizar instalador e instalar dependências iniciais
python -m pip install --upgrade pip
pip install -r requirements.txt  # arquivo será versionado quando as dependências forem definidas
```

## Execução (roadmap)
1. **Backend/API:** `python -m src.backend.app` – rodará o servidor Flask quando implementado.
2. **Análises:** scripts sob `src/analysis/` expostos via CLI ou integrados à API.
3. **Frontend:** aplicação em `src/frontend/` (ex.: `npm install && npm run dev` quando configurado).
4. **Docker:** `docker compose up` (arquivo será acrescentado na fase DevOps).

## Testes
```bash
# Testes unitários e de integração
pytest

# Com relatórios de cobertura (meta ≥ 90%)
pytest --cov=src --cov-report=term-missing
```

Relatórios adicionais serão disponibilizados em `docs/qa/` à medida que o projeto evoluir.

## Estrutura Inicial
```
src/
  backend/
  analysis/
  db/
  frontend/
tests/
  backend/
  analysis/
  frontend/
docs/
  db/
  backend/
  analysis/
  frontend/
  qa/
  infra/
```

## Próximos Passos
- Definir requisitos de dados do grafo e preparar dataset amostral.
- Especificar contratos da API em `docs/backend/openapi.yaml`.
- Configurar dependências principais (Flask, PySpark, GraphFrames, Pandas).
- Definir `GRAPH_DATA_SOURCE` para alternar entre dados de demonstração (`sample`), dados aleatórios (`random`) ou coleta via API (`api`).
