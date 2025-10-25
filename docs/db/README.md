# Módulo de Dados (DB Architect)

- Cliente da API (`TCEDataClient`) em `src/db/sources/` com paginação controlada.
- Normalização de payloads em DataFrames (`src/db/dataframes.py`).
- Construção do grafo heterogêneo em `src/db/graph_builder.py`.
- Repositório alto nível (`src/db/repository.py`) integra coleta e montagem do grafo.
- Documentação detalhada do modelo: `docs/db/modelo_er.md`.
