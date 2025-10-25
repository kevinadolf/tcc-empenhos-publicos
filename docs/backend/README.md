# Backend API

## Visão Geral
O backend fornece endpoints RESTful para consultar o grafo de empenhos e os relatórios de anomalias. Utiliza Flask e integra-se ao `GraphService`, que monta o grafo (DB Architect) e executa as análises (Data Science).

## Endpoints
| Método | Rota               | Descrição                              |
|--------|--------------------|----------------------------------------|
| GET    | `/api/health`      | Verifica disponibilidade.              |
| GET    | `/api/graph/summary` | Estatísticas do grafo em memória.     |
| GET    | `/api/anomalies`   | Retorna as anomalias detectadas.       |

A especificação completa encontra-se em `docs/backend/openapi.yaml`.

## Configuração
- Defina variáveis no `.env` (ver README principal).
- `ENABLE_LIVE_FETCH=true` ativa a coleta real dos endpoints do TCE-RJ.
- Para desenvolvimento offline, a API utiliza `SAMPLE_PAYLOAD` com dados mockados.

## Execução Local
```bash
python -m src.backend.app
# ou
flask --app src.backend.app:create_app run
```

### CLI auxiliar
```bash
python -m src.backend.cli summary
python -m src.backend.cli anomalies --limit 5 --pretty
python -m src.backend.cli snapshot --graphml out.graphml --json out.json
```

### Docker
```bash
# API
docker compose up backend

# CLI (usa entrypoint para invocar o comando desejado)
docker compose run --rm cli anomalies --limit 3
docker compose run --rm cli snapshot --graphml graph.graphml
```

## Testes
```bash
pytest tests/backend -q
```

## Próximos Incrementos
1. Adicionar autenticação/controle de acesso.
2. Paginação e filtros nos endpoints.
3. Streaming dos resultados de análise para dashboards em tempo real.
