# Plano Detalhado de Execução

## Visão Geral
- **Objetivo principal:** Construir um sistema de detecção de anomalias em empenhos públicos com base em grafos heterogêneos.
- **Metas complementares:** Expor os resultados via API, visualizar as anomalias no frontend, automatizar deploy/CI/CD e garantir cobertura de testes mínima de 90%.
- **Entregas parciais:** Cada papel entrega código, testes e documentação nas pastas indicadas.

## Sequência Macro
1. Alinhamento inicial, insumos e definição dos modelos de dados.
2. Implementação paralela dos componentes principais (grafo, API, análise, frontend).
3. Integração e automação (infra/CI).
4. Qualidade, testes integrados e documentação final.

## Etapas por Papel

### DB Architect
1. **Requisitos do grafo (docs/db/modelo_er.md):** mapear entidades, atributos e relacionamentos; definir métricas de análise prioritárias.
2. **Protótipo de grafo (src/db/schema.py):** criar estrutura de criação de nós/arestas com NetworkX; adicionar carregamento inicial a partir de dados fictícios.
3. **Funções utilitárias (src/db/utils.py):** operações para atualização incremental e consultas básicas; validações de consistência.
4. **Documentação técnica:** detalhar decisão de modelagem, métricas suportadas e exemplos de uso no `docs/db/modelo_er.md`.

### Backend API Engineer
1. **Configuração do projeto Flask (src/backend/app.py):** definir factory, rotas base e integração com módulo do grafo.
2. **Endpoints principais:** leitura de entidades, navegação no grafo, geração de relatórios de anomalias; contratos descritos em `docs/backend/openapi.yaml`.
3. **Camada de serviços (src/backend/services/):** encapsular regras de negócio e integração com análise/DB.
4. **Testes (tests/backend/):** cobrir rotas e serviços com Pytest e fixtures representando grafos de exemplo.

### Data Science Engineer
1. **Definição de métricas (docs/analysis/relatorio.md):** priorizar métricas (centralidade, entropia, comunidades) e critérios de anomalia.
2. **Implementação modular (src/analysis/metrics.py / detectors.py):** funções puras reutilizáveis que consomem grafos do DB Architect.
3. **Pipelines de análise (src/analysis/pipeline.py):** orquestrar cálculo de métricas e geração de rankings de anomalia para uso pela API.
4. **Validação (tests/analysis/):** testes unitários das métricas e detecções com grafos sintéticos; benchmark básico registrado em relatório.

### Frontend Engineer
1. **Setup inicial (src/frontend/):** definir estrutura (ex: React/Vite) e componentes fundamentais.
2. **Integrações com API:** serviços para consumir endpoints de dados/anomalias.
3. **Visualizações:** componentes para grafos e dashboards (d3.js ou libs similares); foco em navegabilidade e filtros.
4. **Testes de UI (tests/frontend/):** smoke tests e snapshots; plano de acessibilidade em `docs/frontend/design-system.md`.

### DevOps Engineer
1. **Dockerização:** `Dockerfile` para backend/analysis, `docker-compose.yml` orquestrando serviços e dados.
2. **Pipelines CI/CD (.github/workflows/ci.yml):** lint, testes, build e publicação de artefatos.
3. **Templates de ambiente (`.env.example`):** variáveis para DB, API, credenciais mock.
4. **Observabilidade inicial:** hooks para logs e métricas básicos descritos em `docs/infra/observabilidade.md`.

### QA Engineer
1. **Plano de testes (docs/qa/checklist.md):** escopo, critérios de aceite, cobertura mínima e matriz de responsabilidades.
2. **Configuração Pytest (tests/conftest.py):** fixtures compartilhadas e integração com relatórios de cobertura.
3. **Monitoramento de cobertura:** script/CI para gerar report em `docs/qa/relatorios.md`.
4. **Testes end-to-end:** especificar cenários críticos (API + análise + frontend) e automatizar quando viável.

## Entregas Integradas
- **Integração contínua:** validação automática em PRs.
- **Documentação viva:** manter README, OpenAPI, relatórios e checklists atualizados a cada etapa.
- **Revisões cruzadas:** cada papel revisa entregas críticas antes de integração final.

## Próximos Passos Imediatos
1. Criar estruturas de diretórios e arquivos placeholders necessários.
2. Configurar ambiente de desenvolvimento (virtualenv, dependências base).
3. Implementar protótipos mínimos de grafo e API para validar comunicação.
4. Definir dataset amostral para testes locais nas etapas seguintes.
