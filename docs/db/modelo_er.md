# Modelo de Dados em Grafo

## Visão Geral
O grafo heterogêneo representa o ciclo do gasto público conectando órgãos, empenhos, fornecedores e contratos. Cada nó carrega metadados relevantes para auditoria e cada aresta representa uma relação transacional.

## Nós
- **Órgão (`orgao`)**: unidade gestora que emite o empenho. Atributos: `nome`, `sigla`, `municipio`, `uf`.
- **Empenho (`empenho`)**: registro financeiro. Atributos: `numero`, `descricao`, `valor`, `data`.
- **Fornecedor (`fornecedor`)**: destinatário do pagamento. Atributos: `nome`, `documento`, `tipo_documento`, `municipio`, `uf`.
- **Contrato (`contrato`)** *(opcional)*: vínculo contratual associado ao empenho.

## Arestas
- `orgao -> empenho` (`orgao_empenho`): indica qual órgão originou o empenho. Peso: `valor`.
- `empenho -> fornecedor` (`empenho_fornecedor`): vincula o empenho ao fornecedor. Peso: `valor`.
- `empenho -> contrato` (`empenho_contrato`): identifica o contrato relacionado quando existir.

## Construção
1. **Coleta** via `TCEDataClient` nos endpoints `/empenhos`, `/fornecedores` e `/unidades-gestoras`.
2. **Normalização** em DataFrames (`build_empenhos_df`, `build_fornecedores_df`, `build_orgaos_df`).
3. **Montagem do grafo** com `build_heterogeneous_graph`, produzindo DataFrames Spark de vértices e arestas encapsulados em um `GraphFrame`.

## Métricas de Apoio
O grafo suporta as métricas calculadas em PySpark/GraphFrames:
- Centralidade de grau ponderada (valores financeiros).
- Entropia de distribuição de gastos por fornecedor/órgão.
- Detecção de comunidades (propagação de rótulos via GraphFrames) aplicada sobre subgrafos projetados.

## Estratégia de Persistência
Inicialmente, o grafo é mantido em memória. Para reprodutibilidade, o `GraphRepository` aceita payloads locais e o modo live (habilitado via `ENABLE_LIVE_FETCH=true`) consulta o TCE-RJ com paginação controlada.
