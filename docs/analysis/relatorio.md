# Relatório de Engenharia de Dados e Análise

## Objetivo
Implementar métricas de rede e heurísticas de detecção de anomalias sobre o grafo heterogêneo de empenhos, identificando fornecedores, órgãos e comunidades com comportamento fora do padrão.

## Métricas Implementadas
- **Centralidade de grau ponderada (PySpark):** calcula a soma dos valores de empenhos conectados por nó (órgãos e fornecedores) usando agregações distribuídas.
- **Entropia de Shannon por vizinhança:** mede a dispersão dos gastos de cada fornecedor entre os órgãos e vice-versa diretamente nos DataFrames de arestas.
- **Propagação de rótulos (GraphFrames):** aproxima comunidades ponderadas por valor sem necessidade de converter o grafo para NetworkX.

## Heurísticas de Anomalia
- **Fornecedores centralizados:** fornecedores com altos valores somados podem indicar concentração de contratos.
- **Órgãos centralizados:** órgãos que concentram gastos acima da média.
- **Alta entropia:** fornecedores que diversificam excessivamente suas relações podem indicar polivalência suspeita.
- **Comunidades isoladas:** clusters pouco conectados ao resto da rede sugerem possíveis conluios.

## Pipeline
1. Recebe o grafo `SparkGraph` (vértices + arestas em PySpark) gerado pelo DB Architect.
2. Executa `run_default_anomaly_suite` consolidando métricas e anomalias.
3. Expõe o relatório em formato serializável via `AnalysisReport.as_dict()` para uso na API.

## Próximos Passos
- Calibrar limiares com base em dados históricos reais.
- Integrar algoritmos adicionais (PageRank ponderado, detecção supervisionada).
- Persistir histórico de anomalias para acompanhamento longitudinal.
