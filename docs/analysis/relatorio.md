# Relatório de Engenharia de Dados e Análise

## Objetivo
Implementar métricas de rede e heurísticas de detecção de anomalias sobre o grafo heterogêneo de empenhos, identificando fornecedores, órgãos e comunidades com comportamento fora do padrão.

## Métricas Implementadas
- **Centralidade de grau ponderada:** calcula a soma dos valores de empenhos conectados por nó (órgãos e fornecedores).
- **Entropia de Shannon por vizinhança:** mede a dispersão dos gastos de cada fornecedor entre os órgãos e vice-versa.
- **Detecção de comunidades (Louvain):** agrupa nós em comunidades ponderadas por valor.

## Heurísticas de Anomalia
- **Fornecedores centralizados:** fornecedores com altos valores somados podem indicar concentração de contratos.
- **Órgãos centralizados:** órgãos que concentram gastos acima da média.
- **Alta entropia:** fornecedores que diversificam excessivamente suas relações podem indicar polivalência suspeita.
- **Comunidades isoladas:** clusters pouco conectados ao resto da rede sugerem possíveis conluios.

## Pipeline
1. Recebe o grafo `MultiDiGraph` gerado pelo DB Architect.
2. Executa `run_default_anomaly_suite` consolidando métricas e anomalias.
3. Expõe o relatório em formato serializável via `AnalysisReport.as_dict()` para uso na API.

## Próximos Passos
- Calibrar limiares com base em dados históricos reais.
- Integrar algoritmos adicionais (PageRank ponderado, detecção supervisionada).
- Persistir histórico de anomalias para acompanhamento longitudinal.
