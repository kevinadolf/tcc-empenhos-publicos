# Observabilidade e Logs

## Logs
- Flask configurado com `debug=True` apenas em desenvolvimento.
- Configurar `LOG_LEVEL` futuro via `.env` para ajustar verbosidade.

## Métricas Futuras
- Exportar métricas de uso da API (requests/s, tempo de resposta) via Prometheus.
- Monitorar anomalias geradas por período para identificar tendências.

## Próximos Passos
1. Integrar ferramenta de logging estruturado (ex.: `structlog`).
2. Adicionar tracing distribuído quando frontend estiver disponível.
3. Automatizar alerta quando volume de anomalias ultrapassar limites definidos.
