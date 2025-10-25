# Prompt para Codex — Detecção de Anomalias no Fluxo de Despesa Pública

## Objetivo Geral
Criar um sistema que utiliza grafos heterogêneos para detectar anomalias no fluxo de despesa pública do TCE-RJ. Vamos usar os dados da API de Dados Abertos do TCE-RJ e aplicar métricas de redes como **Entropia de Shannon**, **Centralidade de Grau Ponderada**, e **Detecção de Comunidades** para identificar padrões anômalos.

## Stack Tecnológica
- **Backend:** Python + Flask
- **Banco de Dados:** Grafo Heterogêneo (NetworkX ou PyTorch Geometric)
- **Análise de Dados:** Pandas, NumPy, Scikit-learn
- **Infraestrutura:** Docker, GitHub Actions (CI/CD)
- **Testes:** Pytest

## Estrutura do Projeto
O projeto estará dividido em **módulos especializados** (cada um com seu agente). 

---

## Módulos/Agentes

### 1️⃣ DB Architect
- **Objetivo:** Criar o modelo de dados com base nas transações (empenhos) e outros elementos (fornecedores, órgãos, etc.).
- **Tarefas:** Usar **NetworkX** para criar o grafo heterogêneo (nós e arestas).
- **Fontes:** APIs do TCE-RJ (`/empenhos`, `/fornecedores`, `/orgãos`).

### 2️⃣ Backend Engineer
- **Objetivo:** Criar uma API RESTful que fornece os dados do grafo.
- **Tarefas:** Conectar os dados extraídos da API com o grafo e fornecer endpoints para consulta.
- **Fontes:** Integrar as APIs do TCE-RJ e o grafo criado.

### 3️⃣ Data Science Engineer
- **Objetivo:** Implementar as métricas de análise de redes (como centralidade e entropia) e detectar anomalias.
- **Tarefas:** Usar as métricas de redes para identificar padrões anômalos, como **fornecedores polivalentes** e **comunidades isoladas**.
- **Fontes:** O grafo de transações e fornecedores.

---

## Instruções para o Codex
## API: https://dados.tcerj.tc.br/api/v1/docs

1. **Coleta de Dados**: O sistema irá usar os seguintes endpoints da API do TCE-RJ para coletar dados:
    - `GET /empenhos`: Para coletar os empenhos financeiros.
    - `GET /fornecedores`: Para coletar dados sobre os fornecedores (CNPJ/CPF).
    - `GET /unidades-gestoras`: Para coletar os órgãos públicos que geram os empenhos.

2. **Processamento de Dados**: Após coletar os dados, use **Pandas** para estruturar os dados em **DataFrames** e convertê-los para o formato adequado para a construção do grafo.

3. **Construção do Grafo**: Use **NetworkX** para construir um grafo heterogêneo:
    - **Nós:** `Órgãos`, `Fornecedores`, `Empenhos`, `Contratos`.
    - **Arestas:** Representar transações de `Órgão → Fornecedor` via `Empenho`.

4. **Métricas de Análise de Redes**:
    - **Entropia de Shannon** para medir a dispersão de transações entre os fornecedores.
    - **Centralidade de Grau Ponderada** para identificar fornecedores e órgãos com maior influência nas transações.
    - **Detecção de Comunidades** para identificar agrupamentos anômalos no grafo (indicação de possíveis conluios).

5. **Detecção de Anomalias**: Usar as métricas para identificar padrões que sugerem **fraude**, **direcionamento** ou **ineficiência**.

---

## O que você espera do Codex:
1. Use as **APIs do TCE-RJ** (como `/empenhos`, `/fornecedores` e `/unidades-gestoras`) para coletar os dados necessários.
2. Organize os dados coletados em **DataFrames**.
3. Crie o **grafo heterogêneo** com **NetworkX** usando os dados.
4. Aplique as **métricas de análise de redes** para detectar padrões anômalos.
5. Crie a **API** para fornecer os dados processados e as anomalias detectadas.
6. Adicione os **testes de cobertura** usando **Pytest**.
7. Gere a **documentação** detalhada em `docs/` durante o processo (inclusive do modelo de grafo e das métricas).

---

## Primeiros Passos
1. **Gerar o grafo inicial** usando as APIs para coletar os dados de **empenhos**, **fornecedores** e **órgãos**.
2. **Desenvolver a API** para fornecer os dados do grafo.
3. **Aplicar as métricas** de análise de redes (centralidade, entropia, detecção de comunidades).

Agora, gere o código para conectar com as APIs e iniciar a construção do grafo. 
