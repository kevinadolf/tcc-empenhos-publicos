# Objetivo do Projeto
Desenvolver um sistema para detectar anomalias em transações financeiras de despesa pública (empemhos) utilizando uma abordagem baseada em grafos heterogêneos. O sistema deverá identificar padrões de fraude, direcionamento e ineficiência nas transações do TCE-RJ e fornecer visualizações interativas para auditores.

## Stack Tecnológica
- **Backend:** Python + Flask/FastAPI
- **Banco de Dados:** Grafo Heterogêneo (NetworkX ou PyTorch Geometric)
- **Análise de Dados:** Pandas, NumPy, Scikit-learn
- **Infraestrutura:** Docker, GitHub Actions (CI/CD)
- **Testes:** Pytest

## Estrutura do Projeto
O projeto será estruturado em várias áreas de atuação, cada uma delegada a um **agente especializado**. A seguir, estão os detalhes de cada um dos agentes que farão o desenvolvimento.

---

## Agentes e Seus Papéis

### 1️⃣ DB Architect
- **Objetivo:** Criar o modelo de dados para representar o grafo heterogêneo que conecta os nós: `Órgãos`, `Fornecedores`, `Empenhos`, `Contratos`, `Itens de Despesa`.
- **Tarefas:** Definir nós e arestas, suas propriedades e métricas de análise (centralidade, comunidade, entropia, etc.).
- **Entregáveis:** 
  - Código do grafo em `src/db/`
  - Documentação em `docs/db/modelo_er.md`
  - Scripts para manipulação e visualização do grafo.

---

### 2️⃣ Backend API Engineer
- **Objetivo:** Criar a API RESTful que permite a interação com o grafo de dados e facilita a análise de anomalias.
- **Tarefas:** Desenvolver endpoints para acessar os dados dos empenhos, fornecedores, órgãos, entre outros, e fornecer relatórios sobre as anomalias detectadas.
- **Entregáveis:**
  - API RESTful em `src/backend/`
  - Testes de integração e unitários em `tests/backend/`
  - Documentação da API em `docs/backend/openapi.yaml`

---

### 3️⃣ Data Science Engineer (Análise e Detecção de Anomalias)
- **Objetivo:** Implementar as métricas de redes e técnicas de detecção de anomalias no grafo (ex: Entropia de Shannon, Centralidade, Detecção de Comunidades).
- **Tarefas:** Analisar o grafo com técnicas de redes e aplicar algoritmos para identificar anomalias, como fornecedores polivalentes ou comunidades isoladas.
- **Entregáveis:**
  - Algoritmos de análise e detecção de anomalias em `src/analysis/`
  - Testes e validações de métricas em `tests/analysis/`
  - Relatórios sobre os resultados em `docs/analysis/relatorio.md`

---

### 4️⃣ Frontend Engineer
- **Objetivo:** Criar a interface visual para exibição interativa dos dados e anomalias detectadas.
- **Tarefas:** Implementar gráficos interativos, dashboards e relatórios com visualização do grafo e das anomalias.
- **Entregáveis:**
  - Frontend interativo em `src/frontend/`
  - Testes de UI em `tests/frontend/`
  - Documentação do design em `docs/frontend/design-system.md`

---

### 5️⃣ DevOps Engineer
- **Objetivo:** Configurar a infraestrutura de Docker e CI/CD para o desenvolvimento e deploy do sistema.
- **Tarefas:** Criar Dockerfile, docker-compose, e configurar GitHub Actions para CI/CD.
- **Entregáveis:**
  - Dockerfile e docker-compose em `infra/`
  - Configuração de CI/CD em `.github/workflows/ci.yml`
  - `.env.example` para configuração do ambiente

---

### 6️⃣ QA Engineer
- **Objetivo:** Garantir a cobertura de testes e a qualidade do sistema.
- **Tarefas:** Criar testes unitários e de integração, configurar cobertura mínima de 90%.
- **Entregáveis:**
  - Testes em `tests/`
  - Relatórios de cobertura em `docs/qa/relatorios.md`
  - Checklist de QA em `docs/qa/checklist.md`

---

## Fluxo de Execução

1. **Planejamento e Arquitetura**: O Codex começa lendo `docs/00-prompt-inicial.md`, `docs/01-brief.md` e `docs/02-referencia.md` para entender o projeto.
2. **Execução por Papéis**:
   - DB Architect
   - Backend API Engineer
   - Data Science Engineer
   - Frontend Engineer
   - DevOps Engineer
   - QA Engineer
3. **Integração e Testes**: Após o desenvolvimento, o Codex realizará a integração das partes, rodará os testes e validará o sistema.
4. **Documentação**: A documentação será gerada ao longo do processo e armazenada em `docs/`.

---

Agora, gere o plano detalhado e a estrutura inicial do projeto.