(() => {
  const elements = {
    status: document.getElementById("anomaly-status"),
    container: document.getElementById("anomaly-content"),
    refresh: document.getElementById("refresh-anomalies"),
    visualizer: document.getElementById("anomaly-visualizer"),
    selectionLabel: document.getElementById("anomaly-selection-label"),
    graph: document.getElementById("anomaly-graph"),
    details: document.getElementById("anomaly-details"),
    aiSummary: document.getElementById("ai-summary"),
    aiSummaryBtn: document.getElementById("ai-summary-btn"),
    aiQuestion: document.getElementById("ai-question"),
    aiAskBtn: document.getElementById("ai-ask-btn"),
    aiAnswer: document.getElementById("ai-answer"),
    stats: document.getElementById("anomaly-stats"),
  };

  if (!elements.status || !elements.container || !elements.graph) {
    return;
  }

  const TYPE_COLORS = {
    orgao: "#2563eb",
    empenho: "#16a34a",
    fornecedor: "#f97316",
    contrato: "#9333ea",
    contrato_item: "#9333ea",
  };

  const TYPE_LABELS = {
    orgao: "Órgão",
    empenho: "Empenho",
    fornecedor: "Fornecedor",
    contrato: "Contrato",
    contrato_item: "Item de contrato",
  };

  const state = {
    data: {},
    selection: null,
    collapsed: new Set(),
    ai: {
      summary: null,
      loading: false,
      error: null,
      answering: false,
      answerError: null,
      answer: null,
    },
    stats: {
      total: 0,
      bySeverity: {},
    },
  };

  const numberFormatter = new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const integerFormatter = new Intl.NumberFormat("pt-BR");

  function setStatus(message) {
    if (!elements.status || !elements.container) {
      return;
    }
    elements.status.textContent = message;
    elements.status.hidden = false;
    elements.container.hidden = true;
  }

  function clearStatus() {
    if (!elements.status || !elements.container) {
      return;
    }
    elements.status.hidden = true;
    elements.container.hidden = false;
  }

  function toTitle(text) {
    return String(text || "")
      .replace(/_/g, " ")
      .split(" ")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  }

  function severityLabel(severity) {
    if (severity === "alta") {
      return "Alta";
    }
    if (severity === "media") {
      return "Média";
    }
    return "Baixa";
  }

  function formatValue(value) {
    if (Array.isArray(value)) {
      return value.map((item) => formatValue(item)).join(", ");
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return numberFormatter.format(value);
    }
    if (value && typeof value === "object") {
      return JSON.stringify(value);
    }
    if (value === null || value === undefined || value === "") {
      return "—";
    }
    return String(value);
  }

  function formatContextPreview(context = {}) {
    const entries = [];
    Object.entries(context).forEach(([key, value]) => {
      if (key === "subgraph") {
        return;
      }
      if (key === "top_neighbors" && Array.isArray(value)) {
        const labels = value
          .slice(0, 3)
          .map((item) => item.label || item.id)
          .filter(Boolean)
          .join(", ");
        entries.push(`${toTitle(key)}: ${labels || "—"}`);
        return;
      }
      if (Array.isArray(value)) {
        entries.push(`${toTitle(key)}: ${value.slice(0, 3).map((item) => formatValue(item)).join(", ")}`);
        return;
      }
      if (typeof value === "object" && value !== null) {
        const serialized = JSON.stringify(value);
        entries.push(`${toTitle(key)}: ${serialized.slice(0, 60)}${serialized.length > 60 ? "…" : ""}`);
        return;
      }
      entries.push(`${toTitle(key)}: ${formatValue(value)}`);
    });
    return entries.slice(0, 3).join("<br/>") || "—";
  }

  function renderNeighborsTable(neighbors = []) {
    if (!neighbors.length) {
      return "<p>Sem vizinhos destacados.</p>";
    }
    const rows = neighbors
      .map((neighbor) => {
        const value = typeof neighbor.valor === "number" ? numberFormatter.format(neighbor.valor) : "—";
        const nodeValue =
          typeof neighbor.node_valor === "number" ? numberFormatter.format(neighbor.node_valor) : "—";
        return `
          <tr>
            <td>${neighbor.label || neighbor.id}</td>
            <td>${typeLabel(neighbor.type)}</td>
            <td>${neighbor.edge_type || "—"}</td>
            <td>${value}</td>
            <td>${nodeValue}</td>
          </tr>
        `;
      })
      .join("");
    return `
      <table class="anomaly-detail-table">
        <thead>
          <tr>
            <th>Nó</th>
            <th>Tipo</th>
            <th>Relação</th>
            <th>Peso</th>
            <th>Valor do nó</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  function typeLabel(type) {
    if (!type) {
      return "—";
    }
    if (TYPE_LABELS[type]) {
      return TYPE_LABELS[type];
    }
    return toTitle(type);
  }

  function renderContextDetails(context = {}) {
    const entries = [];
    Object.entries(context).forEach(([key, value]) => {
      if (key === "subgraph") {
        return;
      }
      if (key === "top_neighbors" && Array.isArray(value)) {
        entries.push(`
          <section>
            <h4>${toTitle(key)}</h4>
            ${renderNeighborsTable(value)}
          </section>
        `);
        return;
      }
      if (Array.isArray(value) && value.length && typeof value[0] === "object") {
        entries.push(`
          <section>
            <h4>${toTitle(key)}</h4>
            <pre class="anomaly-pre">${JSON.stringify(value, null, 2)}</pre>
          </section>
        `);
        return;
      }
      if (Array.isArray(value)) {
        entries.push(`
          <div class="anomaly-detail">
            <span class="anomaly-detail__term">${toTitle(key)}</span>
            <span class="anomaly-detail__value">${value.map((item) => formatValue(item)).join(", ")}</span>
          </div>
        `);
        return;
      }
      if (typeof value === "object" && value !== null) {
        entries.push(`
          <section>
            <h4>${toTitle(key)}</h4>
            <pre class="anomaly-pre">${JSON.stringify(value, null, 2)}</pre>
          </section>
        `);
        return;
      }
      entries.push(`
        <div class="anomaly-detail">
          <span class="anomaly-detail__term">${toTitle(key)}</span>
          <span class="anomaly-detail__value">${formatValue(value)}</span>
        </div>
      `);
    });
    return entries.length ? entries.join("") : "<p>Sem contexto adicional.</p>";
  }

  function renderAnomalyCard(key, items) {
    const title = toTitle(key);
    const countLabel = items.length
      ? `<span class="tag">${items.length} registros</span>`
      : `<span class="tag tag--empty">Sem registros</span>`;
    const collapsed = state.collapsed.has(key);
    const panelId = `anomaly-panel-${key}`;

    const rows = items
      .map((item, index) => {
        const contextPreview = formatContextPreview(item.context || {});
        const hasGraph = Boolean(item.context?.subgraph?.nodes?.length);
        const severity = item.severity || "media";
        return `
          <tr
            class="anomaly-row"
            data-detector="${key}"
            data-index="${index}"
          >
            <td>${formatValue(item.score)}</td>
            <td>
              ${item.reason}
              <span class="badge-severity badge-severity--${severity}">${severityLabel(severity)}</span>
            </td>
            <td>${contextPreview}</td>
            <td>
              ${
                hasGraph
                  ? `<button
                      type="button"
                      class="button button--secondary button--xs anomaly-viewer"
                      data-detector="${key}"
                      data-index="${index}"
                    >
                      Visualizar
                    </button>`
                  : '<span class="tag tag--empty">Sem grafo</span>'
              }
            </td>
          </tr>
        `;
      })
      .join("");

    return `
      <article class="anomaly-card${collapsed ? " anomaly-card--collapsed" : ""}" data-detector-card="${key}">
        <header class="anomaly-card__header">
          <div class="anomaly-card__heading">
            <button
              class="anomaly-card__toggle"
              type="button"
              data-toggle-detector="${key}"
              aria-expanded="${collapsed ? "false" : "true"}"
              aria-controls="${panelId}"
            >
              <span class="sr-only">Alternar seção ${title}</span>
              <span class="anomaly-card__toggle-icon" aria-hidden="true"></span>
            </button>
            <h2 class="anomaly-card__title">${title}</h2>
          </div>
          ${countLabel}
        </header>
        <div class="anomaly-card__content"${collapsed ? " hidden" : ""} id="${panelId}">
          ${
            items.length
              ? `<table class="anomaly-table anomaly-table--interactive">
                <thead>
                  <tr>
                    <th>Score</th>
                    <th>Motivo</th>
                    <th>Contexto</th>
                    <th>Ações</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>`
              : "<p>Nenhuma ocorrência registrada para este detector.</p>"
          }
        </div>
      </article>
    `;
  }

  function highlightSelection(detector, index) {
    const rows = elements.container.querySelectorAll(".anomaly-row");
    rows.forEach((row) => {
      const rowDetector = row.dataset.detector;
      const rowIndex = Number(row.dataset.index);
      const isActive = detector === rowDetector && index === rowIndex;
      row.classList.toggle("is-active", isActive);
    });
  }

  function renderMiniGraph(subgraph) {
    if (!elements.graph) {
      return;
    }
    elements.graph.innerHTML = "";

    if (!subgraph || !Array.isArray(subgraph.nodes) || !subgraph.nodes.length) {
      elements.graph.innerHTML = "<p class=\"anomaly-graph__empty\">Sem subgrafo associado.</p>";
      return;
    }

    if (typeof d3 === "undefined") {
      elements.graph.innerHTML = "<p class=\"anomaly-graph__empty\">Biblioteca de visualização indisponível.</p>";
      return;
    }

    const width = elements.graph.clientWidth || 420;
    const height = elements.graph.clientHeight || 320;

    const nodes = subgraph.nodes.map((node) => ({
      ...node,
      id: String(node.id),
      isFocus: subgraph.focus ? String(subgraph.focus) === String(node.id) : false,
    }));

    const links = (subgraph.links || []).map((link) => ({
      ...link,
      source: String(link.source),
      target: String(link.target),
    }));

    const svg = d3
      .select(elements.graph)
      .append("svg")
      .attr("class", "mini-graph")
      .attr("width", width)
      .attr("height", height);

    const linkLayer = svg.append("g").attr("class", "mini-graph__links");
    const nodeLayer = svg.append("g").attr("class", "mini-graph__nodes");

    const simulation = d3
      .forceSimulation(nodes)
      .force(
        "link",
        d3
          .forceLink(links)
          .id((node) => node.id)
          .distance(80)
          .strength(0.4),
      )
      .force("charge", d3.forceManyBody().strength(-220))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(24));

    const linkSelection = linkLayer
      .selectAll("line")
      .data(links)
      .join("line")
      .attr("class", "mini-graph__link");

    const nodeSelection = nodeLayer
      .selectAll("g")
      .data(nodes)
      .join((enter) => {
        const group = enter.append("g").attr("class", (node) =>
          `mini-graph__node${node.isFocus ? " mini-graph__node--focus" : ""}`,
        );

        group
          .append("circle")
          .attr("r", (node) => (node.isFocus ? 16 : 12))
          .attr("fill", (node) => TYPE_COLORS[node.type] || "#64748b");

        group
          .append("text")
          .attr("class", "mini-graph__label")
          .attr("dy", 24)
          .text((node) => node.label || node.id);

        return group;
      });

    simulation.on("tick", () => {
      linkSelection
        .attr("x1", (link) => link.source.x)
        .attr("y1", (link) => link.source.y)
        .attr("x2", (link) => link.target.x)
        .attr("y2", (link) => link.target.y);

      nodeSelection.attr("transform", (node) => `translate(${node.x}, ${node.y})`);
    });
  }

  function renderDetails(record, detectorKey) {
    if (!elements.details) {
      return;
    }
    const severity = record.severity || "media";
    const score = typeof record.score === "number" ? numberFormatter.format(record.score) : "—";
    const context = record.context || {};

    elements.details.innerHTML = `
      <section class="anomaly-details__section">
        <header class="anomaly-details__header">
          <h3>${toTitle(detectorKey)}</h3>
          <div class="anomaly-details__meta">
            <span class="badge-severity badge-severity--${severity}">Severidade ${severityLabel(severity)}</span>
            <span class="anomaly-details__score">Score: ${score}</span>
          </div>
        </header>
        <p class="anomaly-details__reason">${record.reason || "Sem descrição detalhada."}</p>
        ${renderContextDetails(context)}
      </section>
    `;
  }

  function updateSelectionLabel(record) {
    if (!elements.selectionLabel) {
      return;
    }
    if (!record) {
      elements.selectionLabel.textContent =
        "Selecione uma anomalia para inspecionar o subgrafo relacionado.";
      return;
    }
    elements.selectionLabel.textContent = `Anomalia selecionada: ${record.reason}`;
  }

  function selectAnomaly(detector, index) {
    const records = state.data[detector] || [];
    const record = records[index];
    if (!record) {
      return;
    }
    state.selection = { detector, index };
    if (state.collapsed.has(detector)) {
      state.collapsed.delete(detector);
      renderAnomalyList();
    } else {
      highlightSelection(detector, index);
    }
    renderMiniGraph(record.context?.subgraph);
    renderDetails(record, detector);
    updateSelectionLabel(record);
  }

  function renderAnomalyList() {
    const keys = Object.keys(state.data || {});
    elements.container.innerHTML = keys
      .map((key) => renderAnomalyCard(key, state.data[key] || []))
      .join("");
    if (state.selection) {
      highlightSelection(state.selection.detector, state.selection.index);
    }
  }

  function handleContainerClick(event) {
    const toggleButton = event.target.closest("[data-toggle-detector]");
    if (toggleButton) {
      const { toggleDetector } = toggleButton.dataset;
      if (toggleDetector) {
        if (state.collapsed.has(toggleDetector)) {
          state.collapsed.delete(toggleDetector);
        } else {
          state.collapsed.add(toggleDetector);
        }
        renderAnomalyList();
      }
      return;
    }

    const trigger = event.target.closest(".anomaly-viewer, .anomaly-row");
    if (!trigger || trigger.classList.contains("tag")) {
      return;
    }
    const detector = trigger.dataset.detector;
    const index = Number(trigger.dataset.index);
    if (!detector || Number.isNaN(index)) {
      return;
    }
    selectAnomaly(detector, index);
  }

  async function loadAnomalies() {
    try {
      setStatus("Carregando anomalias…");
      const response = await fetch("/api/anomalies");
      if (!response.ok) {
        throw new Error(`Erro ao carregar anomalias (status ${response.status})`);
      }
      const data = await response.json();
      state.data = data;
      state.selection = null;
      computeStats(data);
      const keys = Object.keys(data || {});
      Array.from(state.collapsed).forEach((key) => {
        if (!keys.includes(key)) {
          state.collapsed.delete(key);
        }
      });
      if (!keys.length) {
        elements.container.innerHTML =
          "<p>Nenhuma anomalia registrada pelo pipeline atual.</p>";
        renderMiniGraph(null);
        renderDetails({ reason: "Sem anomalias registradas.", context: {} }, "");
        updateSelectionLabel(null);
      } else {
        renderAnomalyList();
        updateSelectionLabel(null);
        renderMiniGraph(null);
        renderDetails({ reason: "Selecione um item para ver detalhes.", context: {} }, "");
      }
      clearStatus();
    } catch (error) {
      console.error(error);
      setStatus(error.message || "Erro inesperado ao carregar as anomalias.");
    }
  }

  function renderAISummary() {
    const target = elements.aiSummary;
    if (!target) return;
    if (state.ai.loading) {
      target.innerHTML = '<p class="ai-box__status">Gerando resumo com IA…</p>';
      return;
    }
    if (state.ai.error) {
      target.innerHTML = `<p class="ai-box__error">Erro: ${state.ai.error}</p>`;
      return;
    }
    if (!state.ai.summary) {
      target.innerHTML = '<p class="ai-box__status">Clique em "Resumo IA" para gerar um sumário das anomalias.</p>';
      return;
    }
    const formatted = renderMarkdown(state.ai.summary);
    target.innerHTML = `
      <p class="ai-box__label">Resumo gerado pela IA:</p>
      <div class="ai-box__content">${formatted}</div>
    `;
  }

  function renderAIAssistant() {
    const target = elements.aiAnswer;
    if (!target) return;
    if (state.ai.answering) {
      target.innerHTML = '<p class="ai-box__status">Consultando a IA…</p>';
      return;
    }
    if (state.ai.answerError) {
      target.innerHTML = `<p class="ai-box__error">Erro: ${state.ai.answerError}</p>`;
      return;
    }
    if (!state.ai.answer) {
      target.innerHTML = '<p class="ai-box__status">Faça uma pergunta para o assistente.</p>';
      return;
    }
    const formatted = renderMarkdown(state.ai.answer);
    target.innerHTML = `<div class="ai-box__content">${formatted}</div>`;
  }

  async function requestAISummary() {
    if (state.ai.loading) return;
    state.ai.loading = true;
    state.ai.error = null;
    renderAISummary();
    try {
      const response = await fetch("/api/ai/summary", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(state.data || {}),
      });
      if (!response.ok) {
        const msg = await response.text();
        throw new Error(msg || `Status ${response.status}`);
      }
      const payload = await response.json();
      state.ai.summary = payload.summary || "IA não retornou resumo.";
    } catch (error) {
      console.error("Erro ao gerar resumo IA:", error);
      state.ai.error = error?.message || "Não foi possível gerar o resumo no momento.";
    } finally {
      state.ai.loading = false;
      renderAISummary();
    }
  }

  async function requestAIAssistant() {
    if (state.ai.answering) return;
    const question = elements.aiQuestion?.value?.trim();
    if (!question) {
      state.ai.answerError = "Digite uma pergunta.";
      renderAIAssistant();
      return;
    }
    state.ai.answering = true;
    state.ai.answerError = null;
    renderAIAssistant();
    try {
      const response = await fetch("/api/ai/assistant", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, context: { anomalies: state.data } }),
      });
      if (!response.ok) {
        const msg = await response.text();
        throw new Error(msg || `Status ${response.status}`);
      }
      const payload = await response.json();
      state.ai.answer = payload.answer || "IA não retornou resposta.";
    } catch (error) {
      console.error("Erro no assistente IA:", error);
      state.ai.answerError = error?.message || "Não foi possível consultar a IA no momento.";
    } finally {
      state.ai.answering = false;
      renderAIAssistant();
    }
  }

  if (elements.refresh) {
    elements.refresh.addEventListener("click", loadAnomalies);
  }

  elements.container.addEventListener("click", handleContainerClick);

  loadAnomalies();
  renderAISummary();
  renderAIAssistant();

  if (elements.aiSummaryBtn) {
    elements.aiSummaryBtn.addEventListener("click", requestAISummary);
  }
  if (elements.aiAskBtn) {
    elements.aiAskBtn.addEventListener("click", requestAIAssistant);
  }

  function renderMarkdown(text) {
    if (!text) return "";
    const lines = text.split("\n").map((line) => line.trim()).filter(Boolean);

    const output = [];
    let listBuffer = [];

    const flushList = () => {
      if (listBuffer.length) {
        output.push("<ul>" + listBuffer.map((it) => `<li>${it}</li>`).join("") + "</ul>");
        listBuffer = [];
      }
    };

    lines.forEach((line) => {
      if (/^###\\s+/.test(line)) {
        flushList();
        output.push(`<h4>${line.replace(/^###\\s+/, "")}</h4>`);
        return;
      }
      if (/^##\\s+/.test(line)) {
        flushList();
        output.push(`<h3>${line.replace(/^##\\s+/, "")}</h3>`);
        return;
      }
      if (/^#\\s+/.test(line)) {
        flushList();
        output.push(`<h2>${line.replace(/^#\\s+/, "")}</h2>`);
        return;
      }
      if (/^[-*•]\\s+/.test(line)) {
        listBuffer.push(line.replace(/^[-*•]\\s+/, ""));
        return;
      }
      flushList();
      output.push(`<p>${line}</p>`);
    });

    flushList();

    const html = output.join("");
    return html
      .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
      .replace(/<p>\s*<ul>/g, "<ul>")
      .replace(/<\/ul>\s*<\/p>/g, "</ul>");
  }

  function computeStats(data) {
    const stats = { total: 0, bySeverity: {} };
    Object.values(data || {}).forEach((items) => {
      (items || []).forEach((item) => {
        stats.total += 1;
        const sev = (item.severity || "media").toLowerCase();
        stats.bySeverity[sev] = (stats.bySeverity[sev] || 0) + 1;
      });
    });
    state.stats = stats;
    renderStats();
  }

  function renderStats() {
    const target = elements.stats;
    if (!target) return;
    const total = state.stats.total || 0;
    const sev = state.stats.bySeverity || {};
    const card = (label, value, className = "") =>
      `<div class="stat-card ${className}"><p class="stat-label">${label}</p><p class="stat-value">${value}</p></div>`;
    target.innerHTML = `
      ${card("Total de anomalias", total, "stat-card--primary")}
      ${card("Severidade alta", sev.alta || 0, "stat-card--alert")}
      ${card("Severidade média", sev.media || 0, "stat-card--warn")}
      ${card("Severidade baixa", sev.baixa || 0, "stat-card--info")}
    `;
  }
})();
