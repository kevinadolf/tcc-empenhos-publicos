(() => {
  const elements = {
    status: document.getElementById("anomaly-status"),
    container: document.getElementById("anomaly-content"),
    refresh: document.getElementById("refresh-anomalies"),
  };

  if (!elements.status || !elements.container) {
    return;
  }

  function setStatus(message) {
    elements.status.textContent = message;
    elements.status.hidden = false;
    elements.container.hidden = true;
  }

  function clearStatus() {
    elements.status.hidden = true;
    elements.container.hidden = false;
  }

  function toTitle(text) {
    return text
      .replace(/_/g, " ")
      .split(" ")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  }

  function formatContext(context) {
    return Object.entries(context).map(([key, value]) => {
      if (Array.isArray(value)) {
        return `${key}: ${value.map((item) => formatValue(item)).join(", ")}`;
      }
      if (typeof value === "object" && value !== null) {
        return `${key}: ${JSON.stringify(value)}`;
      }
      return `${key}: ${formatValue(value)}`;
    });
  }

  function formatValue(value) {
    if (Array.isArray(value)) {
      return value.join(" / ");
    }
    if (typeof value === "number") {
      return new Intl.NumberFormat("pt-BR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(value);
    }
    if (Array.isArray(value) || typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value);
  }

  function renderAnomalyCard(key, items) {
    const title = toTitle(key);
    const countLabel = items.length
      ? `<span class="tag">${items.length} registros</span>`
      : `<span class="tag tag--empty">Sem registros</span>`;

    const rows = items
      .map((item) => {
        const context = formatContext(item.context || {});
        return `
          <tr>
            <td>${formatValue(item.score)}</td>
            <td>${item.reason}</td>
            <td>${context.join("<br/>") || "—"}</td>
          </tr>
        `;
      })
      .join("");

    return `
      <article class="anomaly-card">
        <header class="anomaly-card__header">
          <h2 class="anomaly-card__title">${title}</h2>
          ${countLabel}
        </header>
        ${
          items.length
            ? `<table class="anomaly-table">
                <thead>
                  <tr>
                    <th>Score</th>
                    <th>Motivo</th>
                    <th>Contexto</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>`
            : "<p>Nenhuma ocorrência registrada para este detector.</p>"
        }
      </article>
    `;
  }

  async function loadAnomalies() {
    try {
      setStatus("Carregando anomalias…");
      const response = await fetch("/api/anomalies");
      if (!response.ok) {
        throw new Error(`Erro ao carregar anomalias (status ${response.status})`);
      }
      const data = await response.json();
      const keys = Object.keys(data);
      if (!keys.length) {
        elements.container.innerHTML =
          "<p>Nenhuma anomalia registrada pelo pipeline atual.</p>";
      } else {
        elements.container.innerHTML = keys
          .map((key) => renderAnomalyCard(key, data[key] || []))
          .join("");
      }
      clearStatus();
    } catch (error) {
      console.error(error);
      setStatus(error.message || "Erro inesperado ao carregar as anomalias.");
    }
  }

  if (elements.refresh) {
    elements.refresh.addEventListener("click", loadAnomalies);
  }

  loadAnomalies();
})();
