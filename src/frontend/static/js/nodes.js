(() => {
  const elements = {
    status: document.getElementById("node-status"),
    table: document.getElementById("node-table"),
    tbody: document.querySelector("#node-table tbody"),
    search: document.getElementById("node-search"),
    typeFilter: document.getElementById("node-type-filter"),
  };

  if (!elements.status || !elements.table || !elements.tbody) {
    return;
  }

  let nodes = [];
  let riskScores = {};

  function setStatus(message) {
    elements.status.textContent = message;
    elements.status.hidden = false;
    elements.table.hidden = true;
  }

  function clearStatus() {
    elements.status.hidden = true;
    elements.table.hidden = false;
  }

  function formatCurrency(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
      return "—";
    }
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
    }).format(value);
  }

  function renderRows(list) {
    if (!list.length) {
      elements.tbody.innerHTML =
        '<tr><td colspan="4">Nenhum nó encontrado com os filtros atuais.</td></tr>';
      return;
    }

    elements.tbody.innerHTML = list
      .map((node) => {
        const link = `/?node=${encodeURIComponent(node.id)}`;
        const risk = riskScores[node.id];
        const riskLabel = typeof risk?.score === "number" ? risk.score.toFixed(2) : "—";
        const riskSignals = risk?.signals ? `${risk.signals} sinais` : "";
        const riskBadge = risk
          ? `<span class="badge-risk">${riskLabel}</span> <small class="badge-type">${riskSignals}</small>`
          : "—";
        return `
          <tr data-node-id="${node.id}">
            <td><a href="${link}" class="nav__link">${node.id}</a></td>
            <td>${node.label || "—"}</td>
            <td><span class="badge-type">${node.type}</span></td>
            <td>${formatCurrency(node.valor)}</td>
            <td>${riskBadge}</td>
          </tr>
        `;
      })
      .join("");
  }

  function applyFilters() {
    const searchTerm = elements.search.value.trim().toLowerCase();
    const type = elements.typeFilter.value;
    const filtered = nodes.filter((node) => {
      const matchesType = type === "all" || node.type === type;
      if (!matchesType) return false;
      if (!searchTerm) return true;
      const haystack = `${node.id} ${node.label ?? ""}`.toLowerCase();
      return haystack.includes(searchTerm);
    });
    renderRows(filtered);
  }

  async function loadNodes() {
    try {
      setStatus("Carregando nós…");
      const response = await fetch("/api/graph/nodes");
      if (!response.ok) {
        throw new Error(`Erro ao carregar nós (status ${response.status})`);
      }
      nodes = await response.json();
      await loadRisk();
      clearStatus();
      applyFilters();
    } catch (error) {
      console.error(error);
      setStatus(error.message || "Erro inesperado ao carregar os nós.");
    }
  }

  async function loadRisk() {
    try {
      const response = await fetch("/api/risk");
      if (!response.ok) {
        throw new Error(`Erro ao carregar scores de risco (status ${response.status})`);
      }
      riskScores = await response.json();
    } catch (error) {
      console.warn("Falha ao carregar scores de risco:", error);
      riskScores = {};
    }
  }

  if (elements.search) {
    elements.search.addEventListener("input", applyFilters);
  }

  if (elements.typeFilter) {
    elements.typeFilter.addEventListener("change", applyFilters);
  }

  loadNodes();
})();
