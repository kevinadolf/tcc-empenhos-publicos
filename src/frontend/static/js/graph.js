(() => {
  const elements = {
    canvas: document.getElementById("graph-canvas"),
    status: document.getElementById("graph-status"),
    sidebar: document.getElementById("sidebar-content"),
    fit: document.getElementById("fit-graph"),
    dataSource: document.getElementById("data-source"),
    filterTypeList: document.getElementById("filter-type-list"),
    filterValueMin: document.getElementById("filter-value-min"),
    filterValueMax: document.getElementById("filter-value-max"),
    filterSearch: document.getElementById("filter-search"),
    filterAnomalies: document.getElementById("filter-anomalies"),
    filterClear: document.getElementById("clear-filters"),
    filterSummary: document.getElementById("filter-summary"),
  };

  if (!elements.canvas) {
    return;
  }

  const SEVERITY_RANK = {
    baixa: 1,
    media: 2,
    alta: 3,
  };

  const TYPE_COLORS = {
    orgao: "#2563eb",
    empenho: "#16a34a",
    fornecedor: "#f97316",
    contrato: "#9333ea",
  };

  const TYPE_LABELS = {
    orgao: "Órgãos",
    empenho: "Empenhos",
    fornecedor: "Fornecedores",
    contrato: "Contratos",
    contrato_item: "Itens de contrato",
  };

  const numberFormatter = new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const integerFormatter = new Intl.NumberFormat("pt-BR");

  const state = {
    nodes: [],
    links: [],
    nodesRuntime: [],
    linksRuntime: [],
    nodeSelection: null,
    linkSelection: null,
    adjacency: new Map(),
    selectedId: null,
    hoverId: null,
    svg: null,
    inner: null,
    zoom: null,
    simulation: null,
    selection: {
      node: null,
      link: null,
      label: null,
    },
    dimensions: {
      width: 960,
      height: 640,
    },
    pendingFocusId: null,
    dataSource: "sample",
    anomalyNodes: new Map(),
    availableTypes: [],
    valueExtent: null,
    filters: {
      types: new Set(),
      minValue: null,
      maxValue: null,
      search: "",
      anomaliesOnly: false,
    },
    isLoading: false,
    statusWatcher: {
      timer: null,
      lastStatus: null,
      pendingReload: false,
    },
  };

  const FIELD_LABELS = {
    original_id: "ID original",
    descricao: "Descrição",
    numero: "Número",
    valor: "Valor",
    valor_empenhado: "Valor empenhado",
    data: "Data",
    data_empenho: "Data do empenho",
    documento: "Documento",
    tipo_documento: "Tipo de documento",
    fonte_origem: "Fonte",
    data_ingestao: "Ingestão (UTC)",
    payload_hash: "Hash",
    municipio: "Município",
    uf: "UF",
    sigla: "Sigla",
  };

  const FIELD_HINTS = {
    fonte_origem: "Origem dos dados (api/sample/random/manual).",
    data_ingestao: "Momento em que o payload foi processado (UTC).",
    payload_hash: "Identificador do payload original (uso interno).",
    valor: "Valor associado ao nó (se aplicável).",
    valor_empenhado: "Valor empenhado no registro.",
  };

  function maskDocument(value) {
    if (!value) return value;
    const digits = String(value).replace(/\\D/g, "");
    if (digits.length === 11) {
      return `***.${digits.slice(3, 6)}.***-${digits.slice(-2)}`;
    }
    if (digits.length === 14) {
      return `**.${digits.slice(2, 5)}.***/*${digits.slice(8, 12)}-${digits.slice(-2)}`;
    }
    if (digits.length >= 4) {
      return `${"*".repeat(digits.length - 4)}${digits.slice(-4)}`;
    }
    return "***";
  }

  function formatDate(value) {
    if (!value) return "—";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return String(value);
    }
    return parsed.toLocaleString("pt-BR", { timeZone: "UTC" });
  }

  function formatAttributeValue(key, value) {
    if (value === null || value === undefined || value === "") {
      return "—";
    }
    if (key === "documento") {
      return maskDocument(String(value));
    }
    if (key === "data_ingestao" || key === "data" || key === "data_empenho") {
      return formatDate(value);
    }
    if (key === "valor" || key === "valor_empenhado") {
      return numberFormatter.format(Number(value));
    }
    return Array.isArray(value) ? value.join(" / ") : String(value);
  }

  function renderAttributes(node) {
    const entries = Object.keys(FIELD_LABELS).filter((key) => Object.prototype.hasOwnProperty.call(node, key));
    if (!entries.length) {
      return "<tr><td>Sem atributos adicionais.</td></tr>";
    }
    return entries
      .map((key) => {
        const label = FIELD_LABELS[key] || key;
        const hint = FIELD_HINTS[key] || "";
        const value = formatAttributeValue(key, node[key]);
        return `<tr><th title="${hint}">${label}</th><td>${value}</td></tr>`;
      })
      .join("");
  }

  let searchDebounceHandle = null;

  function typeLabel(type) {
    if (!type) {
      return "Desconhecido";
    }
    return TYPE_LABELS[type] || type.charAt(0).toUpperCase() + type.slice(1);
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

  function toTitle(text) {
    return String(text || "")
      .replace(/_/g, " ")
      .split(" ")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  }

  function setStatus(message, options = {}) {
    if (!elements.status) {
      return;
    }
    const {
      loading = false,
      detail = "",
      action = null,
      variant = "info",
      progress = null,
      progressLabel = "",
    } = options;

    elements.status.hidden = false;
    elements.status.style.display = "block";
    elements.status.classList.remove("graph-status--error", "graph-status--info");
    elements.status.classList.add(
      variant === "error" ? "graph-status--error" : "graph-status--info",
    );
    elements.status.setAttribute("aria-busy", loading ? "true" : "false");

    const hasProgress = Number.isFinite(progress);
    const clampedProgress = hasProgress
      ? Math.max(0, Math.min(100, Number(progress)))
      : null;
    const parts = [];

    if (loading) {
      parts.push('<span class="graph-status__loader" aria-hidden="true"></span>');
    }

    parts.push(`<p class="graph-status__message">${message}</p>`);

    if (detail) {
      parts.push(`<p class="graph-status__detail">${detail}</p>`);
    }

    if (loading) {
      const barClass = hasProgress
        ? "graph-status__bar graph-status__bar--determinate"
        : "graph-status__bar graph-status__bar--indeterminate";
      const ariaAttrs = hasProgress
        ? `role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.round(
            clampedProgress,
          )}"`
        : 'aria-hidden="true"';
      parts.push(`<div class="${barClass}" ${ariaAttrs} data-progress-bar></div>`);
      if (hasProgress) {
        parts.push('<p class="graph-status__progress-label" data-progress-label></p>');
      }
    }

    if (action?.label) {
      parts.push(
        `<button type="button" class="button button--secondary graph-status__action">${action.label}</button>`,
      );
    }

    elements.status.innerHTML = `<div class="graph-status__content">${parts.join("")}</div>`;

    if (loading && hasProgress) {
      const container = elements.status.querySelector("[data-progress-bar]");
      if (container) {
        const fill = document.createElement("span");
        fill.className = "graph-status__bar-fill";
        fill.style.width = `${Math.round(clampedProgress)}%`;
        container.appendChild(fill);
      }
      const labelNode = elements.status.querySelector("[data-progress-label]");
      if (labelNode) {
        const rounded = Math.round(clampedProgress);
        labelNode.textContent = progressLabel
          ? `${rounded}% • ${progressLabel}`
          : `${rounded}%`;
      }
    }

    if (action?.handler) {
      const button = elements.status.querySelector(".graph-status__action");
      if (button) {
        button.addEventListener(
          "click",
          (event) => {
            event.preventDefault();
            action.handler();
          },
          { once: true },
        );
      }
    }
  }

  function clearStatus() {
    if (!elements.status) {
      return;
    }
    elements.status.hidden = true;
    elements.status.style.display = "none";
    elements.status.setAttribute("aria-busy", "false");
  }

  function showError(message, detail = "") {
    setStatus(message, {
      variant: "error",
      detail,
      loading: false,
      action: {
        label: "Tentar novamente",
        handler: () => {
          loadGraph();
        },
      },
    });
  }

  function buildAdjacency(nodes, links) {
    const adjacency = new Map();
    nodes.forEach((node) => {
      adjacency.set(node.id, new Set());
    });
    links.forEach((link) => {
      adjacency.get(link.sourceId)?.add(link.targetId);
      adjacency.get(link.targetId)?.add(link.sourceId);
    });
    state.adjacency = adjacency;
  }

  function normalizeData(raw) {
    const numericValues = [];
    const nodes = (raw.nodes || []).map((node) => {
      const id = String(node.id ?? node.original_id ?? "");
      const type = node.type || "desconhecido";
      const label =
        node.nome || node.descricao || node.numero || node.original_id || id;
      const valor = typeof node.valor === "number" ? node.valor : null;
      if (typeof valor === "number") {
        numericValues.push(valor);
      }
      return {
        ...node,
        id,
        type,
        label,
        layoutX: Number.isFinite(node.x) ? Number(node.x) : null,
        layoutY: Number.isFinite(node.y) ? Number(node.y) : null,
        valor,
      };
    });

    const links = (raw.links || []).map((link, index) => {
      const sourceId = String(link.source);
      const targetId = String(link.target);
      return {
        id: String(link.id ?? `${sourceId}-${targetId}-${index}`),
        source: sourceId,
        target: targetId,
        sourceId,
        targetId,
        tipo: link.tipo || link.key || "relacao",
        valor: typeof link.valor === "number" ? link.valor : null,
      };
    });

    buildAdjacency(nodes, links);
    state.nodes = nodes;
    state.links = links;
    state.availableTypes = Array.from(new Set(nodes.map((node) => node.type).filter(Boolean))).sort();
    state.valueExtent = numericValues.length
      ? [Math.min(...numericValues), Math.max(...numericValues)]
      : null;
  }

  function registerAnomaly(nodeId, detector, severity, reason, score) {
    if (!nodeId) {
      return;
    }
    const id = String(nodeId);
    const normalizedSeverity = ["baixa", "media", "alta"].includes(severity) ? severity : "media";
    const current = state.anomalyNodes.get(id) || {
      severity: "baixa",
      details: [],
    };

    const bestSeverity =
      SEVERITY_RANK[normalizedSeverity] > SEVERITY_RANK[current.severity]
        ? normalizedSeverity
        : current.severity;

    current.severity = bestSeverity;
    current.details.push({
      detector,
      reason,
      severity: normalizedSeverity,
      score,
    });
    state.anomalyNodes.set(id, current);
  }

  async function loadAnomaliesForSource(source) {
    const params = new URLSearchParams();
    if (source) {
      params.set("source", source);
    }

    try {
      const response = await fetch(`/api/anomalies?${params.toString()}`);
      if (!response.ok) {
        throw new Error(`Erro ao carregar anomalias (status ${response.status})`);
      }
      const payload = await response.json();
      state.anomalyNodes = new Map();

      Object.entries(payload || {}).forEach(([detectorKey, items]) => {
        (items || []).forEach((item) => {
          const severity = item.severity || "media";
          const reason = item.reason || detectorKey;
          const score = typeof item.score === "number" ? item.score : null;
          const context = item.context || {};

          const track = (candidateId) => {
            if (!candidateId) {
              return;
            }
            registerAnomaly(candidateId, detectorKey, severity, reason, score);
          };

          if (context.node_id) {
            track(context.node_id);
          }
          if (Array.isArray(context.sample_nodes)) {
            context.sample_nodes.forEach(track);
          }
          if (Array.isArray(context.nodes)) {
            context.nodes.forEach(track);
          }
          if (context.subgraph && Array.isArray(context.subgraph.nodes)) {
            context.subgraph.nodes.forEach((node) => {
              track(node?.id);
            });
          }
        });
      });
    } catch (error) {
      console.warn("Falha ao carregar anomalias para o grafo:", error);
      state.anomalyNodes = new Map();
    }
  }

  function annotateNodesWithAnomalies() {
    state.nodes = state.nodes.map((node) => {
      const anomaly = state.anomalyNodes.get(node.id);
      return {
        ...node,
        isAnomalous: Boolean(anomaly),
        anomalySeverity: anomaly?.severity ?? null,
        anomalyReasons: anomaly?.details ?? [],
      };
    });
  }

  function populateTypeFilters() {
    if (!elements.filterTypeList) {
      return;
    }
    const fragment = document.createDocumentFragment();
    state.filters.types = new Set(state.availableTypes);

    state.availableTypes.forEach((type) => {
      const wrapper = document.createElement("label");
      wrapper.className = "filter-checkbox";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.value = type;
      input.checked = true;
      input.dataset.type = type;
      input.addEventListener("change", handleTypeFilterChange);

      const span = document.createElement("span");
      span.textContent = typeLabel(type);

      wrapper.appendChild(input);
      wrapper.appendChild(span);
      fragment.appendChild(wrapper);
    });

    elements.filterTypeList.innerHTML = "";
    elements.filterTypeList.appendChild(fragment);
  }

  function updateValuePlaceholders() {
    if (!state.valueExtent) {
      if (elements.filterValueMin) {
        elements.filterValueMin.placeholder = "—";
      }
      if (elements.filterValueMax) {
        elements.filterValueMax.placeholder = "—";
      }
      return;
    }
    const [minValue, maxValue] = state.valueExtent;
    if (elements.filterValueMin) {
      elements.filterValueMin.placeholder = numberFormatter.format(minValue);
    }
    if (elements.filterValueMax) {
      elements.filterValueMax.placeholder = numberFormatter.format(maxValue);
    }
  }

  function initSvg() {
    const rect = elements.canvas.getBoundingClientRect();
    const width = rect.width || 960;
    const height = rect.height || 640;
    state.dimensions = { width, height };

    const statusNode = elements.status;
    elements.canvas.innerHTML = "";
    const svg = d3
      .select(elements.canvas)
      .append("svg")
      .attr("class", "graph-svg")
      .attr("width", width)
      .attr("height", height);

    const inner = svg.append("g").attr("class", "graph-inner");

    if (statusNode) {
      elements.canvas.appendChild(statusNode);
    }

    const zoom = d3
      .zoom()
      .scaleExtent([0.25, 4])
      .on("zoom", (event) => {
        inner.attr("transform", event.transform);
      });

    svg.call(zoom);

    state.svg = svg;
    state.inner = inner;
    state.zoom = zoom;

    state.selection.link = inner.append("g").attr("class", "link-layer");
    state.selection.node = inner.append("g").attr("class", "node-layer");
    state.selection.label = inner.append("g").attr("class", "label-layer");
  }

  function applyInitialPositions(nodes) {
    const xs = nodes.map((node) => node.layoutX).filter((value) => value !== null);
    const ys = nodes.map((node) => node.layoutY).filter((value) => value !== null);

    if (!xs.length || !ys.length) {
      return;
    }

    const xScale = d3
      .scaleLinear()
      .domain(d3.extent(xs))
      .range([80, state.dimensions.width - 80]);
    const yScale = d3
      .scaleLinear()
      .domain(d3.extent(ys))
      .range([80, state.dimensions.height - 80]);

    nodes.forEach((node) => {
      if (node.layoutX !== null && node.layoutY !== null) {
        node.x = xScale(node.layoutX);
        node.y = yScale(node.layoutY);
      }
    });
  }

  function nodeClassName(node) {
    const classes = [`node`, `node--${node.type}`];
    if (node.isAnomalous) {
      classes.push("node--anomaly");
    }
    if (node.anomalySeverity) {
      classes.push(`node--severity-${node.anomalySeverity}`);
    }
    return classes.join(" ");
  }

  function renderGraph() {
    const nodes = state.nodes.map((node) => ({ ...node }));
    const links = state.links.map((link) => ({ ...link }));

    applyInitialPositions(nodes);

    const linkSelection = state.selection.link
      .selectAll("line")
      .data(links, (link) => link.id)
      .join(
        (enter) =>
          enter
            .append("line")
            .attr("class", "link"),
        (update) => update,
        (exit) => exit.remove(),
      );

    const nodeSelection = state.selection.node
      .selectAll("g")
      .data(nodes, (node) => node.id)
      .join(
        (enter) => {
          const group = enter.append("g");

          group
            .append("circle")
            .attr("r", (node) => (node.type === "empenho" ? 14 : 10))
            .attr("fill", (node) => TYPE_COLORS[node.type] || "#64748b")
            .attr("stroke", "#fff")
            .attr("stroke-width", 2);

          group
            .append("text")
            .attr("class", "node__label")
            .attr("dy", (node) => (node.type === "empenho" ? 24 : 20))
            .text((node) => node.label);

          group
            .on("mouseenter", (_, node) => {
              state.hoverId = node.id;
              updateInteractionClasses();
            })
            .on("mouseleave", () => {
              state.hoverId = null;
              updateInteractionClasses();
            })
            .on("click", (_, node) => {
              selectNode(node.id);
            });

          return group;
        },
        (update) => update,
        (exit) => exit.remove(),
      )
      .attr("class", (node) => nodeClassName(node));

    const simulation = d3
      .forceSimulation(nodes)
      .force(
        "link",
        d3
          .forceLink(links)
          .id((node) => node.id)
          .distance((link) => (link.tipo === "empenho_contrato" ? 80 : 120))
          .strength(0.15),
      )
      .force("charge", d3.forceManyBody().strength(-320))
      .force(
        "center",
        d3.forceCenter(state.dimensions.width / 2, state.dimensions.height / 2),
      )
      .force(
        "collision",
        d3.forceCollide().radius((node) => (node.type === "empenho" ? 36 : 24)),
      )
      .on("tick", () => {
        linkSelection
          .attr("x1", (link) => link.source.x)
          .attr("y1", (link) => link.source.y)
          .attr("x2", (link) => link.target.x)
          .attr("y2", (link) => link.target.y);

        nodeSelection.attr("transform", (node) => `translate(${node.x}, ${node.y})`);
      })
      .on("end", () => {
        if (state.pendingFocusId) {
          const target = nodes.find((node) => node.id === state.pendingFocusId);
          if (target) {
            selectNode(target.id, { center: true, force: true });
            state.pendingFocusId = null;
            return;
          }
        }
        fitGraph();
      });

    state.simulation = simulation;
    state.nodeSelection = nodeSelection;
    state.linkSelection = linkSelection;
    state.nodesRuntime = nodes;
    state.linksRuntime = links;
  }

  function isNeighbor(candidateId, targetId) {
    if (!targetId || candidateId === targetId) {
      return false;
    }
    return state.adjacency.get(targetId)?.has(candidateId) ?? false;
  }

  function resolveNode(candidate) {
    if (!candidate) {
      return null;
    }
    if (typeof candidate === "object" && candidate.id) {
      return candidate;
    }
    return state.nodesRuntime?.find((node) => node.id === candidate) ?? null;
  }

  function passesFilters(node) {
    if (!node) {
      return false;
    }

    if (state.filters.types.size && !state.filters.types.has(node.type)) {
      return false;
    }

    const valor = typeof node.valor === "number" ? node.valor : null;
    if (state.filters.minValue !== null) {
      if (valor === null || valor < state.filters.minValue) {
        return false;
      }
    }

    if (state.filters.maxValue !== null) {
      if (valor === null || valor > state.filters.maxValue) {
        return false;
      }
    }

    if (state.filters.anomaliesOnly && !node.isAnomalous) {
      return false;
    }

    if (state.filters.search) {
      const needle = state.filters.search;
      const haystack = [
        node.label,
        node.nome,
        node.descricao,
        node.numero,
        node.documento,
        node.original_id,
      ]
        .filter(Boolean)
        .map((value) => String(value).toLowerCase());
      const matches = haystack.some((value) => value.includes(needle));
      if (!matches) {
        return false;
      }
    }

    return true;
  }

  function ensureSelectionVisibility() {
    if (!state.selectedId || !state.nodesRuntime) {
      return;
    }
    const selectedNode = state.nodesRuntime.find((node) => node.id === state.selectedId);
    if (!selectedNode || !passesFilters(selectedNode)) {
      state.selectedId = null;
      updateSidebar(null);
    }
  }

  function updateInteractionClasses() {
    const selectedId = state.selectedId;
    const hoverId = state.hoverId;

    if (state.nodeSelection) {
      state.nodeSelection
        .attr("class", (node) => nodeClassName(node))
        .classed("is-selected", (node) => node.id === selectedId)
        .classed(
          "is-dimmed",
          (node) =>
            Boolean(selectedId) &&
            node.id !== selectedId &&
            !isNeighbor(node.id, selectedId) &&
            (!hoverId || hoverId === selectedId),
        )
        .classed(
          "is-hovered",
          (node) => Boolean(hoverId) && (node.id === hoverId || isNeighbor(node.id, hoverId)),
        )
        .classed("is-hidden", (node) => !passesFilters(node));
    }

    if (state.linkSelection) {
      state.linkSelection
        .classed(
          "is-highlighted",
          (link) =>
            link.sourceId === selectedId ||
            link.targetId === selectedId ||
            link.sourceId === hoverId ||
            link.targetId === hoverId,
        )
        .classed(
          "is-hidden",
          (link) => {
            const sourceNode = resolveNode(link.source);
            const targetNode = resolveNode(link.target);
            return !passesFilters(sourceNode) || !passesFilters(targetNode);
          },
        );
    }
  }

  function selectNode(nodeId, options = {}) {
    if (!nodeId) {
      state.selectedId = null;
      updateSidebar(null);
      updateInteractionClasses();
      return;
    }

    const node = state.nodesRuntime?.find((item) => item.id === nodeId) ?? null;
    if (!node) {
      state.selectedId = null;
      updateSidebar(null);
      updateInteractionClasses();
      return;
    }

    if (!passesFilters(node)) {
      state.selectedId = null;
      updateSidebar(null);
      updateInteractionClasses();
      return;
    }

    const forceSelection = Boolean(options.force);
    if (!forceSelection && state.selectedId === node.id) {
      state.selectedId = null;
      updateSidebar(null);
      updateInteractionClasses();
      return;
    }

    state.selectedId = node.id;
    updateSidebar(node);
    updateInteractionClasses();

    if (options.center) {
      centerOnNode(node);
    }
  }

  function renderAnomalySection(node) {
    if (!node.isAnomalous || !node.anomalyReasons?.length) {
      return "";
    }

    const items = node.anomalyReasons
      .map((entry) => {
        const severity = severityLabel(entry.severity);
        const score = typeof entry.score === "number" ? numberFormatter.format(entry.score) : "—";
        return `
          <li>
            <header>
              <span class="badge-severity badge-severity--${entry.severity}">${severity}</span>
              <strong>${toTitle(entry.detector)}</strong>
            </header>
            <p>${entry.reason || "Sem descrição detalhada."}</p>
            <small>Pontuação: ${score}</small>
          </li>
        `;
      })
      .join("");

    const severityText = severityLabel(node.anomalySeverity);

    return `
      <section>
        <h4>Anomalias</h4>
        <p class="sidebar__highlight sidebar__highlight--${node.anomalySeverity}">
          Severidade ${severityText} – ${integerFormatter.format(node.anomalyReasons.length)} ocorrência(s)
        </p>
        <ul class="anomaly-detail-list">${items}</ul>
      </section>
    `;
  }

  function updateSidebar(node) {
    if (!elements.sidebar) {
      return;
    }

    if (!node) {
      elements.sidebar.innerHTML = "<p>Selecione um nó para ver detalhes e conexões.</p>";
      return;
    }

    const neighbors = Array.from(state.adjacency.get(node.id) ?? []);

    const neighborItems = neighbors
      .map((neighborId) => {
        const neighbor = state.nodesRuntime?.find((item) => item.id === neighborId);
        if (!neighbor) {
          return null;
        }
        return `<li>
          <span>↔</span> <strong>${neighbor.label}</strong>
          <small class="badge-type">${typeLabel(neighbor.type)}</small>
        </li>`;
      })
      .filter(Boolean)
      .join("");

    const attributes = Object.entries(node)
      .filter(([key]) => !["index", "vx", "vy", "x", "y", "label", "layoutX", "layoutY", "isAnomalous", "anomalyReasons", "anomalySeverity"].includes(key));

    const metaInfo = [];
    if (node.fonte_origem) {
      metaInfo.push(`Fonte: ${node.fonte_origem}`);
    }
    if (node.data_ingestao) {
      metaInfo.push(`Ingestão: ${formatDate(node.data_ingestao)}`);
    }

    elements.sidebar.innerHTML = `
      <article class="sidebar-card">
        <header>
          <h3>${node.label}</h3>
          <p class="badge-type">${typeLabel(node.type)}</p>
          ${metaInfo.length ? `<p class="sidebar-meta">${metaInfo.join(" · ")}</p>` : ""}
        </header>
        ${renderAnomalySection(node)}
        <section>
          <h4>Atributos</h4>
          <table>${renderAttributes(node)}</table>
        </section>
        <section>
          <h4>Conexões</h4>
          <ul class="neighbor-list">
            ${neighborItems || "<li>Nenhuma conexão relacionada.</li>"}
          </ul>
        </section>
      </article>
    `;
  }

  function updateFilterSummary() {
    if (!elements.filterSummary || !state.nodesRuntime?.length) {
      return;
    }
    const visible = state.nodesRuntime.filter((node) => passesFilters(node)).length;
    const anomalyCount = state.nodesRuntime.filter(
      (node) => node.isAnomalous && passesFilters(node),
    ).length;

    const base = `${integerFormatter.format(visible)} de ${integerFormatter.format(state.nodes.length)} nós visíveis`;
    const anomalyText = anomalyCount ? ` – ${integerFormatter.format(anomalyCount)} com anomalias` : "";
    elements.filterSummary.textContent = `${base}${anomalyText}`;
  }

  function applyFilters() {
    ensureSelectionVisibility();
    updateInteractionClasses();
    updateFilterSummary();
  }

  function centerOnNode(node) {
    if (!state.svg || !state.zoom) {
      return;
    }
    const { width, height } = state.dimensions;
    const scale = 1.5;
    const translateX = width / 2 - node.x * scale;
    const translateY = height / 2 - node.y * scale;
    state.svg
      .transition()
      .duration(600)
      .call(state.zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(scale));
  }

  function fitGraph() {
    if (!state.nodesRuntime?.length || !state.svg || !state.zoom) {
      return;
    }
    const { width, height } = state.dimensions;
    const xExtent = d3.extent(state.nodesRuntime, (node) => node.x);
    const yExtent = d3.extent(state.nodesRuntime, (node) => node.y);
    const graphWidth = xExtent[1] - xExtent[0];
    const graphHeight = yExtent[1] - yExtent[0];
    const scale = 0.85 / Math.max(graphWidth / width, graphHeight / height);
    const translateX = width / 2 - ((xExtent[0] + xExtent[1]) / 2) * scale;
    const translateY = height / 2 - ((yExtent[0] + yExtent[1]) / 2) * scale;

    state.svg
      .transition()
      .duration(600)
      .call(state.zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(scale));
  }

  function parseFilterValue(value) {
    if (value === "" || value === null || value === undefined) {
      return null;
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return null;
    }
    return numeric;
  }

  function handleTypeFilterChange(event) {
    const input = event.currentTarget;
    if (!(input instanceof HTMLInputElement)) {
      return;
    }
    const type = input.dataset.type;
    if (!type) {
      return;
    }
    if (input.checked) {
      state.filters.types.add(type);
    } else {
      state.filters.types.delete(type);
    }
    applyFilters();
  }

  function handleValueChange() {
    state.filters.minValue = parseFilterValue(elements.filterValueMin?.value ?? null);
    state.filters.maxValue = parseFilterValue(elements.filterValueMax?.value ?? null);
    applyFilters();
  }

  function handleSearchChange(event) {
    const value = event.target.value || "";
    const normalized = value.trim().toLowerCase();
    window.clearTimeout(searchDebounceHandle);
    searchDebounceHandle = window.setTimeout(() => {
      state.filters.search = normalized;
      applyFilters();
    }, 150);
  }

  function handleAnomalyToggle(event) {
    state.filters.anomaliesOnly = Boolean(event.target.checked);
    applyFilters();
  }

  function clearFilters() {
    state.filters.types = new Set(state.availableTypes);
    state.filters.minValue = null;
    state.filters.maxValue = null;
    state.filters.search = "";
    state.filters.anomaliesOnly = false;

    if (elements.filterTypeList) {
      elements.filterTypeList.querySelectorAll("input[type='checkbox']").forEach((input) => {
        input.checked = true;
      });
    }
    if (elements.filterValueMin) {
      elements.filterValueMin.value = "";
    }
    if (elements.filterValueMax) {
      elements.filterValueMax.value = "";
    }
    if (elements.filterSearch) {
      elements.filterSearch.value = "";
    }
    if (elements.filterAnomalies) {
      elements.filterAnomalies.checked = false;
    }
    applyFilters();
  }

  function startFetchStatusTracking(detail, { onProgress } = {}) {
    let active = true;

    const wait = (duration) =>
      new Promise((resolve) => {
        window.setTimeout(resolve, duration);
      });

    const poll = async () => {
      while (active) {
        try {
          const response = await fetch("/api/graph/fetch/status", { cache: "no-store" });
          if (!response.ok) {
            throw new Error("Status endpoint unavailable");
          }
          const statusPayload = await response.json();
          if (!active) {
            return;
          }
          const rawProgress = Number(statusPayload?.progress ?? NaN);
          const hasProgress = Number.isFinite(rawProgress);
          const normalizedProgress = hasProgress
            ? Math.max(0, Math.min(100, rawProgress))
            : null;
          const message =
            typeof statusPayload?.message === "string" && statusPayload.message.trim()
              ? statusPayload.message.trim()
              : "";

          if (hasProgress) {
            onProgress?.(normalizedProgress);
          }

          setStatus("Carregando dados do grafo…", {
            loading: true,
            detail,
            progress: hasProgress ? normalizedProgress : null,
            progressLabel: message,
          });

          const statusValue = String(statusPayload?.status || "").toLowerCase();
          if (statusValue === "completed" || statusValue === "failed") {
            return;
          }
        } catch (error) {
          if (!active) {
            return;
          }
        }

        await wait(1250);
      }
    };

    poll();

    return () => {
      active = false;
    };
  }

  async function loadGraph(options = {}) {
    const { reason = "manual" } = options;
    let slowWarningTimer = null;
    let timeoutId = null;
    let stopTracking = null;

    try {
      state.isLoading = true;
      const source = state.dataSource;
      const params = new URLSearchParams();
      if (source) {
        params.set("source", source);
      }

      const loadingMessages = {
        sample: {
          detail: "Carregando conjunto de demonstração incluído no projeto.",
        },
        random: {
          detail: "Gerando grafo sintético para explorar rapidamente a interface.",
        },
        api: {
          detail: "Buscando dados atualizados no portal do TCE-RJ.",
        },
      };

      const { detail } = loadingMessages[source] || loadingMessages.sample;

      const statusMessage =
        reason === "auto" && source === "api"
          ? "Atualizando grafo com dados mais recentes…"
          : "Carregando dados do grafo…";

      setStatus(statusMessage, {
        loading: true,
        detail,
      });

      if (source === "api") {
        slowWarningTimer = window.setTimeout(() => {
          setStatus("Aguarde, ainda estamos montando o grafo…", {
            loading: true,
            detail:
              "A primeira carga pode levar alguns minutos enquanto processamos milhares de empenhos. Assim que concluir, o resultado fica em cache para as próximas visitas.",
          });
        }, 6000);

        stopTracking = startFetchStatusTracking(detail, {
          onProgress(progressValue) {
            if (progressValue > 0 && slowWarningTimer) {
              window.clearTimeout(slowWarningTimer);
              slowWarningTimer = null;
            }
          },
        });
      }

      const controller = new AbortController();
      timeoutId = window.setTimeout(() => controller.abort(), 60000);

      const graphResponse = await fetch(`/api/graph/snapshot?${params.toString()}`, {
        signal: controller.signal,
      });

      window.clearTimeout(timeoutId);
      timeoutId = null;

      if (!graphResponse.ok) {
        throw new Error(`Não foi possível carregar o grafo (status ${graphResponse.status}).`);
      }
      const payload = await graphResponse.json();
      if (!payload.nodes || !payload.nodes.length) {
        throw new Error("Nenhum nó disponível para exibição.");
      }

      normalizeData(payload);
      populateTypeFilters();
      updateValuePlaceholders();
      await loadAnomaliesForSource(source);
      annotateNodesWithAnomalies();

      initSvg();
      renderGraph();
      clearFilters();
      if (stopTracking) {
        stopTracking();
        stopTracking = null;
      }
      clearStatus();
    } catch (error) {
      console.error(error);
      if (stopTracking) {
        stopTracking();
        stopTracking = null;
      }
      if (error && error.name === "AbortError") {
        const isApi = state.dataSource === "api";
        showError(
          "Tempo limite ao carregar o grafo.",
          isApi
            ? "Tente novamente ou reduza o volume de dados ajustando os parâmetros TCE_API_YEARS / TCE_API_MAX_RECORDS."
            : "Tente novamente; o carregamento rápido deve concluir em instantes.",
        );
      } else {
        showError(error.message || "Erro inesperado ao carregar o grafo.");
      }
    } finally {
      if (slowWarningTimer) {
        window.clearTimeout(slowWarningTimer);
      }
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
      if (stopTracking) {
        stopTracking();
      }
      state.isLoading = false;
      if (state.statusWatcher.pendingReload && state.dataSource === "api" && reason !== "auto") {
        state.statusWatcher.pendingReload = false;
        window.setTimeout(() => loadGraph({ reason: "auto" }), 0);
      }
    }
  }

  function startBackgroundStatusWatcher() {
    if (state.statusWatcher.timer) {
      return;
    }

    const poll = async () => {
      try {
        const response = await fetch("/api/graph/fetch/status", { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Status HTTP ${response.status}`);
        }
        const payload = await response.json();
        const newStatus = String(payload?.status || "").toLowerCase();

        if (state.statusWatcher.lastStatus !== newStatus) {
          const previousStatus = state.statusWatcher.lastStatus;
          state.statusWatcher.lastStatus = newStatus;

          if (newStatus === "completed") {
            if (state.dataSource === "api") {
              if (state.isLoading) {
                state.statusWatcher.pendingReload = true;
              } else {
                loadGraph({ reason: "auto" });
              }
            } else {
              setStatus("Dados atualizados disponíveis.", {
                loading: false,
                detail: "Selecione a origem 'API' para visualizar o grafo mais recente.",
              });
            }
          } else if (previousStatus === "completed" && newStatus === "running" && state.dataSource === "api") {
            setStatus("Carregando dados do grafo…", {
              loading: true,
              detail: "Buscando dados atualizados no portal do TCE-RJ.",
            });
          }
        }
      } catch (error) {
        // Falha na checagem do status não deve interromper o polling; apenas registra no console.
        console.debug("Falha ao consultar status do backend:", error);
      }
    };

    state.statusWatcher.timer = window.setInterval(poll, 5000);
    poll();
  }

  function bootstrap() {
    const params = new URLSearchParams(window.location.search);
    const focusId = params.get("node");
    if (focusId) {
      state.pendingFocusId = focusId;
    }

    const availableSources = ["sample", "random", "api"];
    const requestedSource = params.get("source");
    if (requestedSource && availableSources.includes(requestedSource)) {
      state.dataSource = requestedSource;
    }

    if (elements.fit) {
      elements.fit.addEventListener("click", () => {
        fitGraph();
      });
    }

    if (elements.filterValueMin) {
      elements.filterValueMin.addEventListener("change", handleValueChange);
      elements.filterValueMin.addEventListener("blur", handleValueChange);
    }
    if (elements.filterValueMax) {
      elements.filterValueMax.addEventListener("change", handleValueChange);
      elements.filterValueMax.addEventListener("blur", handleValueChange);
    }
    if (elements.filterSearch) {
      elements.filterSearch.addEventListener("input", handleSearchChange);
    }
    if (elements.filterAnomalies) {
      elements.filterAnomalies.addEventListener("change", handleAnomalyToggle);
    }
    if (elements.filterClear) {
      elements.filterClear.addEventListener("click", clearFilters);
    }

    if (elements.dataSource) {
      const initialSource = availableSources.includes(state.dataSource)
        ? state.dataSource
        : elements.dataSource.value;
      state.dataSource = initialSource;
      elements.dataSource.value = initialSource;

      elements.dataSource.addEventListener("change", (event) => {
        const value = event.target.value;
        state.dataSource = availableSources.includes(value) ? value : "sample";
        const urlParams = new URLSearchParams(window.location.search);
        urlParams.set("source", state.dataSource);
        const newUrl = `${window.location.pathname}?${urlParams.toString()}`;
        window.history.replaceState({}, "", newUrl);
        loadGraph();
      });
    }

    loadGraph();
    startBackgroundStatusWatcher();
  }

  bootstrap();
})();
