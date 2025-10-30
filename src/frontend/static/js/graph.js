(() => {
  const elements = {
    canvas: document.getElementById("graph-canvas"),
    status: document.getElementById("graph-status"),
    sidebar: document.getElementById("sidebar-content"),
    fit: document.getElementById("fit-graph"),
    typeFilter: document.getElementById("type-filter"),
    dataSource: document.getElementById("data-source"),
  };

  if (!elements.canvas) {
    return;
  }

  const state = {
    nodes: [],
    links: [],
    adjacency: new Map(),
    selectedId: null,
    hoverId: null,
    typeFilter: "all",
    svg: null,
    inner: null,
    zoom: null,
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
  };

  const TYPE_COLORS = {
    orgao: "#2563eb",
    empenho: "#16a34a",
    fornecedor: "#f97316",
    contrato: "#9333ea",
  };

  const numberFormatter = new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  function setStatus(message, options = {}) {
    if (!elements.status) {
      return;
    }
    const { loading = false, detail = "", action = null, variant = "info" } = options;

    elements.status.hidden = false;
    elements.status.style.display = "block";
    elements.status.classList.remove("graph-status--error");
    elements.status.classList.remove("graph-status--info");
    elements.status.classList.add(
      variant === "error" ? "graph-status--error" : "graph-status--info",
    );
    elements.status.setAttribute("aria-busy", loading ? "true" : "false");

    const parts = [];

    if (loading) {
      parts.push('<span class="graph-status__loader" aria-hidden="true"></span>');
    }

    parts.push(`<p class="graph-status__message">${message}</p>`);

    if (detail) {
      parts.push(`<p class="graph-status__detail">${detail}</p>`);
    }

    if (loading) {
      parts.push('<div class="graph-status__bar" role="progressbar" aria-hidden="true"></div>');
    }

    if (action?.label) {
      parts.push(
        `<button type="button" class="button button--secondary graph-status__action">${action.label}</button>`,
      );
    }

    elements.status.innerHTML = `<div class="graph-status__content">${parts.join("")}</div>`;

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
    const nodes = (raw.nodes || []).map((node) => {
      const id = String(node.id ?? node.original_id ?? "");
      const type = node.type || "desconhecido";
      const label =
        node.nome ||
        node.descricao ||
        node.numero ||
        node.original_id ||
        id;
      return {
        ...node,
        id,
        type,
        label,
        layoutX: Number.isFinite(node.x) ? Number(node.x) : null,
        layoutY: Number.isFinite(node.y) ? Number(node.y) : null,
        valor: typeof node.valor === "number" ? node.valor : null,
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
        valor: link.valor ?? null,
      };
    });

    buildAdjacency(nodes, links);
    state.nodes = nodes;
    state.links = links;
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

  function renderGraph() {
    const nodes = state.nodes.map((node) => ({ ...node }));
    const links = state.links.map((link) => ({ ...link }));

    applyInitialPositions(nodes);

    const linkSelection = state.selection.link
      .selectAll("line")
      .data(links, (link) => link.id)
      .join("line")
      .attr("class", "link");

    const nodeSelection = state.selection.node
      .selectAll("g")
      .data(nodes, (node) => node.id)
      .join((enter) => {
        const group = enter
          .append("g")
          .attr("class", (node) => `node node--${node.type}`);

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
      });

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
      .force("center", d3.forceCenter(state.dimensions.width / 2, state.dimensions.height / 2))
      .force("collision", d3.forceCollide().radius((node) => (node.type === "empenho" ? 36 : 24)))
      .on("tick", () => {
        linkSelection
          .attr("x1", (link) => link.source.x)
          .attr("y1", (link) => link.source.y)
          .attr("x2", (link) => link.target.x)
          .attr("y2", (link) => link.target.y);

        nodeSelection.attr("transform", (node) => `translate(${node.x}, ${node.y})`);
      })
      .on("end", () => {
        if (state.pendingFocusId && state.typeFilter === "all") {
          const target = nodes.find((node) => node.id === state.pendingFocusId);
          if (target) {
            selectNode(target.id, { center: true });
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

  function isNodeVisible(node) {
    return state.typeFilter === "all" || node.type === state.typeFilter;
  }

  function updateInteractionClasses() {
    const selectedId = state.selectedId;
    const hoverId = state.hoverId;

    if (state.nodeSelection) {
      state.nodeSelection
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
          (node) =>
            Boolean(hoverId) && (node.id === hoverId || isNeighbor(node.id, hoverId)),
        )
        .classed("is-hidden", (node) => !isNodeVisible(node));
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
        .classed("is-hidden", (link) => isNodeFiltered(link.source) || isNodeFiltered(link.target));
    }
  }

  function selectNode(nodeId, options = {}) {
    const node = state.nodesRuntime?.find((item) => item.id === nodeId) ?? null;
    if (!node) {
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
          <small class="badge-type">${neighbor.type}</small>
        </li>`;
      })
      .filter(Boolean)
      .join("");

    const attributes = Object.entries(node)
      .filter(([key]) => !["index", "vx", "vy", "x", "y", "label", "layoutX", "layoutY"].includes(key))
      .map(([key, value]) => {
        if (value === null || value === undefined || value === "") {
          return null;
        }
        if (typeof value === "number") {
          return `<tr><th>${key}</th><td>${numberFormatter.format(value)}</td></tr>`;
        }
        if (Array.isArray(value)) {
          return `<tr><th>${key}</th><td>${value.join(" / ")}</td></tr>`;
        }
        if (typeof value === "object") {
          return `<tr><th>${key}</th><td>${JSON.stringify(value)}</td></tr>`;
        }
        return `<tr><th>${key}</th><td>${value}</td></tr>`;
      })
      .filter(Boolean)
      .join("");

    elements.sidebar.innerHTML = `
      <article class="sidebar-card">
        <h3>${node.label}</h3>
        <p class="badge-type">${node.type}</p>
        <section>
          <h4>Atributos</h4>
          <table>${attributes || "<tr><td>Sem atributos adicionais.</td></tr>"}</table>
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

  function isNodeFiltered(node) {
    return state.typeFilter !== "all" && node.type !== state.typeFilter;
  }

  function applyTypeFilter(value) {
    state.typeFilter = value;
    if (state.selectedId) {
      const selectedNode = state.nodesRuntime?.find((node) => node.id === state.selectedId);
      if (selectedNode && isNodeFiltered(selectedNode)) {
        state.selectedId = null;
        updateSidebar(null);
      }
    }
    updateInteractionClasses();
  }

  function updateLinkVisibility() {
    if (!state.linkSelection) {
      return;
    }
    state.linkSelection.classed("is-hidden", (link) => {
      const sourceType = link.source.type ?? state.nodesRuntime?.find((n) => n.id === link.sourceId)?.type;
      const targetType = link.target.type ?? state.nodesRuntime?.find((n) => n.id === link.targetId)?.type;
      const sourceHidden = state.typeFilter !== "all" && sourceType !== state.typeFilter;
      const targetHidden = state.typeFilter !== "all" && targetType !== state.typeFilter;
      return sourceHidden || targetHidden;
    });
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

  async function loadGraph() {
    let slowWarningTimer = null;
    let timeoutId = null;

    try {
      const source = state.dataSource || "sample";
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

      setStatus("Carregando dados do grafo…", {
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
      }

      const controller = new AbortController();
      timeoutId = window.setTimeout(() => controller.abort(), 60000);

      const params = new URLSearchParams();
      if (source) {
        params.set("source", source);
      }
      const url = `/api/graph/snapshot?${params.toString()}`;

      const response = await fetch(url, {
        signal: controller.signal,
      });

      window.clearTimeout(timeoutId);
      timeoutId = null;
      if (!response.ok) {
        throw new Error(`Não foi possível carregar o grafo (status ${response.status}).`);
      }
      const payload = await response.json();
      if (!payload.nodes || !payload.nodes.length) {
        throw new Error("Nenhum nó disponível para exibição.");
      }

      normalizeData(payload);
      initSvg();
      renderGraph();
      clearStatus();
    } catch (error) {
      console.error(error);
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
        slowWarningTimer = null;
      }
      if (timeoutId) {
        window.clearTimeout(timeoutId);
        timeoutId = null;
      }
    }
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

    if (elements.typeFilter) {
      elements.typeFilter.addEventListener("change", (event) => {
        applyTypeFilter(event.target.value);
      });
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
  }

  bootstrap();
})();
