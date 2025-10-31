from src.analysis.detectors import detect_isolated_communities
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO
from src.backend.services.sample_data import SAMPLE_PAYLOAD
from src.db.repository import GraphRepository

repo = GraphRepository()
graph, graph_data = repo.load_graph(payloads=SAMPLE_PAYLOAD)

communities = detect_isolated_communities(graph, NODE_ORGAO, NODE_FORNECEDOR)
print("comunidades:", len(communities))
for item in communities:
    print(item.reason, item.context.get("node_count"),
item.context.get("fallback_selected"))