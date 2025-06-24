#!/usr/bin/env python3
"""
Layered NetworkX viewer for City-Clerk graphs.
• Groups nodes by document-type tier.
• Shows only STRUCTURAL / CROSS_REF edges by default.
• Lets you press <E> to toggle MENTION edges on/off interactively.
"""

import pickle, json, math, networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path

DOC_LAYERS = {
    "meeting": 0,
    "agenda_item": 1,
    "ordinance": 2,
    "resolution": 2,
    "person": 3,
    "organization": 3,
    "project": 4,
    "document_number": 4,
    "cross_reference": 4,
    "money": 4,
}

COLOR = {
    "meeting":        "#2E8B57",
    "agenda_item":    "#1E90FF",
    "ordinance":      "#DC143C", 
    "resolution":     "#DC143C",
    "person":         "#FF8C00",
    "organization":   "#9A3412",
    "project":        "#7C3AED",
    "document_number":"#059669",
    "cross_reference":"#0891B2",
    "money":          "#CA8A04",
    "other":          "#6B7280",
}

def doc_layer(node_data):
    # Use the actual "type" attribute that exists in GraphRAG nodes
    typ = (node_data.get("type") or "").lower()
    return DOC_LAYERS.get(typ, max(DOC_LAYERS.values())+1)

def load_graph() -> nx.MultiDiGraph:
    pkl = Path("local_graph_data/city_clerk_graph.pkl")
    if pkl.exists():
        with open(pkl, "rb") as fh:
            full_graph = pickle.load(fh)
            
        # Filter to only show connected nodes
        connected_nodes = set()
        for src, dst in full_graph.edges():
            connected_nodes.add(src)
            connected_nodes.add(dst)
            
        # Create subgraph with only connected nodes
        connected_graph = full_graph.subgraph(connected_nodes).copy()
        print(f"📊 Filtered to {connected_graph.number_of_nodes()} connected nodes (was {full_graph.number_of_nodes()})")
        print(f"📊 {connected_graph.number_of_edges()} edges retained")
        
        return connected_graph
        
    json_path = Path("local_graph_data/city_clerk_graph.json")
    if json_path.exists():
        with open(json_path) as fh:
            data = json.load(fh)
        G = nx.MultiDiGraph()
        for n in data["nodes"]:
            G.add_node(n["id"], **n)
        for e in data["links"]:
            G.add_edge(e["source"], e["target"], **e)
        return G
    raise SystemExit("❌ graph file not found – run builder stage first")

def layered_layout(G):
    """Improved vertical layering with better spacing."""
    pos = {}
    by_layer = {}
    for nid, data in G.nodes(data=True):
        layer = doc_layer(data)
        by_layer.setdefault(layer, []).append(nid)

    if not by_layer:
        return pos
        
    # Better spacing for each layer
    for layer, nodes in by_layer.items():
        num_nodes = len(nodes)
        if num_nodes == 1:
            pos[nodes[0]] = (0.5, -layer * 2)  # Center single nodes, more vertical space
        else:
            # Spread nodes across the width with padding
            for idx, nid in enumerate(sorted(nodes)):
                x_pos = (idx / (num_nodes - 1)) * 0.8 + 0.1  # Use 80% of width with 10% padding
                pos[nid] = (x_pos, -layer * 2)  # More vertical spacing between layers
    return pos

def build_plot(G, include_mentions=False):
    pos = layered_layout(G)

    # node styling
    ncolor, nsize = [], []
    for nid, data in G.nodes(data=True):
        typ = (data.get("type") or "other").lower()
        color = COLOR.get(typ, COLOR["other"])
        ncolor.append(color)
        nsize.append(800 if typ == "meeting" else 600 if typ == "agenda_item" else 400)

    # classify edges - show meaningful relationships by default
    struct = [(u,v) for u,v,d in G.edges(data=True)
              if d.get("kind") in ("STRUCTURAL","CROSS_REF","OTHER")]
    mention = [(u,v) for u,v,d in G.edges(data=True)
               if d.get("kind") == "MENTION"]

    plt.figure(figsize=(18, 10))
    nx.draw_networkx_nodes(G, pos, node_color=ncolor, node_size=nsize, alpha=.9)
    nx.draw_networkx_edges(G, pos, edgelist=struct,
                           arrowstyle="-|>", arrowsize=12, width=1.6,
                           edge_color="#666666")
    if include_mentions:
        nx.draw_networkx_edges(G, pos, edgelist=mention,
                               arrowstyle="-|>", arrowsize=8, width=.4,
                               style="dotted", edge_color="#BBBBBB")
    # labels (short-title)
    labels = {n: (G.nodes[n].get("title") or "")[:25] for n in G.nodes}
    nx.draw_networkx_labels(G, pos, labels, font_size=7)

    plt.axis("off")
    plt.title("📑  City Clerk Document Graph (layered)", fontsize=15)
    plt.tight_layout()

def main():
    G = load_graph()
    include_mentions = False

    def on_key(event):
        nonlocal include_mentions
        if event.key.lower() == "e":           # toggle mention edges
            include_mentions = not include_mentions
            plt.clf()
            build_plot(G, include_mentions)
            plt.draw()

    build_plot(G, include_mentions)
    plt.gcf().canvas.mpl_connect('key_press_event', on_key)
    print("🔍  press <E> to toggle mention edges on/off")
    plt.show()

if __name__ == "__main__":
    main() 