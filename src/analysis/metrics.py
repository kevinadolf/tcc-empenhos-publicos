"""Graph metrics implemented with PySpark DataFrames and GraphFrames."""

from __future__ import annotations

import math
from typing import Dict, Iterable

from pyspark.sql import DataFrame, functions as F

from src.common.spark_graph import SparkGraph


def weighted_degree_centrality(graph: SparkGraph, node_type: str) -> Dict[str, float]:
    """Compute weighted degree centrality for vertices of a given type."""
    vertices = graph.vertices.alias("v")
    edges = graph.edges.alias("e")
    weight_col = F.coalesce(F.col("e.valor"), F.lit(1.0))

    outgoing = (
        edges.join(vertices, F.col("e.src") == F.col("v.id"))
        .where(F.col("v.node_type") == node_type)
        .groupBy(F.col("e.src").alias("node_id"))
        .agg(F.sum(weight_col).alias("centrality"))
    )

    incoming = (
        edges.join(vertices, F.col("e.dst") == F.col("v.id"))
        .where(F.col("v.node_type") == node_type)
        .groupBy(F.col("e.dst").alias("node_id"))
        .agg(F.sum(weight_col).alias("centrality"))
    )

    scores = outgoing.unionByName(incoming, allowMissingColumns=True)
    aggregated = scores.groupBy("node_id").agg(F.sum("centrality").alias("centrality"))
    return {row["node_id"]: float(row["centrality"]) for row in aggregated.collect()}


def entropy(values: Iterable[float]) -> float:
    total = float(sum(values))
    if total == 0:
        return 0.0
    probs = [value / total for value in values if value > 0]
    if not probs:
        return 0.0
    return -sum(p * math.log(p, 2) for p in probs)


def shannon_entropy_by_neighbor(
    graph: SparkGraph,
    focus_type: str,
    neighbor_type: str,
) -> Dict[str, float]:
    vertices = graph.vertices.alias("v")
    edges = graph.edges.alias("e")
    focus_vertices = vertices.filter(F.col("v.node_type") == focus_type).select(
        F.col("v.id").alias("focus_id"),
    )
    neighbor_vertices = vertices.filter(F.col("v.node_type") == neighbor_type).select(
        F.col("v.id").alias("neighbor_id"),
    )

    weight_col = F.coalesce(F.col("e.valor"), F.lit(1.0))

    outgoing = (
        edges.join(focus_vertices, F.col("e.src") == F.col("focus_id"))
        .join(neighbor_vertices, F.col("e.dst") == F.col("neighbor_id"))
        .select("focus_id", "neighbor_id", weight_col.alias("weight"))
    )

    incoming = (
        edges.join(focus_vertices, F.col("e.dst") == F.col("focus_id"))
        .join(neighbor_vertices, F.col("e.src") == F.col("neighbor_id"))
        .select("focus_id", "neighbor_id", weight_col.alias("weight"))
    )

    distributions = outgoing.unionByName(incoming, allowMissingColumns=True)
    if distributions.rdd.isEmpty():
        return {row["focus_id"]: 0.0 for row in focus_vertices.collect()}

    totals = distributions.groupBy("focus_id").agg(F.sum("weight").alias("total_weight"))
    joined = distributions.join(totals, on="focus_id")

    entropy_components = joined.filter(F.col("weight") > 0).select(
        "focus_id",
        (
            -F.col("weight")
            / F.col("total_weight")
            * (F.log(F.col("weight") / F.col("total_weight")) / F.log(F.lit(2.0)))
        ).alias("component"),
    )

    entropy_df = entropy_components.groupBy("focus_id").agg(
        F.sum("component").alias("entropy"),
    )

    results = (
        focus_vertices.join(entropy_df, on="focus_id", how="left")
        .fillna({"entropy": 0.0})
        .collect()
    )
    return {row["focus_id"]: float(row["entropy"]) for row in results}


def project_bipartite(
    graph: SparkGraph,
    source_type: str,
    target_type: str,
) -> DataFrame:
    """Return the edge projection between two node types."""
    vertices = graph.vertices.alias("v")
    edges = graph.edges.alias("e")

    source_vertices = vertices.filter(F.col("v.node_type") == source_type).select(
        F.col("v.id").alias("source_id"),
    )
    target_vertices = vertices.filter(F.col("v.node_type") == target_type).select(
        F.col("v.id").alias("target_id"),
    )

    weight_col = F.coalesce(F.col("e.valor"), F.lit(1.0))

    return (
        edges.join(source_vertices, F.col("e.src") == F.col("source_id"))
        .join(target_vertices, F.col("e.dst") == F.col("target_id"))
        .select("source_id", "target_id", weight_col.alias("weight"))
    )


def community_detection_louvain(
    graph: SparkGraph,
    max_iter: int = 20,
) -> Dict[str, int]:
    """Approximate Louvain communities via label propagation on GraphFrames."""
    try:
        graphframe = graph.as_graphframe()
    except Exception:  # pragma: no cover - GraphFrames is optional at runtime
        return {
            row["id"]: 0 for row in graph.vertices.select("id").collect()
        }

    communities = graphframe.labelPropagation(maxIter=max_iter).select("id", "label")
    return {row["id"]: int(row["label"]) for row in communities.collect()}
