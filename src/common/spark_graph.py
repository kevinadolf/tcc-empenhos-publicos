"""Lightweight wrapper around Spark DataFrames representing a graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Optional

from pyspark.sql import DataFrame, SparkSession


@dataclass(repr=False)
class SparkGraph:
    spark: SparkSession
    vertices: DataFrame
    edges: DataFrame
    _graphframe: Optional["GraphFrame"] = field(default=None, init=False, repr=False)

    def as_graphframe(self) -> "GraphFrame":
        if self._graphframe is None:
            from graphframes import GraphFrame  # imported lazily to avoid heavy dependency at module import

            self._graphframe = GraphFrame(self.vertices, self.edges)
        return self._graphframe

    def num_vertices(self) -> int:
        return int(self.vertices.count())

    def num_edges(self) -> int:
        return int(self.edges.count())

    def vertices_iter(self) -> Iterator:
        return self.vertices.toLocalIterator()

    def edges_iter(self) -> Iterator:
        return self.edges.toLocalIterator()

