"""High-level orchestration for running anomaly analysis pipelines."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List

from src.analysis.detectors import AnomalyResult, run_default_anomaly_suite
from src.common.spark_graph import SparkGraph


@dataclass
class AnalysisReport:
    anomalies: Dict[str, List[AnomalyResult]]

    def as_dict(self) -> Dict:
        return {
            key: [asdict(anomaly) for anomaly in value]
            for key, value in self.anomalies.items()
        }


def analyze_graph(graph: SparkGraph) -> AnalysisReport:
    anomalies = run_default_anomaly_suite(graph)
    return AnalysisReport(anomalies=anomalies)
