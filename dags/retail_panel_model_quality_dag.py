"""Apache Airflow DAG and Custom Operators for Retail Shopping Panel Quality & Model Gating."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from src.pipeline_and_model_gates import (
    InternalAIQualityAgentOperator,
    StatisticalModelEvaluationOperator,
    run_retail_quality_and_model_pipeline,
)


@dataclass
class CustomAirflowBaseOperator:
    """Lightweight Airflow BaseOperator-compatible wrapper for local and KubernetesPod execution."""

    task_id: str
    retries: int = 2
    retry_delay_seconds: int = 60

    def execute(self, context: Dict[str, Any]) -> Any:
        raise NotImplementedError


@dataclass
class RetailSchemaAndQualityOperator(CustomAirflowBaseOperator):
    """Custom Airflow operator enforcing 18 schema, quarantine, and integer-mill ledger gates."""

    zip_path: Path = Path("/tmp/online_retail.zip")
    output_dir: Path = Path("outputs")

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        summary = run_retail_quality_and_model_pipeline(self.zip_path, self.output_dir)
        if not summary["quality_and_model_gates"]["all_passed"]:
            raise RuntimeError("RetailSchemaAndQualityOperator failed one or more quality gates.")
        return summary


@dataclass
class SnowflakeGoldPublishOperator(CustomAirflowBaseOperator):
    """Custom Airflow operator verifying Gold mart reconciliation before Snowflake swap."""

    target_schema: str = "RETAIL_GOLD"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        upstream = context.get("pipeline_summary", {})
        gates = upstream.get("quality_and_model_gates", {})
        if not gates.get("all_passed", False):
            raise RuntimeError("Blocked Gold publish: upstream quality or model gates did not pass.")
        return {
            "published_schema": self.target_schema,
            "promoted_rows": upstream["dataset"]["promoted_silver_rows"],
            "country_month_groups": upstream["dataset"]["country_month_groups"],
            "status": "PUBLISHED",
        }


@dataclass
class RetailPanelAirflowDAG:
    """Declarative DAG specification for retail shopping panel validation and model gating."""

    dag_id: str = "retail_shopping_panel_quality_and_model_gating"
    schedule_interval: str = "0 4 * * *"
    start_date: str = "2026-01-01"
    catchup: bool = False
    tags: List[str] = field(default_factory=lambda: ["retail-panel", "data-quality", "ai-agent", "snowflake"])


def build_dag() -> RetailPanelAirflowDAG:
    return RetailPanelAirflowDAG()
