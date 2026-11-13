"""Apache Airflow DAG Definition for REST API -> S3 Bronze -> Snowflake Star Schema & DQ Gates."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from api_s3_ingestion_ci_mart.src.api_s3_snowflake_etl import run_pipeline


@dataclass
class TaskNode:
    task_id: str
    callable_fn: Callable[..., object]
    upstream: list[str] = field(default_factory=list)


@dataclass
class AirflowDAGSpec:
    dag_id: str
    schedule_interval: str
    tags: list[str]
    tasks: dict[str, TaskNode] = field(default_factory=dict)

    def add_task(self, node: TaskNode) -> None:
        self.tasks[node.task_id] = node


def execute_rest_api_extract(module_dir: Path) -> dict:
    """Task 1: Ingest USGS GeoJSON API feed, partition by S3 Hive date key, and run ETL."""
    return run_pipeline(module_dir=module_dir, fetch_live=False)


def enforce_snowflake_dq_gates(report: dict) -> bool:
    """Task 2: Block Gold table promotion unless 10/10 SQL data-quality gates pass."""
    dq = report.get("star_schema_and_dq", {})
    if not dq.get("all_dq_gates_passed", False):
        raise RuntimeError(
            f"Data quality gate failure: {dq.get('dq_gates_passed', 0)}/{dq.get('dq_gates_total', 0)} passed"
        )
    return True


def build_api_s3_snowflake_dag() -> AirflowDAGSpec:
    """Construct lightweight, zero-daemon DAG specification verified in CI."""
    dag = AirflowDAGSpec(
        dag_id="rest_api_s3_snowflake_star_schema_pipeline",
        schedule_interval="@daily",
        tags=["rest-api", "aws-s3", "snowflake", "star-schema", "data-quality"],
    )
    dag.add_task(TaskNode(task_id="extract_rest_geojson_to_s3_bronze", callable_fn=execute_rest_api_extract))
    dag.add_task(
        TaskNode(
            task_id="enforce_snowflake_star_schema_dq_gates",
            callable_fn=enforce_snowflake_dq_gates,
            upstream=["extract_rest_geojson_to_s3_bronze"],
        )
    )
    return dag
