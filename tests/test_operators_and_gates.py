"""Unit and integration tests for Custom Airflow Quality Operators, Internal AI Agent, and Model Gates."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from dags.retail_panel_model_quality_dag import (
    SnowflakeGoldPublishOperator,
    build_dag,
)
from src.pipeline_and_model_gates import (
    InternalAIQualityAgentOperator,
    StatisticalModelEvaluationOperator,
    price_to_mills,
)


class TestRetailPanelQualityAndModelGates(unittest.TestCase):
    def setUp(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.summary_path = self.base_dir / "outputs" / "validation_18_gates_summary.json"

    def test_price_to_mills_exact_decimal_conversion(self) -> None:
        self.assertEqual(price_to_mills("2.55"), 2550)
        self.assertEqual(price_to_mills("-11062.06"), -11062060)
        self.assertEqual(price_to_mills(0), 0)

    def test_internal_ai_quality_agent_structured_json(self) -> None:
        agent = InternalAIQualityAgentOperator()
        report = agent.execute_triage(
            quarantine_rows=[{"source_row_number": 10, "unit_price_mills": -100}],
            stock_codes={"85123A", "POST", "DOT", "22423"},
            guest_checkout_lines=135078,
            duplicate_lines=10147,
        )
        self.assertTrue(report["schema_valid"])
        self.assertEqual(report["tool_calls_executed"], 3)
        self.assertEqual(report["non_merchandise_code_count"], 2)
        self.assertEqual(report["merchandise_sku_count"], 2)

    def test_airflow_dag_and_gold_publish_operator_guardrail(self) -> None:
        dag = build_dag()
        self.assertEqual(dag.dag_id, "retail_shopping_panel_quality_and_model_gating")
        pub_op = SnowflakeGoldPublishOperator(task_id="publish_gold_mart")
        with self.assertRaises(RuntimeError):
            pub_op.execute({"pipeline_summary": {"quality_and_model_gates": {"all_passed": False}}})

    def test_verified_18_gates_and_holdout_roc_auc(self) -> None:
        self.assertTrue(self.summary_path.exists(), "Expected outputs/validation_18_gates_summary.json")
        data = json.loads(self.summary_path.read_text(encoding="utf-8"))
        self.assertEqual(data["dataset"]["source_rows"], 541909)
        self.assertEqual(data["dataset"]["promoted_silver_rows"], 541907)
        self.assertEqual(data["dataset"]["quarantined_negative_price_rows"], 2)
        self.assertEqual(data["dataset"]["known_customer_accounts"], 4372)
        self.assertEqual(data["quality_and_model_gates"]["total_gates"], 18)
        self.assertEqual(data["quality_and_model_gates"]["passed_gates"], 18)
        self.assertTrue(data["quality_and_model_gates"]["all_passed"])
        self.assertGreaterEqual(data["statistical_model_evaluation"]["holdout_roc_auc"], 0.93)
        self.assertLess(data["statistical_model_evaluation"]["score_psi"], 0.05)


if __name__ == "__main__":
    unittest.main()
