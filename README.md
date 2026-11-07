# Retail Airflow Quality Platform — Custom Airflow Operators, Internal AI Agents & Model Gating

Production-grade retail consumer shopping panel pipeline combining **custom Apache Airflow operators**, **internal AI data-quality triage agents**, **statistical model evaluation gates**, **Snowflake SQL (`SQL Joins & CTEs`)**, and **declarative Terraform, Ansible, Docker, and Kubernetes infrastructure**.

## Key Verified Results (`outputs/validation_18_gates_summary.json`)

- **Dataset Scale & Hash Provenance:** Processed **541,909 retail shopping basket rows** across **4,372 known consumer accounts**, **4,070 distinct SKUs**, and **38 countries** from the UCI Online Retail dataset (DOI `10.24432/C5BW33`, SHA-256 `f5385cbb54bbebf7196389109c6b0621faab0c304e3702548165e71c84aede8b`).
- **18/18 (`100%`) Automated Quality & Model Gates Passed:**
  - Quarantined **2 negative-price bad-debt adjustment rows** and promoted **541,907 validated Silver rows** with exact integer-mills ledger parity (**`9,769,872,054` GBP mills** = `£9,769,872.054` net; `$0.00` reconciliation drift).
  - Preserved **135,078 guest-checkout lines** in an explicit `UNKNOWN_GUEST` governance bucket and flagged **10,147 duplicate source lines** and **9,288 cancellation lines** (`C` prefix).
- **Internal AI Quality Agent (`InternalAIQualityAgentOperator`):** Executed deterministic structured JSON tool calls (`inspect_quarantine_anomalies`, `reconcile_sku_taxonomy`, `audit_guest_and_duplicate_lines`) across all **4,070 StockCodes**, separating non-merchandise postage/fee codes from retail SKUs and generating automated SQL remediation scripts.
- **Statistical Model Integration Gate (`StatisticalModelEvaluationOperator`):** Evaluated consumer repeat-purchase propensity across **4,372 accounts** (`3,270` train / `1,102` holdout), achieving **0.9366 (`0.937`) holdout ROC-AUC**, **0.1018 Brier score** (**`57.16%` relative improvement** over prior baseline `0.2375`), and **0.0007 Population Stability Index (`PSI < 0.05`)** before promoting Gold tables.
- **Declarative Infrastructure:** Includes Terraform (`infra/terraform/main.tf`), Ansible (`infra/ansible/playbook.yml`), Kubernetes Deployment (`infra/k8s/airflow-worker-deployment.yaml`), and `Dockerfile`.

## Repository Structure

```text
├── dags/
│   └── retail_panel_model_quality_dag.py      # Custom Airflow operators & DAG definition
├── docs/
│   └── schema_and_architecture.md             # Schema mapping & architecture specification
├── infra/
│   ├── ansible/playbook.yml                   # Ansible worker provisioning playbook
│   ├── k8s/airflow-worker-deployment.yaml     # Kubernetes deployment manifest
│   └── terraform/main.tf                      # Terraform S3 & Kubernetes namespace IaC
├── outputs/
│   ├── ai_agent_triage_report.json            # Internal AI agent structured JSON triage output
│   ├── gold_country_monthly_mart.csv          # 314-row Gold country x month reconciled mart
│   └── validation_18_gates_summary.json       # Verified 18/18 gate execution results
├── sql/
│   └── snowflake_retail_panel_ddl_and_ctes.sql # Snowflake DDL & SQL Joins/CTEs views
├── src/
│   └── pipeline_and_model_gates.py            # End-to-end ETL, AI agent & statistical model gate engine
├── tests/
│   └── test_operators_and_gates.py            # Unit & integration test suite (4/4 passing)
└── Dockerfile
```

## Quickstart

```bash
python3 -m src.pipeline_and_model_gates
python3 -m unittest discover -s tests -p "test_*.py" -v
```
