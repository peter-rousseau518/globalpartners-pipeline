# GlobalPartners Business Insights Pipeline

An all-AWS batch data pipeline that turns restaurant order data in **SQL Server** into a daily **Customer Lifetime Value (CLV)** history and six **Streamlit** dashboards — with every transformation written in **PySpark**, no new licenses, and no Snowflake or dbt.

![Architecture](docs/architecture.png)

**Flow:** Amazon RDS for SQL Server → AWS Glue (PySpark) → S3 Bronze → Silver → Gold (Apache Iceberg, Glue Data Catalog) → Amazon Athena → Streamlit on EC2.
**Orchestration:** AWS Step Functions with retries, email alerts (SNS) and automatic start/stop of the database.
**CI/CD:** GitHub Actions — lint + PySpark unit tests on every push; deploy to AWS on merge to `main` through OIDC (no stored AWS keys).

---

## Results at a glance

| | |
| --- | --- |
| Source rows | 203,519 order lines · 193,017 options · 131,328 orders · 28 locations · Apr 2020 – Feb 2024 |
| Customers with a daily CLV history | 20,059 (10,789,256 customer-day snapshot rows) |
| Net revenue (clean, excl. bulk outliers) | $1,835,705 |
| CLV held by the top 20% of customers | 77% |
| High-value customers lapsed or at risk | CLV of $862,984 |
| Orders with a discount | 9.6% · $22,749 total (1.22% of gross) |

## Key business findings

- **Value is concentrated.** The top 20% of customers hold 77% of lifetime value. 757 active VIPs alone hold $344K.
- **The biggest win-back pool:** 2,006 High-CLV customers have lapsed, holding $529K of lifetime value. The Churn Risk page exports this list.
- **Loyalty members are worth about 3× more** (average CLV $161 vs $52; 73% vs 41% repeat). Caveat: loyalty cards only appear from March 2023, so members are recent customers — the monthly view compares like-for-like periods.
- **Weekdays outsell weekends and holidays** ($1,469 vs $1,148 vs $1,132 average net revenue per day). Breakfast and lunch dominate.
- **Five locations produce 59% of revenue** (last 12 months). Top locations win on order volume, not order size.
- **Customers order more after their first discount** (0.90 → 1.53 orders in the 30 days before vs after) — an association, not proof of cause.

## Data-quality findings that changed the design

| Finding | Impact | Handling |
| --- | --- | --- |
| `item_price` is the **line total**, not a unit price (9,873 of 13,496 multi-quantity lines = unit × qty) | Revenue would have been overstated 5× ($10.0M vs $1.88M) | `unit_price = item_price ÷ quantity`; revenue uses the line total |
| `date_dim` covers **2023 only**; orders run 2020–2024 | 60% of lines had no calendar row | Calendar generated in PySpark for 2020–2024 with US federal holidays |
| **No negative option prices** — the specified discount rule finds nothing | Discount analysis impossible as specified | Discount = item sold ≥ $0.05 below its usual price (modal unit price per location, item and month) |
| 826 **test/development** orders | Would inflate every metric | Quarantined |
| 188 **corrupted category values** (pasted admin URLs, typos) | Split categories | Cleaned with rules in Silver |
| 9 **bulk-quantity lines** (100–500 units) | Distort revenue and CLV | Flagged `is_outlier`, excluded from metrics |
| One customer with **2,124 orders** | Likely a staff/house account | Kept; flagged for business review |

Full profile: [`docs/data_profile.md`](docs/data_profile.md).

## Why each tool

| Layer | Choice | Why |
| --- | --- | --- |
| Source | Amazon RDS for SQL Server Express | Requirement: SQL Server source. Managed, license-included, nothing to patch. |
| Ingestion | AWS Glue PySpark job over JDBC | Requirement: all logic in PySpark. Serverless; reads SQL Server with a read-only login and checks row counts. AWS DMS was unnecessary for a daily batch. |
| Storage | Amazon S3, Bronze / Silver / Gold | Cheap and durable. Bronze is an immutable copy, so any layer can be rebuilt without touching SQL Server. |
| Table format | Apache Iceberg (open source, built into Glue 5.0) | Atomic table replacement, schema in the Glue Catalog, time travel for rollback. No license. |
| Transform | AWS Glue 5.0 (Spark 3.5) | Serverless PySpark, billed per second. EMR would add cluster management for no gain at this size. |
| Query | Amazon Athena | Serverless SQL over Iceberg; the dashboard pays only for what it scans. |
| Dashboard | Streamlit on EC2 (t3.small) | Requirement: Streamlit. One small instance with an IAM role — no keys on the server. |
| Orchestration | AWS Step Functions | Visual workflow, per-step retries, failure alerts, and **Redrive** to resume a failed run from the failed step. Amazon MWAA (Airflow) costs ~$350+/month minimum. |
| Alerts | Amazon SNS | Email on success or failure, with the error and cause. |
| Scheduling | Amazon EventBridge Scheduler | Daily 6 AM US Eastern — defined in code but **not enabled** in the demo account, to control cost. |
| CI/CD | GitHub Actions + AWS OIDC | Requirement: GitHub CI/CD. OIDC gives each run a short-lived AWS login, so no access keys are stored in GitHub. |

## Data model (Gold, `gp_gold`)

| Table | Grain |
| --- | --- |
| `dim_date` | Day, 2020–2024, with holidays |
| `dim_customer` | Customer × loyalty-status version (SCD Type 2) |
| `dim_restaurant`, `dim_item`, `dim_channel` | One row per location / item / ordering app |
| `fact_order_line` | Order line, with unit price, usual (list) price, discount, gross and net |
| `fact_order` | Order |
| **`fact_customer_clv_daily`** | **Customer × day** since first order: CLV to date, tier (High 20% / Medium 60% / Low 20%), RFM scores and segment, churn status — *the primary deliverable* |
| `mart_*` | Pre-aggregated tables for each dashboard |

## Repository

```text
glue_jobs/          PySpark jobs: ingest_sqlserver, silver_transform, gold_model, gold_metrics
sql_server/         Table DDL, S3 → RDS bulk load, read-only Glue login
orchestration/      Step Functions workflow, its IAM policy, daily schedule (not enabled)
streamlit_app/      Dashboard (6 pages), EC2 user-data script, EC2 IAM policy
tests/              PySpark unit tests for the business rules
infra/github/       OIDC trust + deploy role policy for GitHub Actions
scripts/            Source profiling script
docs/               Solution design, data profile, architecture diagram (draw.io / SVG / PNG)
.github/workflows/  ci.yml (lint + tests), deploy.yml (deploy to AWS)
```

## Running it

1. **Source:** run `sql_server/01_create_tables.sql`, upload the CSVs to S3, run `sql_server/02_load_from_s3.sql`, then `03_create_glue_reader.sql`.
2. **Pipeline:** Step Functions → `gp-daily-pipeline` → **Start execution**. It starts SQL Server, runs Bronze → Silver → Gold → metrics, stops SQL Server and emails the result (about 25–40 minutes).
3. **Dashboard:** launch EC2 with `streamlit_app/deploy/user_data.sh` and the role policy in `streamlit_app/deploy/`, then open `http://<public-ip>:8501`.
4. **Tests locally:** `pip install -r requirements-dev.txt && pytest`.

If a run fails, the alert email names the failed step. Fix the cause, open the execution and click **Redrive** — completed steps are not repeated, and every job is idempotent, so re-running a day never duplicates data.

## Cost (approximate, us-east-1)

| Item | Cost |
| --- | --- |
| One full pipeline run (four Glue jobs) | ~$0.50 |
| RDS SQL Server Express db.t3.small | billed only while running; stopped after each run |
| EC2 t3.small for the dashboard | ~$0.50/day while running |
| S3, Athena, Step Functions, SNS | cents per month |

## Lessons learned

- **Profile before modelling.** The line-total pricing and the 2023-only calendar would each have produced confidently wrong dashboards.
- **Relative scores need the right population.** RFM quintiles across all customers made long-lapsed buyers look like VIPs; scoring only customers active in the window fixed it.
- **Small databases need memory headroom.** The SQL Server load stalled on a 1 GB instance (`RESOURCE_SEMAPHORE`); 2 GB plus index-free bulk inserts fixed it.
- **GitHub OIDC subjects now include IDs** (`repo:owner@id/repo@id:ref:…`). Trust policies written for the older name-only format are rejected — check CloudTrail for the exact subject.
