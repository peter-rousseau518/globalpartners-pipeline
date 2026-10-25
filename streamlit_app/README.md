# GlobalPartners Streamlit dashboard

Six pages over the Gold layer (`gp_gold`), queried through Amazon Athena.

| Page | Source tables |
| --- | --- |
| Overview | mart_customer_current, mart_sales_daily |
| Customer Segmentation | mart_customer_current, fact_customer_clv_daily (one customer at a time) |
| Churn Risk | mart_customer_current, fact_customer_clv_daily (first-of-month snapshots) |
| Sales Trends | mart_sales_daily, dim_date |
| Loyalty Impact | mart_customer_current, mart_loyalty_monthly |
| Location Performance | mart_location_monthly, dim_restaurant |
| Pricing & Discounts | mart_discount_monthly, mart_discount_effect, fact_order_line |

## Run locally against Parquet copies
```bash
pip install -r requirements.txt
DATA_MODE=local LOCAL_DATA_DIR=./local_data streamlit run Overview.py
```

## Run against Athena
```bash
DATA_MODE=athena ATHENA_S3_STAGING=s3://<bucket>/athena-results/ streamlit run Overview.py
```

## Deploy on EC2
See `deploy/user_data.sh` (first-boot install + systemd service) and `deploy/ec2_role_policy.json` (least-privilege role).
The service re-syncs code from S3 on every restart: upload new files to `s3://<bucket>/streamlit_app/`, then reboot the instance.
