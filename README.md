# GlobalPartners Business Insights Pipeline

All-AWS data pipeline: Amazon RDS for SQL Server → AWS Glue (PySpark) → S3 Bronze/Silver/Gold (Iceberg) → Athena → Streamlit on EC2. It calculates daily Customer Lifetime Value, RFM segments, churn indicators, sales trends, loyalty impact, location performance and discount effectiveness.

![Architecture](docs/architecture.png)

- [Solution design](docs/solution_design.md)
- [Editable diagram (draw.io)](docs/architecture.drawio)
