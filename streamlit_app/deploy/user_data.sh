#!/bin/bash
# GlobalPartners dashboard - EC2 user data (Amazon Linux 2023).
# Runs once at first boot: installs Python, copies the app from S3, starts it as a service.
# Edit the three values below before pasting into the EC2 launch form.
APP_BUCKET="aws-glue-assets-ACCOUNT_ID-us-east-1"
ATHENA_RESULTS="s3://aws-glue-assets-ACCOUNT_ID-us-east-1/athena-results/"
APP_PASSWORD="change-me"

set -euxo pipefail
dnf install -y python3.11 python3.11-pip
mkdir -p /opt/gp-dashboard
cat > /etc/gp-dashboard.env <<ENV
DATA_MODE=athena
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
GOLD_DB=gp_gold
ATHENA_S3_STAGING=${ATHENA_RESULTS}
APP_PASSWORD=${APP_PASSWORD}
APP_BUCKET=${APP_BUCKET}
ENV
chmod 600 /etc/gp-dashboard.env

aws s3 sync "s3://${APP_BUCKET}/streamlit_app/" /opt/gp-dashboard/
python3.11 -m venv /opt/gp-dashboard/.venv
/opt/gp-dashboard/.venv/bin/pip install --upgrade pip
/opt/gp-dashboard/.venv/bin/pip install -r /opt/gp-dashboard/requirements.txt

cat > /etc/systemd/system/gp-dashboard.service <<'UNIT'
[Unit]
Description=GlobalPartners Streamlit dashboard
After=network-online.target

[Service]
EnvironmentFile=/etc/gp-dashboard.env
WorkingDirectory=/opt/gp-dashboard
# Pull the latest app code from S3 on every (re)start
ExecStartPre=/bin/bash -c 'aws s3 sync "s3://${APP_BUCKET}/streamlit_app/" /opt/gp-dashboard/ --exclude ".venv/*"'
ExecStart=/opt/gp-dashboard/.venv/bin/streamlit run Overview.py --server.port 8501 --server.address 0.0.0.0
Restart=always
User=root

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now gp-dashboard
