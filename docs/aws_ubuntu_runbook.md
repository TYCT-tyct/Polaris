# Polaris AWS Ubuntu 24.04 Runbook

## 1. Host Preparation
```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3-pip postgresql-client
```

## 2. App Setup
```bash
cd /home/ubuntu
git clone <your-repo-url> polaris
cd /home/ubuntu/polaris
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
cp .env.example .env
```

Set `POLARIS_DATABASE_URL` to your production Postgres DSN.

## 3. Migrate and Validate
```bash
source .venv/bin/activate
set -a
source .env
set +a
python -m polaris.cli migrate
python -m polaris.cli doctor --handle elonmusk
python -m polaris.cli harvest-once --handle elonmusk
```

Why `source .env`: current CLI settings read process env vars directly. Without this,
manual commands may fall back to default DSN.

## 4. systemd Service
Create `/etc/systemd/system/polaris-harvest.service`:

```ini
[Unit]
Description=Polaris Data Harvester
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/polaris
EnvironmentFile=/home/ubuntu/polaris/.env
ExecStart=/home/ubuntu/polaris/.venv/bin/python -m polaris.cli run --handle elonmusk
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable polaris-harvest
sudo systemctl start polaris-harvest
sudo systemctl status polaris-harvest
```

## 5. Operational Checks
- Check logs: `journalctl -u polaris-harvest -f`
- Check database growth and ingestion:
  - `select count(*) from ops_collector_run where started_at >= now() - interval '1 hour';`
  - `select count(*) from fact_quote_top_raw where captured_at >= now() - interval '1 hour';`
  - `select count(*) from fact_tweet_post;`
