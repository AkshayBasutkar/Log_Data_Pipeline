Log Data Pipeline Monitoring Dashboard

Features
- Log parsing
- KPI monitoring
- Error analysis
- Request monitoring
- Anomaly detection

Tech Stack
- Python
- Streamlit
- Pandas
- Plotly

Run the dashboard

1) Install dependencies (from the repo root):

   - `pip install -r analysis/requirements.txt`

2) Start the dashboard:

   - `streamlit run analysis/dashboard/dashboard.py`

Data sources
- **MongoDB**: if `MONGO_URI` is reachable, reads from `<DB>.logs_parsed`
- **Local files fallback**: parses `analysis/data/apache.log`, `analysis/data/app.json`, `analysis/data/syslog.log`
- **CSV fallback**: also loads `analysis/data/logs.csv` if present

What you’ll see
- **KPIs**: total logs, errors, error rate, critical count, services, hosts, anomalies, p95 latency (when available), HTTP 5xx rate (when available)
- **Charts**: log volume over time, error rate over time, level distribution, top services/hosts, HTTP status codes, severity distribution, derived anomaly spikes, top error messages
- **Explorer**: recent error events + raw filtered dataset sample