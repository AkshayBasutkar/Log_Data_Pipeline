Log Data Pipeline Monitoring Dashboard

Features
- Log parsing
- KPI monitoring
- Error analysis
- Entity analysis
- Alert visibility

Tech Stack
- Python
- Streamlit
- Pandas
- Plotly
- MongoDB

Run the analysis and dashboard

1. Install dependencies from the repo root:
   - `pip install -r analysis/requirements.txt`
2. Build a metrics snapshot from MongoDB logs:
   - `python analysis/build_metrics.py`
3. Start the dashboard:
   - `streamlit run analysis/dashboard.py`

Data source
- Raw logs: reads processed documents from `<DB>.logs_parsed`
- Analysis snapshots: writes and reads dashboard-ready aggregates in `<DB>.metrics`
- Alerts: reads alert records from `<DB>.alerts` when present

What you'll see
- KPIs: values loaded from the latest MongoDB metrics snapshot
- Charts: distributions and time series loaded from the latest MongoDB metrics snapshot
- Entity analytics: HDFS blocks, Spark stages and tasks, Windows KBs and DLLs
- Tables: recent processed logs and recent error logs stored in the snapshot
