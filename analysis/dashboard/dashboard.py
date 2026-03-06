import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pathlib import Path
import sys

st.set_page_config(layout="wide")

st.title("Log Pipeline Monitoring Dashboard")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from log_parser import parse_apache, parse_app_json, parse_syslog


def load_from_mongo():
    client = MongoClient(
        "mongodb://localhost:27017",
        serverSelectionTimeoutMS=2000,
        connectTimeoutMS=2000,
        socketTimeoutMS=2000,
    )
    db = client["log_pipeline_db"]
    logs = list(db.logs_parsed.find({}, {"_id": 0}))
    return pd.DataFrame(logs)


def load_from_files():
    logs = []

    apache_file = DATA_DIR / "apache.log"
    if apache_file.exists():
        with apache_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parsed = parse_apache(line)
                if parsed:
                    logs.append(parsed)

    app_file = DATA_DIR / "app.json"
    if app_file.exists():
        with app_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    logs.append(parse_app_json(line))
                except Exception:
                    continue

    syslog_file = DATA_DIR / "syslog.log"
    if syslog_file.exists():
        with syslog_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                logs.append(parse_syslog(line))

    return pd.DataFrame(logs)


data_source = "MongoDB"
try:
    df = load_from_mongo()
except PyMongoError:
    st.info("MongoDB unavailable at localhost:27017. Showing data parsed directly from local files.")
    df = load_from_files()
    data_source = "local files"

if df.empty:
    st.warning("No logs found in MongoDB or local files under analysis/data.")
    st.stop()

if "level" not in df.columns:
    st.error("Missing required 'level' field in parsed logs.")
    st.stop()

if "service" not in df.columns:
    df["service"] = "unknown"

st.caption(f"Data source: {data_source}")

# KPIs
total_logs = len(df)

errors = df[df["level"] == "ERROR"]

error_rate = (len(errors) / total_logs) * 100

col1, col2 = st.columns(2)

col1.metric("Total Logs", total_logs)
col2.metric("Error Rate", f"{error_rate:.2f}%")

st.divider()

# Log Level Distribution
level_counts = df["level"].value_counts().reset_index()
level_counts.columns = ["Level", "Count"]

fig1 = px.pie(level_counts, values="Count", names="Level", title="Log Level Distribution")

st.plotly_chart(fig1, use_container_width=True)

# Logs per service
service_counts = df["service"].value_counts().reset_index()
service_counts.columns = ["Service", "Logs"]

fig2 = px.bar(service_counts, x="Service", y="Logs", title="Logs per Service")

st.plotly_chart(fig2, use_container_width=True)
