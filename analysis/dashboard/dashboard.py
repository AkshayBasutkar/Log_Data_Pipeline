import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ModuleNotFoundError:
    MongoClient = None
    PyMongoError = Exception


st.set_page_config(
    layout="wide",
    page_title="Log Data Pipeline Dashboard",
)
st.title("Log Data Pipeline Monitoring Dashboard")
st.caption("Operational analytics for log volume, errors, and anomalies.")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from log_parser import parse_apache, parse_app_json, parse_syslog


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("LOG_PIPELINE_DB_NAME", "log_pipeline")


LEVEL_ORDER = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


def _to_datetime(series: pd.Series) -> pd.Series:
    # Accepts strings/datetimes; normalizes to tz-aware UTC when possible.
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    if dt.isna().all():
        # fallback for already-naive datetime objects without tz
        dt = pd.to_datetime(series, errors="coerce")
    return dt


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "timestamp" not in out.columns:
        out["timestamp"] = pd.NaT
    out["timestamp"] = _to_datetime(out["timestamp"])

    if "ingested_at" in out.columns:
        out["ingested_at"] = _to_datetime(out["ingested_at"])

    for col, default in [
        ("level", "INFO"),
        ("service", "unknown"),
        ("host", "unknown"),
        ("message", ""),
        ("source_type", "unknown"),
    ]:
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default).astype(str)

    out["level"] = out["level"].str.upper()
    out["service"] = out["service"].str.strip()
    out["host"] = out["host"].str.strip()
    out["source_type"] = out["source_type"].str.strip()

    if "status_code" not in out.columns:
        out["status_code"] = pd.NA
    out["status_code"] = pd.to_numeric(out["status_code"], errors="coerce")

    if "response_time_ms" not in out.columns:
        out["response_time_ms"] = pd.NA
    out["response_time_ms"] = pd.to_numeric(out["response_time_ms"], errors="coerce")

    if "severity_score" not in out.columns:
        out["severity_score"] = pd.NA
    out["severity_score"] = pd.to_numeric(out["severity_score"], errors="coerce")

    if "is_anomaly" not in out.columns:
        out["is_anomaly"] = False
    # Avoid pandas silent downcasting warnings with explicit boolean conversion.
    out["is_anomaly"] = out["is_anomaly"].astype("boolean").fillna(False).astype(bool)

    if "anomaly_score" not in out.columns:
        out["anomaly_score"] = pd.NA
    out["anomaly_score"] = pd.to_numeric(out["anomaly_score"], errors="coerce")

    out["is_error"] = out["level"].isin(["ERROR", "CRITICAL"])

    # Helpful derived time buckets for charts
    if out["timestamp"].notna().any():
        out["date"] = out["timestamp"].dt.date
        out["hour"] = out["timestamp"].dt.floor("h")
        out["minute"] = out["timestamp"].dt.floor("min")
    else:
        out["date"] = pd.NaT
        out["hour"] = pd.NaT
        out["minute"] = pd.NaT

    return out


@st.cache_data(ttl=30, show_spinner=False)
def load_from_mongo():
    if MongoClient is None:
        raise PyMongoError("pymongo is not installed")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=2000,
        connectTimeoutMS=2000,
        socketTimeoutMS=2000,
    )
    logs = list(client[DB_NAME].logs_parsed.find({}, {"_id": 0}))
    return pd.DataFrame(logs)


def _load_csv_logs(file_path: Path) -> pd.DataFrame:
    # analysis/data/logs.csv sample schema: timestamp,level,ip,message
    df = pd.read_csv(file_path)
    df = df.rename(columns={"ip": "host"})
    df["service"] = df.get("service", "unknown")
    df["source_type"] = df.get("source_type", "csv")
    df["status_code"] = df.get("status_code", pd.NA)
    df["response_time_ms"] = df.get("response_time_ms", pd.NA)
    df["severity_score"] = df.get("severity_score", pd.NA)
    df["anomaly_score"] = df.get("anomaly_score", pd.NA)
    df["is_anomaly"] = df.get("is_anomaly", False)
    df["ingested_at"] = df.get("ingested_at", pd.Timestamp.utcnow())
    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_from_files():
    logs = []

    csv_file = DATA_DIR / "logs.csv"
    if csv_file.exists():
        try:
            df_csv = _load_csv_logs(csv_file)
            logs.extend(df_csv.to_dict(orient="records"))
        except Exception:
            pass

    apache_file = DATA_DIR / "apache.log"
    if apache_file.exists():
        with apache_file.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                parsed = parse_apache(line)
                if parsed:
                    logs.append(parsed)

    app_file = DATA_DIR / "app.json"
    if app_file.exists():
        with app_file.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = parse_app_json(line)
                except Exception:
                    continue

                if parsed:
                    logs.append(parsed)

    syslog_file = DATA_DIR / "syslog.log"
    if syslog_file.exists():
        with syslog_file.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                parsed = parse_syslog(line)
                if parsed:
                    logs.append(parsed)

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

df = _normalize_df(df)

st.caption(f"Data source: {data_source}")

min_ts = df["timestamp"].min()
max_ts = df["timestamp"].max()
if pd.isna(min_ts) or pd.isna(max_ts):
    st.warning("Timestamps are missing/unparseable in the dataset; time-series charts will be limited.")


# -----------------------------
# Sidebar filters
# -----------------------------

st.sidebar.header("Filters")

levels = [lvl for lvl in LEVEL_ORDER if lvl in set(df["level"].unique())]
levels = levels or sorted(df["level"].unique().tolist())

services = sorted(df["service"].unique().tolist())
hosts = sorted(df["host"].unique().tolist())
sources = sorted(df["source_type"].unique().tolist())

selected_levels = st.sidebar.multiselect("Log levels", options=levels, default=levels)
selected_services = st.sidebar.multiselect("Services", options=services, default=services[: min(10, len(services))] or services)
selected_sources = st.sidebar.multiselect("Sources", options=sources, default=sources)

host_limit = 250
selected_hosts = st.sidebar.multiselect(
    f"Hosts (showing up to {host_limit})",
    options=hosts[:host_limit],
    default=hosts[: min(20, len(hosts), host_limit)],
)

if df["timestamp"].notna().any():
    start_default = min_ts.to_pydatetime()
    end_default = max_ts.to_pydatetime()
    selected_range = st.sidebar.date_input(
        "Date range (UTC)",
        value=(start_default.date(), end_default.date()),
        min_value=start_default.date(),
        max_value=end_default.date(),
    )
else:
    selected_range = None

only_errors = st.sidebar.toggle("Only errors (ERROR/CRITICAL)", value=False)


filtered = df[
    df["level"].isin(selected_levels)
    & df["service"].isin(selected_services if selected_services else services)
    & df["source_type"].isin(selected_sources)
    & df["host"].isin(selected_hosts if selected_hosts else hosts[:host_limit])
].copy()

if selected_range and filtered["timestamp"].notna().any():
    start_date, end_date = selected_range
    start_dt = pd.Timestamp(start_date, tz="UTC")
    end_dt = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    filtered = filtered[(filtered["timestamp"] >= start_dt) & (filtered["timestamp"] <= end_dt)]

if only_errors:
    filtered = filtered[filtered["is_error"]]

if filtered.empty:
    st.warning("No logs match the current filters.")
    st.stop()

# -----------------------------
# KPI summary
# -----------------------------

total_logs = int(len(filtered))
errors = filtered[filtered["is_error"]]
error_count = int(len(errors))
error_rate = (error_count / total_logs) * 100 if total_logs else 0.0
critical_count = int((filtered["level"] == "CRITICAL").sum())
unique_services = int(filtered["service"].nunique())
unique_hosts = int(filtered["host"].nunique())
anomaly_count = int(filtered["is_anomaly"].sum())

p95_rt = None
rt = filtered["response_time_ms"].dropna()
if len(rt) > 0:
    p95_rt = float(rt.quantile(0.95))

five_xx_rate = None
sc = filtered["status_code"].dropna()
if len(sc) > 0:
    five_xx_rate = float((sc >= 500).mean() * 100)

cols = st.columns(6)
cols[0].metric("Total logs", f"{total_logs:,}")
cols[1].metric("Errors", f"{error_count:,}", help="ERROR + CRITICAL")
cols[2].metric("Error rate", f"{error_rate:.2f}%")
cols[3].metric("Critical", f"{critical_count:,}")
cols[4].metric("Services", f"{unique_services:,}")
cols[5].metric("Hosts", f"{unique_hosts:,}")

cols2 = st.columns(3)
cols2[0].metric("Anomalies", f"{anomaly_count:,}", help="Flagged by pipeline or derived anomaly view below.")
cols2[1].metric("p95 response time (ms)", "—" if p95_rt is None else f"{p95_rt:,.0f}")
cols2[2].metric("HTTP 5xx rate", "—" if five_xx_rate is None else f"{five_xx_rate:.2f}%")

st.divider()

# -----------------------------
# Trend charts
# -----------------------------

st.subheader("Trends")

trend_cols = st.columns(2)

if filtered["timestamp"].notna().any():
    vol = (
        filtered.groupby(["minute", "level"], dropna=True)
        .size()
        .reset_index(name="count")
        .sort_values("minute")
    )
    fig_vol = px.area(
        vol,
        x="minute",
        y="count",
        color="level",
        category_orders={"level": LEVEL_ORDER},
        title="Log volume over time (per minute)",
    )
    fig_vol.update_layout(legend_title_text="Level", yaxis_title="Events")
    trend_cols[0].plotly_chart(fig_vol, width="stretch")

    per_min = filtered.groupby("minute", dropna=True).agg(total=("is_error", "size"), errors=("is_error", "sum")).reset_index()
    per_min["error_rate"] = (per_min["errors"] / per_min["total"]) * 100
    fig_er = px.line(per_min, x="minute", y="error_rate", title="Error rate over time (per minute)")
    fig_er.update_layout(yaxis_title="Error rate (%)")
    trend_cols[1].plotly_chart(fig_er, width="stretch")
else:
    trend_cols[0].info("No usable timestamps found; trend charts are disabled.")
    trend_cols[1].info("No usable timestamps found; trend charts are disabled.")


# -----------------------------
# Distribution & breakdown
# -----------------------------

st.subheader("Breakdowns")

b1, b2, b3 = st.columns(3)

level_counts = filtered["level"].value_counts().reindex(LEVEL_ORDER).dropna().reset_index()
level_counts.columns = ["Level", "Count"]
fig_levels = px.pie(level_counts, values="Count", names="Level", hole=0.5, title="Log level distribution")
b1.plotly_chart(fig_levels, width="stretch")

service_counts = filtered["service"].value_counts().head(15).reset_index()
service_counts.columns = ["Service", "Logs"]
fig_services = px.bar(service_counts, x="Service", y="Logs", title="Top services by log volume")
fig_services.update_layout(xaxis_title="", yaxis_title="Logs")
b2.plotly_chart(fig_services, width="stretch")

host_counts = filtered["host"].value_counts().head(15).reset_index()
host_counts.columns = ["Host", "Logs"]
fig_hosts = px.bar(host_counts, x="Host", y="Logs", title="Top hosts by log volume")
fig_hosts.update_layout(xaxis_title="", yaxis_title="Logs")
b3.plotly_chart(fig_hosts, width="stretch")


st.subheader("Quality signals")

q1, q2 = st.columns(2)

if filtered["status_code"].notna().any():
    sc_df = filtered.dropna(subset=["status_code"]).copy()
    sc_df["status_code"] = sc_df["status_code"].astype(int)
    sc_counts = sc_df["status_code"].value_counts().head(20).reset_index()
    sc_counts.columns = ["Status code", "Count"]
    fig_sc = px.bar(sc_counts, x="Status code", y="Count", title="HTTP status codes (top 20)")
    q1.plotly_chart(fig_sc, width="stretch")
else:
    q1.info("No HTTP status code data available in current filters.")

if filtered["severity_score"].notna().any():
    sev = filtered.dropna(subset=["severity_score"]).copy()
    fig_sev = px.histogram(sev, x="severity_score", nbins=10, title="Severity score distribution")
    fig_sev.update_layout(xaxis_title="Severity score", yaxis_title="Events")
    q2.plotly_chart(fig_sev, width="stretch")
else:
    q2.info("No severity score data available in current filters.")


# -----------------------------
# Anomaly view (derived)
# -----------------------------

st.subheader("Anomaly detection (volume spikes)")

if filtered["timestamp"].notna().any():
    per_min_total = filtered.groupby("minute", dropna=True).size().rename("count").reset_index()
    per_min_total["mean"] = per_min_total["count"].rolling(window=30, min_periods=10).mean()
    per_min_total["std"] = per_min_total["count"].rolling(window=30, min_periods=10).std()
    per_min_total["z"] = (per_min_total["count"] - per_min_total["mean"]) / per_min_total["std"]
    per_min_total["spike"] = per_min_total["z"] >= 3.0

    fig_anom = go.Figure()
    fig_anom.add_trace(go.Scatter(x=per_min_total["minute"], y=per_min_total["count"], mode="lines", name="Events/min"))
    spikes = per_min_total[per_min_total["spike"]]
    if not spikes.empty:
        fig_anom.add_trace(
            go.Scatter(
                x=spikes["minute"],
                y=spikes["count"],
                mode="markers",
                marker=dict(size=10),
                name="Spike (z ≥ 3)",
            )
        )
    fig_anom.update_layout(title="Derived spikes based on rolling z-score", xaxis_title="", yaxis_title="Events/min")
    st.plotly_chart(fig_anom, width="stretch")
else:
    st.info("No usable timestamps found; anomaly panel is disabled.")


# -----------------------------
# Top offenders & log explorer
# -----------------------------

st.subheader("Top offenders")
o1, o2 = st.columns(2)

svc_err = (
    filtered.groupby("service", dropna=False)
    .agg(total=("service", "size"), errors=("is_error", "sum"))
    .reset_index()
)
svc_err["error_rate"] = (svc_err["errors"] / svc_err["total"]) * 100
svc_err = svc_err.sort_values(["error_rate", "errors"], ascending=False).head(15)
fig_svc_err = px.bar(svc_err, x="service", y="error_rate", title="Top services by error rate (filtered)")
fig_svc_err.update_layout(xaxis_title="", yaxis_title="Error rate (%)")
o1.plotly_chart(fig_svc_err, width="stretch")

msg_err = (
    filtered[filtered["is_error"]]
    .groupby("message", dropna=False)
    .size()
    .sort_values(ascending=False)
    .head(15)
    .reset_index(name="errors")
)
fig_msg = px.bar(msg_err, x="message", y="errors", title="Top error messages (filtered)")
fig_msg.update_layout(xaxis_title="", yaxis_title="Errors")
o2.plotly_chart(fig_msg, width="stretch")


st.subheader("Log explorer")

with st.expander("Show recent error events", expanded=True):
    view = filtered[filtered["is_error"]].copy()
    view = view.sort_values("timestamp", ascending=False).head(200)
    st.dataframe(
        view[["timestamp", "level", "service", "host", "source_type", "status_code", "message"]],
        use_container_width=True,
        hide_index=True,
    )

with st.expander("Show raw filtered dataset (sample)", expanded=False):
    sample = filtered.sort_values("timestamp", ascending=False).head(500)
    st.dataframe(sample, use_container_width=True, hide_index=True)
