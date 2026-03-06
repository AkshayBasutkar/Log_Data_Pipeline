from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from analytics import load_latest_metrics_snapshot, refresh_metrics_snapshot


st.set_page_config(layout="wide", page_title="Log Data Pipeline Dashboard")
st.title("Log Data Pipeline Dashboard")
st.caption("Dashboard reads precomputed analysis snapshots stored in MongoDB `metrics`.")


@st.cache_data(ttl=30, show_spinner=False)
def get_metrics_snapshot():
    return load_latest_metrics_snapshot()


def _frame(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


with st.sidebar:
    st.header("Metrics Snapshot")
    if st.button("Refresh Analysis", use_container_width=True):
        try:
            refresh_metrics_snapshot()
            get_metrics_snapshot.clear()
            st.success("Metrics snapshot rebuilt from MongoDB logs.")
        except Exception as exc:
            st.error(f"Failed to refresh analysis: {exc}")

snapshot = get_metrics_snapshot()
if not snapshot:
    st.warning("No metrics snapshot found in MongoDB. Use 'Refresh Analysis' to generate one from `logs_parsed`.")
    st.stop()

overview = snapshot.get("overview", {})
distributions = snapshot.get("distributions", {})
entity_analysis = snapshot.get("entity_analysis", {})
errors = snapshot.get("errors", {})
alerts = snapshot.get("alerts", [])
recent_logs = snapshot.get("recent_logs", [])

generated_at = snapshot.get("generated_at")
st.caption(f"Snapshot generated at: {generated_at}")

st.subheader("Overview")
row_1 = st.columns(4)
row_1[0].metric("Total Logs", f"{overview.get('total_logs', 0):,}")
row_1[1].metric("Errors", f"{overview.get('error_count', 0):,}")
row_1[2].metric("Warnings", f"{overview.get('warning_count', 0):,}")
row_1[3].metric("Error Rate", f"{overview.get('error_rate_pct', 0.0):.2f}%")

row_2 = st.columns(4)
row_2[0].metric("Avg Severity", "N/A" if overview.get("avg_severity") is None else f"{overview['avg_severity']:.2f}")
row_2[1].metric("Services", f"{overview.get('active_services', 0):,}")
row_2[2].metric("Components", f"{overview.get('active_components', 0):,}")
row_2[3].metric("Logs / Second", f"{overview.get('logs_per_second', 0.0):.4f}")

row_3 = st.columns(4)
row_3[0].metric("Unique IPs", f"{overview.get('unique_ips', 0):,}")
row_3[1].metric("Unique Blocks", f"{overview.get('unique_blocks', 0):,}")
row_3[2].metric("Hadoop Apps", f"{overview.get('unique_hadoop_apps', 0):,}")
row_3[3].metric(
    "Avg Processing Latency",
    "N/A" if overview.get("avg_processing_latency_sec") is None else f"{overview['avg_processing_latency_sec']:.4f}s",
)

row_4 = st.columns(4)
row_4[0].metric("Source Files", f"{overview.get('source_files', 0):,}")
row_4[1].metric("Hadoop Jobs", f"{overview.get('unique_hadoop_jobs', 0):,}")
row_4[2].metric("Top Source", overview.get("top_source_type", "N/A"))
row_4[3].metric("Top Service", overview.get("top_service", "N/A"))

st.caption(
    f"Top source: `{overview.get('top_source_type', 'N/A')}` | "
    f"Top service: `{overview.get('top_service', 'N/A')}` | "
    f"Top component: `{overview.get('top_component', 'N/A')}`"
)

tab_summary, tab_entities, tab_table = st.tabs(["Summary", "Entity Analysis", "Recent Logs"])

with tab_summary:
    left, right = st.columns(2)

    levels_df = _frame(distributions.get("levels", []))
    services_df = _frame(distributions.get("services", []))
    components_df = _frame(distributions.get("components", []))
    source_types_df = _frame(distributions.get("source_types", []))
    top_messages_df = _frame(errors.get("top_messages", []))

    with left:
        if not levels_df.empty:
            st.plotly_chart(px.bar(levels_df, x="level", y="count", title="Logs by Level"), use_container_width=True)
        if not services_df.empty:
            st.plotly_chart(px.bar(services_df, x="service", y="count", title="Top Services"), use_container_width=True)

    with right:
        if not source_types_df.empty:
            st.plotly_chart(px.pie(source_types_df, names="source_type", values="count", title="Logs by Source Type"), use_container_width=True)
        if not components_df.empty:
            st.plotly_chart(px.bar(components_df, x="component", y="count", title="Top Components"), use_container_width=True)

    if not top_messages_df.empty:
        st.plotly_chart(
            px.bar(top_messages_df, x="count", y="message", orientation="h", title="Top Error Messages"),
            use_container_width=True,
        )

with tab_entities:
    left, right = st.columns(2)

    entity_specs = [
        ("blocks", "blocks_involved", "Most Active HDFS Blocks"),
        ("hadoop_applications", "applications_involved", "Most Active Hadoop Applications"),
        ("spark_stages_errors", "stages_involved", "Spark Stages with Errors"),
        ("windows_kbs_errors", "kbs_involved", "Windows KBs with Errors"),
        ("ips", "ips_involved", "Most Active IPs"),
        ("hadoop_jobs", "jobs_involved", "Most Active Hadoop Jobs"),
        ("spark_tasks_errors", "tasks_involved", "Spark Tasks with Errors"),
        ("windows_dlls_errors", "dlls_involved", "Windows DLLs with Errors"),
        ("hadoop_containers", "containers_involved", "Most Active Hadoop Containers"),
    ]

    frames = {
        key: _frame(entity_analysis.get(key, []))
        for key, _, _ in entity_specs
    }

    for key, column, title in entity_specs[:4]:
        df = frames[key]
        if not df.empty:
            left.plotly_chart(px.bar(df, x=column, y="count", title=title), use_container_width=True)

    for key, column, title in entity_specs[4:]:
        df = frames[key]
        if not df.empty:
            right.plotly_chart(px.bar(df, x=column, y="count", title=title), use_container_width=True)

    alerts_df = _frame(alerts)
    if alerts_df.empty:
        st.info("No alerts found in MongoDB.")
    else:
        st.subheader("Alerts")
        st.dataframe(alerts_df, use_container_width=True, hide_index=True)

with tab_table:
    recent_logs_df = _frame(recent_logs)
    recent_errors_df = _frame(errors.get("recent", []))

    st.subheader("Recent Processed Logs")
    if recent_logs_df.empty:
        st.info("No recent logs available in the latest snapshot.")
    else:
        st.dataframe(recent_logs_df, use_container_width=True, hide_index=True)

    st.subheader("Recent Errors")
    if recent_errors_df.empty:
        st.info("No recent errors available in the latest snapshot.")
    else:
        st.dataframe(recent_errors_df, use_container_width=True, hide_index=True)
