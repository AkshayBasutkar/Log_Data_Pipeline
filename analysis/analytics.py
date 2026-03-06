import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

try:
    from pymongo import DESCENDING, MongoClient
    from pymongo.errors import PyMongoError
except ModuleNotFoundError:
    DESCENDING = -1
    MongoClient = None
    PyMongoError = Exception


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("LOG_PIPELINE_DB_NAME", "log_pipeline")
METRICS_COLLECTION = "metrics"
LEVEL_ORDER = ["CRITICAL", "ERROR", "FATAL", "WARNING", "WARN", "INFO", "DEBUG"]
LIST_COLUMNS = [
    "blocks_involved",
    "ips_involved",
    "applications_involved",
    "app_attempts_involved",
    "containers_involved",
    "jobs_involved",
    "tids_involved",
    "stages_involved",
    "tasks_involved",
    "rdds_involved",
    "kbs_involved",
    "dlls_involved",
    "hex_codes",
]
TEXT_DEFAULTS = {
    "level": "INFO",
    "service": "unknown-service",
    "component": "unknown-component",
    "message": "",
    "source_type": "unknown",
    "category": "general",
    "source_file": "unknown",
}


def _get_client():
    if MongoClient is None:
        raise PyMongoError("pymongo is not installed")

    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=2000,
        connectTimeoutMS=2000,
        socketTimeoutMS=2000,
    )


def _get_db():
    return _get_client()[DB_NAME]


def _to_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if parsed.isna().all():
        parsed = pd.to_datetime(series, errors="coerce")
    return parsed


def _ensure_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if pd.isna(value):
        return []
    return [value]


def _safe_mode(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned.ne("")]
    if cleaned.empty:
        return "N/A"
    return cleaned.value_counts().idxmax()


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    serializable = df.copy()
    for column in serializable.columns:
        if pd.api.types.is_datetime64_any_dtype(serializable[column]):
            serializable[column] = serializable[column].map(
                lambda value: value.to_pydatetime() if pd.notna(value) else None
            )
    return serializable.to_dict(orient="records")


def _counts(df: pd.DataFrame, column: str, top_n: int = 10) -> pd.DataFrame:
    if column not in df.columns or df.empty:
        return pd.DataFrame(columns=[column, "count"])
    return (
        df[column]
        .value_counts()
        .head(top_n)
        .rename_axis(column)
        .reset_index(name="count")
    )


def _exploded_counts(
    df: pd.DataFrame,
    column: str,
    top_n: int = 10,
    errors_only: bool = False,
) -> pd.DataFrame:
    if column not in df.columns or df.empty:
        return pd.DataFrame(columns=[column, "count"])

    source = df[df["is_error"]] if errors_only else df
    exploded = source.explode(column)
    exploded = exploded[exploded[column].notna() & exploded[column].astype(str).ne("")]

    if exploded.empty:
        return pd.DataFrame(columns=[column, "count"])

    return (
        exploded[column]
        .value_counts()
        .head(top_n)
        .rename_axis(column)
        .reset_index(name="count")
    )


def load_logs_from_mongo() -> pd.DataFrame:
    logs = list(_get_db().logs_parsed.find({}, {"_id": 0}))
    return pd.DataFrame(logs)


def load_alerts_from_mongo() -> pd.DataFrame:
    try:
        alerts = list(_get_db().alerts.find({}, {"_id": 0}))
        return pd.DataFrame(alerts)
    except Exception:
        return pd.DataFrame()


def normalize_logs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()

    if "timestamp" not in out.columns:
        out["timestamp"] = pd.NaT
    out["timestamp"] = _to_datetime(out["timestamp"])

    if "ingested_at" not in out.columns:
        out["ingested_at"] = pd.NaT
    out["ingested_at"] = _to_datetime(out["ingested_at"])

    if "processed_at" not in out.columns:
        out["processed_at"] = pd.NaT
    out["processed_at"] = _to_datetime(out["processed_at"])

    for column, default_value in TEXT_DEFAULTS.items():
        if column not in out.columns:
            out[column] = default_value
        out[column] = out[column].fillna(default_value).astype(str).str.strip()

    out["level"] = out["level"].str.upper()

    for column in LIST_COLUMNS:
        if column not in out.columns:
            out[column] = [[] for _ in range(len(out))]
        out[column] = out[column].map(_ensure_list)

    if "severity_score" not in out.columns:
        out["severity_score"] = pd.NA
    out["severity_score"] = pd.to_numeric(out["severity_score"], errors="coerce")

    out["primary_ip"] = out["ips_involved"].map(lambda values: values[0] if values else "N/A")
    out["is_error"] = out["level"].isin(["ERROR", "CRITICAL", "FATAL"])
    out["is_warning"] = out["level"].isin(["WARN", "WARNING"])

    if out["timestamp"].notna().any():
        out["minute"] = out["timestamp"].dt.floor("min")
    else:
        out["minute"] = pd.NaT

    out["processing_latency_sec"] = (
        out["processed_at"] - out["ingested_at"]
    ).dt.total_seconds()

    return out


def compute_metrics(df: pd.DataFrame) -> dict[str, Any]:
    total_logs = int(len(df))
    errors_df = df[df["is_error"]]
    warnings_df = df[df["is_warning"]]

    metrics: dict[str, Any] = {
        "total_logs": total_logs,
        "error_count": int(len(errors_df)),
        "warning_count": int(len(warnings_df)),
        "error_rate_pct": (len(errors_df) / total_logs * 100) if total_logs else 0.0,
        "avg_severity": float(df["severity_score"].dropna().mean()) if df["severity_score"].notna().any() else None,
        "active_services": int(df["service"].nunique()),
        "active_components": int(df["component"].nunique()),
        "active_sources": int(df["source_type"].nunique()),
        "source_files": int(df["source_file"].nunique()),
        "unique_ips": int(df.explode("ips_involved")["ips_involved"].dropna().replace("", pd.NA).dropna().nunique()),
        "unique_blocks": int(df.explode("blocks_involved")["blocks_involved"].dropna().replace("", pd.NA).dropna().nunique()),
        "unique_hadoop_apps": int(df.explode("applications_involved")["applications_involved"].dropna().replace("", pd.NA).dropna().nunique()),
        "unique_hadoop_jobs": int(df.explode("jobs_involved")["jobs_involved"].dropna().replace("", pd.NA).dropna().nunique()),
        "top_service": _safe_mode(df["service"]),
        "top_component": _safe_mode(df["component"]),
        "top_source_type": _safe_mode(df["source_type"]),
        "top_error_message": _safe_mode(errors_df["message"]) if not errors_df.empty else "N/A",
        "avg_processing_latency_sec": (
            float(df["processing_latency_sec"].dropna().mean())
            if df["processing_latency_sec"].notna().any()
            else None
        ),
    }

    if df["timestamp"].notna().any():
        min_ts = df["timestamp"].min()
        max_ts = df["timestamp"].max()
        span_seconds = max((max_ts - min_ts).total_seconds(), 1.0)
        metrics["logs_per_second"] = total_logs / span_seconds
        metrics["time_window_start"] = min_ts.to_pydatetime()
        metrics["time_window_end"] = max_ts.to_pydatetime()
    else:
        metrics["logs_per_second"] = 0.0
        metrics["time_window_start"] = None
        metrics["time_window_end"] = None

    return metrics


def build_level_distribution(df: pd.DataFrame) -> pd.DataFrame:
    levels = (
        df["level"]
        .value_counts()
        .rename_axis("level")
        .reset_index(name="count")
    )
    levels["sort_order"] = levels["level"].map(
        {level: index for index, level in enumerate(LEVEL_ORDER)}
    ).fillna(len(LEVEL_ORDER))
    return levels.sort_values(["sort_order", "count"], ascending=[True, False]).drop(columns=["sort_order"])


def build_time_series(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df["minute"].isna().all():
        return pd.DataFrame(columns=["minute", "total_logs", "error_logs", "warning_logs", "error_rate_pct"])

    series = (
        df.groupby("minute", dropna=True)
        .agg(
            total_logs=("message", "size"),
            error_logs=("is_error", "sum"),
            warning_logs=("is_warning", "sum"),
        )
        .reset_index()
    )
    series["error_rate_pct"] = (series["error_logs"] / series["total_logs"]) * 100
    return series


def build_time_series_by_source(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df["minute"].isna().all():
        return pd.DataFrame(
            columns=["minute", "source_type", "total_logs", "error_logs", "warning_logs", "error_rate_pct"]
        )

    series = (
        df.groupby(["minute", "source_type"], dropna=True)
        .agg(
            total_logs=("message", "size"),
            error_logs=("is_error", "sum"),
            warning_logs=("is_warning", "sum"),
        )
        .reset_index()
    )
    series["error_rate_pct"] = (series["error_logs"] / series["total_logs"]) * 100
    return series


def create_metrics_indexes():
    db = _get_db()
    db[METRICS_COLLECTION].create_index([("generated_at", DESCENDING)])
    db[METRICS_COLLECTION].create_index([("snapshot_type", DESCENDING)])


def build_metrics_snapshot(top_n: int = 10, recent_limit: int = 100) -> dict[str, Any]:
    logs_df = normalize_logs(load_logs_from_mongo())
    alerts_df = load_alerts_from_mongo()

    if logs_df.empty:
        raise ValueError("No logs found in MongoDB collection `logs_parsed`.")

    errors_df = logs_df[logs_df["is_error"]]
    metrics = compute_metrics(logs_df)

    top_errors = (
        errors_df["message"]
        .value_counts()
        .head(top_n)
        .rename_axis("message")
        .reset_index(name="count")
    ) if not errors_df.empty else pd.DataFrame(columns=["message", "count"])

    snapshot = {
        "snapshot_type": "dashboard_metrics",
        "generated_at": datetime.now(timezone.utc),
        "overview": metrics,
        "distributions": {
            "levels": _records(build_level_distribution(logs_df)),
            "services": _records(_counts(logs_df, "service", top_n=top_n)),
            "components": _records(_counts(logs_df, "component", top_n=top_n)),
            "source_types": _records(_counts(logs_df, "source_type", top_n=top_n)),
        },
        "time_series": {
            "by_minute": _records(build_time_series(logs_df)),
            "by_minute_by_source": _records(build_time_series_by_source(logs_df)),
        },
        "entity_analysis": {
            "blocks": _records(_exploded_counts(logs_df, "blocks_involved", top_n=top_n)),
            "ips": _records(_exploded_counts(logs_df, "ips_involved", top_n=top_n)),
            "hadoop_applications": _records(_exploded_counts(logs_df, "applications_involved", top_n=top_n)),
            "hadoop_jobs": _records(_exploded_counts(logs_df, "jobs_involved", top_n=top_n)),
            "hadoop_containers": _records(_exploded_counts(logs_df, "containers_involved", top_n=top_n)),
            "spark_stages_errors": _records(_exploded_counts(logs_df, "stages_involved", top_n=top_n, errors_only=True)),
            "spark_tasks_errors": _records(_exploded_counts(logs_df, "tasks_involved", top_n=top_n, errors_only=True)),
            "windows_kbs_errors": _records(_exploded_counts(logs_df, "kbs_involved", top_n=top_n, errors_only=True)),
            "windows_dlls_errors": _records(_exploded_counts(logs_df, "dlls_involved", top_n=top_n, errors_only=True)),
        },
        "errors": {
            "top_messages": _records(top_errors),
            "recent": _records(
                errors_df.sort_values("timestamp", ascending=False)
                .head(recent_limit)[
                    [
                        column
                        for column in [
                            "timestamp",
                            "level",
                            "source_type",
                            "service",
                            "component",
                            "source_file",
                            "primary_ip",
                            "message",
                        ]
                        if column in errors_df.columns
                    ]
                ]
            ),
        },
        "recent_logs": _records(
            logs_df.sort_values("timestamp", ascending=False)
            .head(recent_limit)[
                [
                    column
                    for column in [
                        "timestamp",
                        "level",
                        "source_type",
                        "service",
                        "component",
                        "source_file",
                        "primary_ip",
                        "message",
                    ]
                    if column in logs_df.columns
                ]
            ]
        ),
        "alerts": _records(alerts_df),
    }

    return snapshot


def persist_metrics_snapshot(snapshot: dict[str, Any]) -> Any:
    create_metrics_indexes()
    result = _get_db()[METRICS_COLLECTION].insert_one(snapshot)
    return result.inserted_id


def refresh_metrics_snapshot(top_n: int = 10, recent_limit: int = 100) -> dict[str, Any]:
    snapshot = build_metrics_snapshot(top_n=top_n, recent_limit=recent_limit)
    inserted_id = persist_metrics_snapshot(snapshot)
    snapshot["_id"] = inserted_id
    return snapshot


def load_latest_metrics_snapshot() -> dict[str, Any] | None:
    return _get_db()[METRICS_COLLECTION].find_one(
        {"snapshot_type": "dashboard_metrics"},
        sort=[("generated_at", DESCENDING)],
        projection={"_id": 0},
    )
