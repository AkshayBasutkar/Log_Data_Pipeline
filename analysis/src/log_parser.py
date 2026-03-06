import re
import json
from datetime import datetime, timezone

# ------------------------
# Apache Parser
# ------------------------

apache_pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+)'


def _severity_from_level(level: str) -> int:
    lvl = (level or "").upper()
    return {
        "DEBUG": 1,
        "INFO": 2,
        "WARNING": 4,
        "WARN": 4,
        "ERROR": 7,
        "CRITICAL": 9,
        "FATAL": 9,
    }.get(lvl, 3)


def _parse_apache_time(value: str):
    # Example: 06/Mar/2026:10:15:32 (no timezone in sample logs)
    try:
        return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_iso_time(value: str):
    # Handles "2026-03-06T10:15:32" and similar.
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_syslog_time(value: str):
    # Example: "Mar 06 10:15:34" (no year in syslog; assume current year)
    try:
        now = datetime.now(timezone.utc)
        dt = datetime.strptime(f"{now.year} {value}", "%Y %b %d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def parse_apache(line):

    match = re.match(apache_pattern, line)

    if not match:
        return None

    ip, time, request, status = match.groups()
    ts = _parse_apache_time(time) or datetime.now(timezone.utc)
    status_code = int(status)
    level = "ERROR" if status_code >= 500 else "INFO"

    return {
        "timestamp": ts,
        "level": level,
        "service": "api",
        "host": ip,
        "message": request,
        "status_code": status_code,
        "response_time_ms": None,
        "source_type": "apache",
        "severity_score": _severity_from_level(level),
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.now(timezone.utc),
    }


# ------------------------
# JSON App Logs
# ------------------------

def parse_app_json(line):

    data = json.loads(line)
    level = (data.get("level", "INFO") or "INFO").upper()
    ts = _parse_iso_time(str(data.get("timestamp", "")).strip()) or datetime.now(timezone.utc)

    return {
        "timestamp": ts,
        "level": level,
        "service": data.get("service", "app"),
        "host": "app-server",
        "message": data.get("message"),
        "status_code": None,
        "response_time_ms": None,
        "source_type": "app",
        "severity_score": _severity_from_level(level),
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.now(timezone.utc),
    }


# ------------------------
# Syslog Parser
# ------------------------

def parse_syslog(line):

    # Example: "Mar 06 10:15:34 server01 kernel: CPU usage critical: 98%"
    m = re.match(r"^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+([^:]+):\s*(.*)$", line)
    if m:
        time_part, host, service_part, msg = m.groups()
        ts = _parse_syslog_time(time_part) or datetime.now(timezone.utc)
        service = (service_part or "kernel").strip()
        message = msg.strip()
    else:
        ts = datetime.now(timezone.utc)
        host = "server01"
        service = "kernel"
        message = line

    level = "CRITICAL" if "critical" in message.lower() else "WARNING" if "high" in message.lower() else "INFO"
    return {
        "timestamp": ts,
        "level": level,
        "service": service,
        "host": host,
        "message": message,
        "status_code": None,
        "response_time_ms": None,
        "source_type": "syslog",
        "severity_score": _severity_from_level(level),
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.now(timezone.utc),
    }