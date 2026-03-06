import re
import json
from datetime import datetime

# ------------------------
# Apache Parser
# ------------------------

apache_pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+)'

def parse_apache(line):

    match = re.match(apache_pattern, line)

    if not match:
        return None

    ip, time, request, status = match.groups()

    return {
        "timestamp": datetime.utcnow(),
        "level": "ERROR" if int(status) >= 500 else "INFO",
        "service": "api",
        "host": ip,
        "message": request,
        "status_code": int(status),
        "response_time_ms": None,
        "source_type": "apache",
        "severity_score": 5,
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.utcnow()
    }


# ------------------------
# JSON App Logs
# ------------------------

def parse_app_json(line):

    data = json.loads(line)

    return {
        "timestamp": datetime.utcnow(),
        "level": data.get("level", "INFO"),
        "service": data.get("service", "app"),
        "host": "app-server",
        "message": data.get("message"),
        "status_code": None,
        "response_time_ms": None,
        "source_type": "app",
        "severity_score": 6,
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.utcnow()
    }


# ------------------------
# Syslog Parser
# ------------------------

def parse_syslog(line):

    return {
        "timestamp": datetime.utcnow(),
        "level": "CRITICAL",
        "service": "kernel",
        "host": "server01",
        "message": line,
        "status_code": None,
        "response_time_ms": None,
        "source_type": "syslog",
        "severity_score": 8,
        "anomaly_score": None,
        "is_anomaly": False,
        "ingested_at": datetime.utcnow()
    }