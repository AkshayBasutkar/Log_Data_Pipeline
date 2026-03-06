import re
from datetime import datetime


# ------------------------------------------------
# Severity Scoring
# ------------------------------------------------

LEVEL_SCORES = {
    "INFO": 1,
    "DEBUG": 1,
    "WARN": 4,
    "WARNING": 4,
    "ERROR": 7,
    "FATAL": 9,
    "CRITICAL": 10
}


# ------------------------------------------------
# Service Mapping
# ------------------------------------------------

SERVICE_MAP = {
    "hdfs": "storage-service",
    "hadoop": "compute-service",
    "spark": "analytics-service",
    "windows": "system-service"
}


# ------------------------------------------------
# Category Keyword Patterns (Word Boundary Safe)
# ------------------------------------------------

CATEGORY_PATTERNS = {
    "network": [
        r"\bip\b",
        r"\bconnection\b",
        r"\bsocket\b"
    ],
    "storage": [
        r"\bblock\b",
        r"\bdisk\b",
        r"\bfilesystem\b"
    ],
    "execution": [
        r"\btask\b",
        r"\bstage\b",
        r"\bexecutor\b"
    ],
    "security": [
        r"\bauthentication\b",
        r"\baccess\b",
        r"\bpermission\b"
    ],
    "system": [
        r"\bupdate\b",
        r"\binstall\b",
        r"\bpackage\b"
    ]
}

# Precompile regex patterns for performance
CATEGORY_REGEX = {
    category: [re.compile(p, re.IGNORECASE) for p in patterns]
    for category, patterns in CATEGORY_PATTERNS.items()
}


def compute_severity(level: str) -> int:
    """
    Convert log level into numeric severity score.
    """

    return LEVEL_SCORES.get(level.upper(), 2)


def detect_category(message: str) -> str:
    """
    Categorize logs based on message content using
    regex word-boundary matching to avoid false positives.
    """

    for category, patterns in CATEGORY_REGEX.items():

        for pattern in patterns:

            if pattern.search(message):
                return category

    return "general"


def enrich_log(log: dict) -> dict:
    """
    Add derived metadata fields to a parsed log.
    """

    level = log.get("level", "INFO")
    source = log.get("source_type", "unknown")
    message = log.get("message", "")

    log["severity_score"] = compute_severity(level)

    log["service"] = SERVICE_MAP.get(source, "unknown-service")

    log["category"] = detect_category(message)

    log["processed_at"] = datetime.utcnow()

    return log


if __name__ == "__main__":

    sample = {
        "timestamp": "2026-03-06T10:14:22",
        "level": "ERROR",
        "message": "Executor failed to process task 4.0 in stage 2.0",
        "source_type": "spark"
    }

    enriched = enrich_log(sample)

    from pprint import pprint
    pprint(enriched)
