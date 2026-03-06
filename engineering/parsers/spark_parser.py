import re
from datetime import datetime

# Spark log structure
LOG_PATTERN = re.compile(
    r"^(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2}:\d{2})\s+([A-Z]+)\s+(.*?):\s+(.*)$"
)

# Entity extraction
TID_PATTERN = re.compile(r"TID\s+(\d+)")
STAGE_PATTERN = re.compile(r"stage\s+(\d+(?:\.\d+)?)", re.IGNORECASE)
TASK_PATTERN = re.compile(r"task\s+(\d+(?:\.\d+)?)", re.IGNORECASE)
RDD_PATTERN = re.compile(r"(rdd_\d+_\d+)", re.IGNORECASE)


def parse_spark(log_line: str):
    """
    Parse a single Spark log line into structured JSON.
    """

    match = LOG_PATTERN.match(log_line)

    if not match:
        return None

    date, time, level, component, message = match.groups()

    # Convert timestamp
    try:
        timestamp = datetime.strptime(
            f"{date} {time}",
            "%y/%m/%d %H:%M:%S"
        )
    except:
        timestamp = None

    tids = list(set(TID_PATTERN.findall(message)))
    stages = list(set(STAGE_PATTERN.findall(message)))
    tasks = list(set(TASK_PATTERN.findall(message)))
    rdds = list(set(RDD_PATTERN.findall(message)))

    parsed_log = {
        "timestamp": timestamp,
        "date": date,
        "time": time,
        "level": level.upper(),
        "component": component,
        "message": message,
        "tids_involved": tids,
        "stages_involved": stages,
        "tasks_involved": tasks,
        "rdds_involved": rdds,
        "source_type": "spark",
        "ingested_at": datetime.utcnow()
    }

    return parsed_log


def append_multiline(previous_log, line):
    """
    Handle multiline Spark logs (Java/Scala stack traces).
    """

    if not previous_log:
        return previous_log

    previous_log["message"] += " " + line

    previous_log["tids_involved"].extend(TID_PATTERN.findall(line))
    previous_log["stages_involved"].extend(STAGE_PATTERN.findall(line))
    previous_log["tasks_involved"].extend(TASK_PATTERN.findall(line))
    previous_log["rdds_involved"].extend(RDD_PATTERN.findall(line))

    previous_log["tids_involved"] = list(set(previous_log["tids_involved"]))
    previous_log["stages_involved"] = list(set(previous_log["stages_involved"]))
    previous_log["tasks_involved"] = list(set(previous_log["tasks_involved"]))
    previous_log["rdds_involved"] = list(set(previous_log["rdds_involved"]))

    return previous_log


