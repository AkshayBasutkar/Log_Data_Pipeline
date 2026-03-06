import re
from datetime import datetime


LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})\s+"
    r"(\d{2}:\d{2}:\d{2}),(\d{3})\s+"
    r"([A-Z]+)\s+\[(.*?)\]\s+"
    r"(.*?):\s+(.*)$"
)

APPLICATION_PATTERN = re.compile(r"\bapplication_\d+_\d+\b", re.IGNORECASE)
APP_ATTEMPT_PATTERN = re.compile(r"\bappattempt_\d+_\d+_\d+\b", re.IGNORECASE)
CONTAINER_PATTERN = re.compile(r"\bcontainer_\d+_\d+_\d+_\d+\b", re.IGNORECASE)
JOB_PATTERN = re.compile(r"\bjob_\d+_\d+\b", re.IGNORECASE)
TASK_PATTERN = re.compile(r"\btask_\d+_\d+_[mr]_\d+\b", re.IGNORECASE)
IP_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+)")


def parse_hadoop(log_line: str):
    """
    Parse a single Hadoop/YARN/MapReduce log line into structured JSON.
    """

    match = LOG_PATTERN.match(log_line)
    if not match:
        return None

    date, time, millis, level, thread, component, message = match.groups()

    try:
        timestamp = datetime.strptime(
            f"{date} {time},{millis}",
            "%Y-%m-%d %H:%M:%S,%f",
        )
    except ValueError:
        timestamp = None

    parsed_log = {
        "timestamp": timestamp,
        "date": date,
        "time": f"{time},{millis}",
        "level": level.upper(),
        "thread": thread.strip(),
        "component": component.strip(),
        "message": message,
        "applications_involved": list(set(APPLICATION_PATTERN.findall(message))),
        "app_attempts_involved": list(set(APP_ATTEMPT_PATTERN.findall(message))),
        "containers_involved": list(set(CONTAINER_PATTERN.findall(message))),
        "jobs_involved": list(set(JOB_PATTERN.findall(message))),
        "tasks_involved": list(set(TASK_PATTERN.findall(message))),
        "ips_involved": list(set(IP_PATTERN.findall(message))),
        "source_type": "hadoop",
        "ingested_at": datetime.utcnow(),
    }

    return parsed_log


def append_multiline(previous_log, line):
    """
    Handle multiline Hadoop stack traces and extend extracted entities.
    """

    if not previous_log:
        return previous_log

    previous_log["message"] += " " + line
    previous_log["applications_involved"].extend(APPLICATION_PATTERN.findall(line))
    previous_log["app_attempts_involved"].extend(APP_ATTEMPT_PATTERN.findall(line))
    previous_log["containers_involved"].extend(CONTAINER_PATTERN.findall(line))
    previous_log["jobs_involved"].extend(JOB_PATTERN.findall(line))
    previous_log["tasks_involved"].extend(TASK_PATTERN.findall(line))
    previous_log["ips_involved"].extend(IP_PATTERN.findall(line))

    for key in [
        "applications_involved",
        "app_attempts_involved",
        "containers_involved",
        "jobs_involved",
        "tasks_involved",
        "ips_involved",
    ]:
        previous_log[key] = list(set(previous_log[key]))

    return previous_log
