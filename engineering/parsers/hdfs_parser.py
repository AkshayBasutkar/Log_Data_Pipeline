import re
from datetime import datetime


# Base HDFS log structure
LOG_PATTERN = re.compile(
    r"^(\d{6})\s+(\d{6})\s+(\d+)\s+([A-Z]+)\s+(.*?):\s+(.*)$"
)

# Extract entities
BLOCK_PATTERN = re.compile(r"(blk_-?\d+)")
IP_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+)")


def parse_hdfs(log_line: str):
    """
    Parse a single HDFS log line into structured JSON.
    """

    match = LOG_PATTERN.match(log_line)

    if not match:
        return None

    date, time, pid, level, component, message = match.groups()

    # Convert timestamp to ISO format
    try:
        timestamp = datetime.strptime(date + time, "%y%m%d%H%M%S")
    except:
        timestamp = None

    # Extract operational entities
    blocks = list(set(BLOCK_PATTERN.findall(message)))
    ips = list(set(IP_PATTERN.findall(message)))

    parsed_log = {
        "timestamp": timestamp,
        "date": date,
        "time": time,
        "pid": int(pid),
        "level": level,
        "component": component,
        "message": message,
        "blocks_involved": blocks,
        "ips_involved": ips,
        "source_type": "hdfs",
        "ingested_at": datetime.utcnow()
    }

    return parsed_log


def append_multiline(previous_log, line):
    """
    Handle multi-line logs such as stack traces.
    """

    if not previous_log:
        return previous_log

    previous_log["message"] += " " + line

    # Extract additional entities
    previous_log["blocks_involved"].extend(BLOCK_PATTERN.findall(line))
    previous_log["ips_involved"].extend(IP_PATTERN.findall(line))

    # Remove duplicates
    previous_log["blocks_involved"] = list(set(previous_log["blocks_involved"]))
    previous_log["ips_involved"] = list(set(previous_log["ips_involved"]))

    return previous_log


