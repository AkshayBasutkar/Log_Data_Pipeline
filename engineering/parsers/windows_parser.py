import re
from datetime import datetime

# Windows CBS / CSI log structure
LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\s+([A-Za-z]+)\s+([A-Z]+)\s+(.*)$"
)

# Entity extraction patterns
KB_PATTERN = re.compile(r"(KB\d{6,8})")
DLL_PATTERN = re.compile(r"([a-zA-Z0-9_.-]+\.dll)", re.IGNORECASE)
HEX_PATTERN = re.compile(r"(0x[0-9a-fA-F]+)")
IP_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+)")


def parse_windows(log_line: str):
    """
    Parse a single Windows log line into structured JSON.
    """

    match = LOG_PATTERN.match(log_line)

    if not match:
        return None

    date, time, level, component, message = match.groups()

    # Convert timestamp
    try:
        timestamp = datetime.strptime(
            f"{date} {time}",
            "%Y-%m-%d %H:%M:%S"
        )
    except:
        timestamp = None

    # Extract entities
    kbs = list(set(KB_PATTERN.findall(message)))
    dlls = list(set(DLL_PATTERN.findall(message)))
    hex_codes = list(set(HEX_PATTERN.findall(message)))
    ips = list(set(IP_PATTERN.findall(message)))

    parsed_log = {
        "timestamp": timestamp,
        "date": date,
        "time": time,
        "level": level.upper(),
        "component": component.strip(),
        "message": message,
        "kbs_involved": kbs,
        "dlls_involved": dlls,
        "hex_codes": hex_codes,
        "ips_involved": ips,
        "source_type": "windows",
        "ingested_at": datetime.utcnow()
    }

    return parsed_log


def append_multiline(previous_log, line):
    """
    Handle multiline Windows logs such as stack traces.
    """

    if not previous_log:
        return previous_log

    previous_log["message"] += " " + line

    previous_log["kbs_involved"].extend(KB_PATTERN.findall(line))
    previous_log["dlls_involved"].extend(DLL_PATTERN.findall(line))
    previous_log["hex_codes"].extend(HEX_PATTERN.findall(line))
    previous_log["ips_involved"].extend(IP_PATTERN.findall(line))

    previous_log["kbs_involved"] = list(set(previous_log["kbs_involved"]))
    previous_log["dlls_involved"] = list(set(previous_log["dlls_involved"]))
    previous_log["hex_codes"] = list(set(previous_log["hex_codes"]))
    previous_log["ips_involved"] = list(set(previous_log["ips_involved"]))

    return previous_log


