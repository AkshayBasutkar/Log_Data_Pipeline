"""
format_detector.py

Detects the source system of a log line.
Supports classification for:

- HDFS logs
- Hadoop logs
- Spark logs
- Windows logs

Detection Strategy:
1. Timestamp Regex Detection
2. Component Identification
3. Service Keyword Matching
4. Fallback Classification
"""

import re

# ------------------------------------------------
# Phase 1: Timestamp Patterns
# ------------------------------------------------

HDFS_TIMESTAMP = re.compile(r'^\d{6}\s\d{6}')
SPARK_TIMESTAMP = re.compile(r'^\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}')
WINDOWS_TIMESTAMP = re.compile(r'^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},')


# ------------------------------------------------
# Phase 2: Component Identifiers
# ------------------------------------------------

HADOOP_COMPONENTS = [
    "org.apache.hadoop.",
    "mapreduce",
    "yarn.",
    "mrappmaster",
    "jobhistory",
    "asyncdispatcher",
]

HDFS_COMPONENTS = [
    "dfs.datanode",
    "dfs.fsnamesystem",
    "dfs.datablockscanner"
]

SPARK_COMPONENTS = [
    "executor.",
    "spark.",
    "tasksetmanager",
    "dagscheduler",
    "memorystore",
    "torrentbroadcast"
]

WINDOWS_COMPONENTS = [
    "cbs",
    "csi"
]


# ------------------------------------------------
# Phase 3: Keyword Identifiers
# ------------------------------------------------

HADOOP_KEYWORDS = [
    "application_",
    "appattempt_",
    "container_",
    "job_",
    "hadoop",
    "yarn",
]

HDFS_KEYWORDS = [
    "blk_",
    "receiving block",
    "addstoredblock"
]

SPARK_KEYWORDS = [
    "tid",
    "rdd_",
    "bytes result sent",
    "broadcast"
]

WINDOWS_KEYWORDS = [
    "package_for_kb",
    "hresult",
    "trustedinstaller",
    "windowsupdateagent",
    "wcpinitialize"
]


# ------------------------------------------------
# Main Detection Function
# ------------------------------------------------

def _matches_any(candidates: list[str], line: str) -> bool:
    return any(candidate in line for candidate in candidates)


def detect_format(log_line: str) -> str:
    """
    Detect the log source format.

    Args:
        log_line (str): Raw log line

    Returns:
        str: One of the following
            - 'hdfs'
            - 'hadoop'
            - 'spark'
            - 'windows'
            - 'unknown'
    """

    # Performance optimization: only scan first part of log
    line = log_line[:80].lower()

    # ----------------------------
    # Phase 1: Timestamp Detection
    # ----------------------------

    if HDFS_TIMESTAMP.match(line):
        return "hdfs"

    if SPARK_TIMESTAMP.match(line):
        return "spark"

    if WINDOWS_TIMESTAMP.match(line):
        if _matches_any(HADOOP_COMPONENTS, line) or _matches_any(HADOOP_KEYWORDS, line):
            return "hadoop"
        if _matches_any(WINDOWS_COMPONENTS, line) or _matches_any(WINDOWS_KEYWORDS, line):
            return "windows"

    # ----------------------------
    # Phase 2: Component Matching
    # ----------------------------

    for component in HADOOP_COMPONENTS:
        if component in line:
            return "hadoop"

    for component in HDFS_COMPONENTS:
        if component in line:
            return "hdfs"

    for component in SPARK_COMPONENTS:
        if component in line:
            return "spark"

    for component in WINDOWS_COMPONENTS:
        if component in line:
            return "windows"

    # ----------------------------
    # Phase 3: Keyword Matching
    # ----------------------------

    for keyword in HADOOP_KEYWORDS:
        if keyword in line:
            return "hadoop"

    for keyword in HDFS_KEYWORDS:
        if keyword in line:
            return "hdfs"

    for keyword in SPARK_KEYWORDS:
        if keyword in line:
            return "spark"

    for keyword in WINDOWS_KEYWORDS:
        if keyword in line:
            return "windows"

    return "unknown"


# ------------------------------------------------
# Error Detection Utility (used later in alerts)
# ------------------------------------------------

ERROR_KEYWORDS = [
    "error",
    "fail",
    "fatal",
    "exception",
    "critical"
]


def is_error(log_line: str) -> bool:
    """
    Detect whether a log line represents an error.

    Args:
        log_line (str)

    Returns:
        bool
    """

    line = log_line.lower()

    return any(word in line for word in ERROR_KEYWORDS)
