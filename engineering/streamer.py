import time
from pathlib import Path


def stream_logs(file_path: str, delay: float = 0.2):
    """
    Streams logs from a file line by line with a delay to simulate
    real-time log ingestion.

    Args:
        file_path (str): Path to the log file
        delay (float): Delay between logs in seconds

    Yields:
        str: A single log line
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {file_path}")

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            log_line = line.strip()

            if log_line:
                yield log_line
                time.sleep(delay)


def stream_multiple_logs(file_paths, delay=0.2):
    """
    Streams logs from multiple files sequentially.

    Args:
        file_paths (list): List of log file paths
        delay (float): Delay between logs

    Yields:
        tuple: (file_name, log_line)
    """

    for file_path in file_paths:
        file_name = Path(file_path).name

        for log in stream_logs(file_path, delay):
            yield file_name, log


if __name__ == "__main__":

    log_files = [
        "logs/HDFS.log",
        "logs/Windows.log",
        "logs/Spark.log"
    ]

    for source, log in stream_multiple_logs(log_files, delay=0.1):
        print(f"[{source}] {log}")