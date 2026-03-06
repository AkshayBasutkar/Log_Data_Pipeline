from pathlib import Path

from streamer import stream_multiple_logs
from format_detector import detect_format

from parsers.hdfs_parser import parse_hdfs
from parsers.hadoop_parser import parse_hadoop
from parsers.spark_parser import parse_spark
from parsers.windows_parser import parse_windows

from enrich import enrich_log
from db import check_connection, insert_log


BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "dataset"

LOG_FILES = [
    DATASET_DIR / "Hadoop_2k.log",
    DATASET_DIR / "HDFS_2k.log",
    DATASET_DIR / "Windows_2k.log",
    DATASET_DIR / "Spark_2k.log",
]


PARSERS = {
    "hdfs": parse_hdfs,
    "hadoop": parse_hadoop,
    "windows": parse_windows,
    "spark": parse_spark,
}


def run_pipeline(log_files=None, delay: float = 0.05):
    """
    Stream, parse, enrich, and persist dataset logs.
    """

    files_to_stream = [str(path) for path in (log_files or LOG_FILES)]
    storage_enabled = check_connection()

    print("Starting log pipeline...\n")

    if not storage_enabled:
        print("MongoDB unavailable or pymongo missing. Logs will be parsed without persistence.\n")

    for source_file, log_line in stream_multiple_logs(files_to_stream, delay=delay):
        try:
            log_type = detect_format(log_line)

            if log_type == "unknown":
                continue

            parser = PARSERS.get(log_type)
            if parser is None:
                continue

            parsed_log = parser(log_line)
            if not parsed_log:
                continue

            parsed_log["source_file"] = source_file

            enriched_log = enrich_log(parsed_log)
            if storage_enabled:
                insert_log(enriched_log)

            print(
                f"[{'INGESTED' if storage_enabled else 'PARSED'}] {log_type.upper()} | "
                f"{enriched_log.get('level')} | "
                f"{enriched_log.get('component')}"
            )
        except Exception as exc:
            print(f"Pipeline error for {source_file}: {exc}")


if __name__ == "__main__":
    run_pipeline()
