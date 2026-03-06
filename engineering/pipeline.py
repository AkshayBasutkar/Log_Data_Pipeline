from streamer import stream_multiple_logs
from format_detector import detect_format

from parsers.hdfs_parser import parse_hdfs
from parsers.windows_parser import parse_windows
from parsers.spark_parser import parse_spark

from enrich import enrich_log
from db import insert_log


# --------------------------------------------
# Log File Paths
# --------------------------------------------

LOG_FILES = [
    "logs/HDFS.log",
    "logs/Windows.log",
    "logs/Spark.log"
]


# --------------------------------------------
# Parser Mapping
# --------------------------------------------

PARSERS = {
    "hdfs": parse_hdfs,
    "windows": parse_windows,
    "spark": parse_spark
}


# --------------------------------------------
# Pipeline Runner
# --------------------------------------------

def run_pipeline():

    print("Starting log pipeline...\n")

    for source_file, log_line in stream_multiple_logs(LOG_FILES, delay=0.05):

        try:

            # Step 1: Detect format
            log_type = detect_format(log_line)

            if log_type == "unknown":
                continue

            # Step 2: Parse log
            parser = PARSERS.get(log_type)

            parsed_log = parser(log_line)

            if not parsed_log:
                continue

            # Step 3: Enrich log
            enriched_log = enrich_log(parsed_log)

            # Step 4: Store in MongoDB
            insert_log(enriched_log)

            print(
                f"[INGESTED] {log_type.upper()} | "
                f"{enriched_log.get('level')} | "
                f"{enriched_log.get('component')}"
            )

        except Exception as e:
            print("Pipeline error:", e)




if __name__ == "__main__":

    run_pipeline()