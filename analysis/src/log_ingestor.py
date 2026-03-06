from pathlib import Path

from log_parser import parse_apache, parse_app_json, parse_syslog
from mongo_client import get_db


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

def _ingest_file(file_path: Path, parser):
    inserted = 0
    collection = get_db()["logs_parsed"]

    with file_path.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()

            if not line:
                continue

            parsed = parser(line)

            if not parsed:
                continue

            collection.insert_one(parsed)
            inserted += 1

    return inserted


def ingest_apache(file_path=None):
    return _ingest_file(Path(file_path) if file_path else DATA_DIR / "apache.log", parse_apache)


def ingest_json(file_path=None):
    return _ingest_file(Path(file_path) if file_path else DATA_DIR / "app.json", parse_app_json)


def ingest_syslog(file_path=None):
    return _ingest_file(Path(file_path) if file_path else DATA_DIR / "syslog.log", parse_syslog)


def ingest_all():
    return {
        "apache": ingest_apache(),
        "app_json": ingest_json(),
        "syslog": ingest_syslog(),
    }


if __name__ == "__main__":
    counts = ingest_all()
    print(f"Logs successfully inserted into MongoDB: {counts}")
