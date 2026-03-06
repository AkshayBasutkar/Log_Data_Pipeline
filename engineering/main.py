from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parent.parent
ANALYSIS_SRC_DIR = ROOT_DIR / "analysis" / "src"

if str(ANALYSIS_SRC_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_SRC_DIR))

from pipeline import run_pipeline
from log_ingestor import ingest_all


def run_all():
    """
    Run both ingestion flows against the shared MongoDB database.
    """

    run_pipeline()

    try:
        counts = ingest_all()
        print(f"Batch ingestion complete: {counts}")
    except Exception as exc:
        print(f"Batch ingestion skipped: {exc}")


if __name__ == "__main__":
    run_all()
