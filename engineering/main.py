from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parent.parent
ANALYSIS_DIR = ROOT_DIR / "analysis"

if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from pipeline import run_pipeline
from analytics import refresh_metrics_snapshot


def run_all():
    """
    Ingest logs into MongoDB and refresh the dashboard metrics snapshot.
    """

    run_pipeline(delay=0)

    try:
        snapshot = refresh_metrics_snapshot()
        overview = snapshot.get("overview", {})
        print(
            "Metrics snapshot refreshed: "
            f"{overview.get('total_logs', 0)} logs, "
            f"{overview.get('error_count', 0)} errors"
        )
    except Exception as exc:
        print(f"Metrics snapshot refresh skipped: {exc}")


if __name__ == "__main__":
    run_all()
