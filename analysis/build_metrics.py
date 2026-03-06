from analytics import refresh_metrics_snapshot


def main():
    snapshot = refresh_metrics_snapshot()
    overview = snapshot.get("overview", {})
    print("Metrics snapshot generated.")
    print(f"Total logs: {overview.get('total_logs', 0)}")
    print(f"Errors: {overview.get('error_count', 0)}")
    print(f"Generated at: {snapshot.get('generated_at')}")


if __name__ == "__main__":
    main()
