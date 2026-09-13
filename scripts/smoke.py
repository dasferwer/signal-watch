import json
import os
import statistics
import time
from pathlib import Path

from demo_client import Client, wait_for

from signalwatch.config import LIVE_RUN

ROOT = Path(__file__).resolve().parents[1]


def main():
    client = Client(os.getenv("BASE_URL", "http://localhost:8000"))
    try:
        wait_for(
            lambda: client.http.get("/ready").status_code == 200, description="worker readiness"
        )
        assert client.http.get("/openapi.json").status_code == 200
        assert (
            client.http.get("/admin/status", headers={"X-Admin-Token": "wrong"}).status_code == 403
        )
        events = [
            json.loads(line) for line in (ROOT / "data/demo_events.jsonl").read_text().splitlines()
        ]
        manifest = json.loads((ROOT / "data/demo_manifest.json").read_text())
        receipts, latencies = [], []
        for start in range(0, len(events), 100):
            started = time.perf_counter()
            receipts.extend(client.ingest(events[start : start + 100]))
            wait_for(client.caught_up, description="live decisions")
            latencies.append((time.perf_counter() - started) * 1000)
        sequence = client.status()["sequence"]
        for start in range(0, len(events), 100):
            assert all(row["duplicate"] for row in client.ingest(events[start : start + 100]))
        assert client.status()["sequence"] == sequence
        decisions = {str(row["event_id"]): row for row in client.all_decisions()}
        assert decisions[manifest["accepted_late_event_id"]]["status"] == "scored"
        assert decisions[manifest["rejected_late_event_id"]]["status"] == "late"
        alerts = [row for row in decisions.values() if row["alert"]]
        selected = json.loads((ROOT / "artifacts/manifest.json").read_text())["selected_policy"]
        assert alerts and all(row["result"]["policy"] == selected for row in alerts)
        sample = alerts[0]
        review_path = f"/admin/runs/{LIVE_RUN}/decisions/{sample['event_sequence']}/review"
        version = sample["review_version"] or 0
        review = {
            "verdict": "needs_context",
            "note": "Демонстрационное событие: нужно проверить контекст.",
        }
        saved = client.request("PUT", review_path, json=review, headers={"If-Match": str(version)})
        assert (
            client.request("PUT", review_path, json=review, headers={"If-Match": str(version)})
            == saved
        )
        replay = client.replay_matches()
        drift = client.request("GET", "/admin/drift")
        assert drift["sample_count"] >= 100 and drift["status"] in {"stable", "shift"}
        metrics = client.http.get("/metrics")
        assert "signalwatch_unprocessed_events" in metrics.text
        print(
            json.dumps(
                {
                    "result": "passed",
                    "demo_submitted": len(receipts),
                    "demo_unique": manifest["unique_event_count"],
                    "live_decisions": len(decisions),
                    "live_alerts": len(alerts),
                    "replay": replay,
                    "drift": drift["status"],
                    "batch_completion_ms": {
                        "median": statistics.median(latencies),
                        "max": max(latencies),
                        "note": "Includes ingestion, queue/polling, processing and client polling; batches contain up to 100 events.",
                    },
                },
                indent=2,
            )
        )
    finally:
        client.http.close()


if __name__ == "__main__":
    main()
