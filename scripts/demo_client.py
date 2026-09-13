import time
from uuid import uuid4

import httpx

from signalwatch.config import LIVE_RUN, settings


def wait_for(check, *, timeout=180, description="condition"):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if value := check():
                return value
        except (httpx.HTTPError, KeyError) as error:
            last_error = str(error)
        time.sleep(0.15)
    raise TimeoutError(f"Timed out waiting for {description}: {last_error}")


class Client:
    def __init__(self, url):
        self.http = httpx.Client(
            base_url=url,
            timeout=30,
            headers={
                "X-Admin-Token": settings.admin_token,
                "X-Ingest-Token": settings.ingest_token,
            },
        )

    def request(self, method, path, **kwargs):
        response = self.http.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json()

    def ingest(self, events):
        return self.request("POST", "/events", json={"events": events})["events"]

    def status(self):
        return self.request("GET", "/admin/status")

    def run(self, run_id=LIVE_RUN):
        return self.request("GET", f"/admin/runs/{run_id}")

    def caught_up(self):
        status = self.status()
        live = next(row for row in status["runs"] if str(row["id"]) == LIVE_RUN)
        return live if live["cursor"] == status["sequence"] and not live["error"] else None

    def replay_matches(self):
        wait_for(self.caught_up)
        replay = self.request("POST", "/admin/replays")
        wait_for(
            lambda: self.run(replay["id"])["status"] == "completed",
            description="replay",
            timeout=300,
        )
        original = self.request(
            "GET", f"/admin/runs/{LIVE_RUN}/digest", params={"through": replay["upper_bound"]}
        )
        repeated = self.request("GET", f"/admin/runs/{replay['id']}/digest")
        assert original == repeated, {"live": original, "replay": repeated}
        return {"run_id": replay["id"], **repeated}

    def all_decisions(self):
        result, cursor = [], 0
        while True:
            page = self.request(
                "GET", f"/admin/runs/{LIVE_RUN}/decisions", params={"after": cursor, "limit": 200}
            )
            if not page["items"]:
                return result
            result.extend(page["items"])
            cursor = page["next_after"]


def event(**changes):
    return {
        "event_id": str(uuid4()),
        "occurred_at": time.time() - 1,
        "actor_id": "recovery-" + uuid4().hex,
        "source_ip": "192.0.2.200",
        "country": "GB",
        "kind": "api",
        "success": True,
        "bytes_sent": 100,
        **changes,
    }
