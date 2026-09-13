import json
import os
import subprocess
from pathlib import Path

from demo_client import Client, event, wait_for

from signalwatch.config import LIVE_RUN

ROOT = Path(__file__).resolve().parents[1]


def compose(*args, worker_delay="0", dispatcher_delay="0"):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env={
            **os.environ,
            "WORKER_BEFORE_COMMIT_DELAY": worker_delay,
            "DISPATCHER_AFTER_PUBLISH_DELAY": dispatcher_delay,
        },
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    ).stdout


def sql(query):
    return compose(
        "exec", "-T", "database", "psql", "-U", "signal", "-d", "signal", "-At", "-c", query
    ).strip()


def main():
    client = Client(os.getenv("BASE_URL", "http://localhost:8230"))
    report = {}
    try:
        wait_for(client.caught_up)
        compose("stop", "--timeout", "15", "rabbitmq")
        client.ingest([event()])
        wait_for(client.caught_up, description="PostgreSQL fallback with RabbitMQ stopped")
        assert client.status()["outbox"]["pending"] > 0
        compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
        wait_for(
            lambda: client.status()["outbox"]["pending"] == 0, description="publisher recovery"
        )
        report["rabbitmq_outage"] = (
            "passed; event processed through the database sweep and notification later confirmed"
        )
        print("RabbitMQ outage: passed", flush=True)

        compose("stop", "--timeout", "15", "redis")
        actor = "cache-recovery"
        client.ingest([event(actor_id=actor, success=False)])
        wait_for(client.caught_up, description="processing without Redis")
        client.ingest([event(actor_id=actor)])
        wait_for(client.caught_up)
        compose("up", "-d", "--wait", "--wait-timeout", "120", "redis")
        # Сбрасываем только кеш этого стенда; события и решения остаются в PostgreSQL.
        compose("exec", "-T", "redis", "redis-cli", "FLUSHDB")
        client.ingest([event(actor_id=actor)])
        wait_for(client.caught_up)
        report["redis_loss"] = client.replay_matches()
        print("Redis loss and replay comparison: passed", flush=True)

        compose("up", "-d", "--no-deps", "--force-recreate", "worker", worker_delay="30")
        receipt = client.ingest([event()])[0]
        cursor = client.run()["cursor"]
        wait_for(
            lambda: (
                f"commit delayed; run={LIVE_RUN} sequence={receipt['sequence']}"
                in compose("logs", "--no-color", "--tail=100", "worker")
            ),
            description="uncommitted decision before cursor update",
        )
        assert client.run()["cursor"] == cursor < receipt["sequence"]
        assert (
            int(
                sql(
                    f"SELECT count(*) FROM decisions WHERE run_id='{LIVE_RUN}' AND event_sequence={receipt['sequence']};"
                )
            )
            == 0
        )
        compose("kill", "-s", "SIGKILL", "worker")
        compose("up", "-d", "--no-deps", "--force-recreate", "worker")
        wait_for(client.caught_up, description="worker rollback and retry")
        assert (
            int(
                sql(
                    f"SELECT count(*) FROM decisions WHERE run_id='{LIVE_RUN}' AND event_sequence={receipt['sequence']};"
                )
            )
            == 1
        )
        report["worker_crash"] = {
            "cursor_before_kill": cursor,
            "event_recovered": receipt["sequence"],
            "result": "passed",
        }
        print("Worker crash before commit: passed", flush=True)

        wait_for(lambda: client.status()["outbox"]["pending"] == 0)
        compose("up", "-d", "--no-deps", "--force-recreate", "dispatcher", dispatcher_delay="30")
        client.ingest([event()])
        pending_id = sql("SELECT max(id) FROM outbox WHERE published_at IS NULL;")
        wait_for(
            lambda: (
                f"broker confirmed notification, commit delayed; outbox={pending_id}"
                in compose("logs", "--no-color", "--tail=100", "dispatcher")
            ),
            description="notification confirmed before outbox commit",
        )
        assert client.status()["outbox"]["pending"] > 0
        compose("kill", "-s", "SIGKILL", "dispatcher")
        compose("up", "-d", "--no-deps", "--force-recreate", "dispatcher")
        wait_for(
            lambda: client.status()["outbox"]["pending"] == 0,
            description="outbox republished after crash",
        )
        wait_for(client.caught_up)
        report["dispatcher_crash"] = client.replay_matches()
        print("Dispatcher crash and repeated notification: passed", flush=True)
    finally:
        compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq", "redis")
        compose("up", "-d", "--no-deps", "--force-recreate", "worker", "dispatcher")
        wait_for(client.caught_up)
        client.http.close()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
