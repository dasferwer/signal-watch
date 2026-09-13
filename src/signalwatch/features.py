"""Признаки используют только уже обработанную часть потока до времени события."""

FEATURE_NAMES = [
    "actor_requests_60s",
    "actor_failures_300s",
    "actor_failure_ratio_300s",
    "actor_countries_300s",
    "source_signups_300s",
    "source_actors_300s",
    "actor_bytes_60s",
]
WINDOW_SECONDS = 300
ALLOWED_LATENESS_SECONDS = 30
FEATURE_VERSION = "event-time-v1"


def extract_features(history: list[dict], event: dict) -> dict[str, float]:
    moment = float(event["occurred_at"])
    # Повторная доставка текущего события не должна увеличивать счётчики.
    unique = {
        row["event_id"]: row
        for row in history
        if moment - WINDOW_SECONDS < float(row["occurred_at"]) <= moment
        and row["event_id"] != event["event_id"]
    }
    unique[event["event_id"]] = event
    actor = [row for row in unique.values() if row["actor_id"] == event["actor_id"]]
    recent = [row for row in actor if float(row["occurred_at"]) > moment - 60]
    source = [row for row in unique.values() if row["source_ip"] == event["source_ip"]]
    failures = sum(not row["success"] for row in actor)
    return {
        "actor_requests_60s": float(len(recent)),
        "actor_failures_300s": float(failures),
        "actor_failure_ratio_300s": failures / len(actor),
        "actor_countries_300s": float(len({row["country"] for row in actor})),
        "source_signups_300s": float(sum(row["kind"] == "signup" for row in source)),
        "source_actors_300s": float(len({row["actor_id"] for row in source})),
        "actor_bytes_60s": float(sum(row["bytes_sent"] for row in recent)),
    }
