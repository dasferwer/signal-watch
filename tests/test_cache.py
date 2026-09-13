import hashlib
import json

import pytest

from signalwatch.catalog import ingest
from signalwatch.config import LIVE_RUN
from signalwatch.db import canonical, engine, one
from signalwatch.processing import process_run

pytestmark = pytest.mark.usefixtures("clean_state")


async def test_cache_round_trip_requires_exact_cursor_and_model(cache, event, detector):
    history = [event().model_dump(mode="json")]
    assert await cache.save(LIVE_RUN, 4, detector.identity, history)
    assert await cache.load(LIVE_RUN, 4, detector.identity) == history
    assert await cache.load(LIVE_RUN, 3, detector.identity) is None
    assert await cache.load(LIVE_RUN, 4, "different-model") is None
    assert 0 < await cache.redis.ttl(cache.key(LIVE_RUN)) <= 3600


async def test_late_cache_save_cannot_overwrite_a_newer_cursor(cache, event, detector):
    fresh = [event(occurred_at=1100).model_dump(mode="json")]
    stale = [event(occurred_at=1000).model_dump(mode="json")]
    assert await cache.save(LIVE_RUN, 5, detector.identity, fresh)
    assert await cache.save(LIVE_RUN, 4, detector.identity, stale) is False
    assert await cache.load(LIVE_RUN, 5, detector.identity) == fresh


async def test_cache_history_checksum_detects_changed_content(cache, event, detector):
    await cache.save(LIVE_RUN, 1, detector.identity, [event().model_dump(mode="json")])
    payload = json.loads(await cache.redis.get(cache.key(LIVE_RUN)))
    payload["history"][0]["success"] = False
    await cache.redis.set(cache.key(LIVE_RUN), canonical(payload))
    assert await cache.load(LIVE_RUN, 1, detector.identity) is None


@pytest.mark.parametrize(
    "payload", ["not-json", "[]", "null", '{"history": []}', '{"cursor": "bad"}']
)
async def test_invalid_cache_envelopes_are_misses(cache, detector, payload):
    await cache.redis.set(cache.key(LIVE_RUN), payload)
    assert await cache.load(LIVE_RUN, 1, detector.identity) is None


@pytest.mark.parametrize(
    "damage", [{"occurred_at": "not-a-time"}, {"success": "false"}, {"bytes_sent": -1}]
)
async def test_invalid_history_with_valid_checksum_falls_back_to_postgres(
    cache, detector, event, damage
):
    await ingest([event(success=False)])
    await process_run(LIVE_RUN, detector, cache)
    payload = json.loads(await cache.redis.get(cache.key(LIVE_RUN)))
    payload["history"][0].update(damage)
    payload["sha256"] = hashlib.sha256(canonical(payload["history"]).encode()).hexdigest()
    await cache.redis.set(cache.key(LIVE_RUN), canonical(payload))
    assert await cache.load(LIVE_RUN, 1, detector.identity) is None
    await ingest([event(occurred_at=1001)])
    await process_run(LIVE_RUN, detector, cache)
    async with engine.connect() as conn:
        row = await one(
            conn,
            "SELECT features FROM decisions WHERE run_id=:run AND event_sequence=2",
            run=LIVE_RUN,
        )
    assert row["features"]["actor_requests_60s"] == 2
    assert row["features"]["actor_failures_300s"] == 1


async def test_valid_save_replaces_a_corrupt_cache_envelope(cache, event, detector):
    await cache.redis.set(cache.key(LIVE_RUN), "broken")
    history = [event().model_dump(mode="json")]
    assert await cache.save(LIVE_RUN, 2, detector.identity, history)
    assert await cache.load(LIVE_RUN, 2, detector.identity) == history
