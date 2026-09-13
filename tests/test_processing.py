import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from signalwatch import processing
from signalwatch.catalog import ingest, start_replay
from signalwatch.config import LIVE_RUN, settings
from signalwatch.db import engine, execute, one
from signalwatch.processing import process_run

pytestmark = pytest.mark.usefixtures("clean_state")


async def decisions(run_id):
    async with engine.connect() as conn:
        rows = await execute(
            conn,
            "SELECT event_sequence,status,features,result,alert FROM decisions WHERE run_id=:run ORDER BY event_sequence",
            run=run_id,
        )
        return [dict(row) for row in rows.mappings()]


async def run_state(run_id=LIVE_RUN):
    async with engine.connect() as conn:
        return await one(conn, "SELECT * FROM runs WHERE id=:run", run=run_id)


async def drain(run_id, detector, cache):
    for _ in range(50):
        if not await process_run(run_id, detector, cache):
            return
    pytest.fail("Run did not drain within 50 batches")


async def test_replay_matches_live_features_and_decisions_with_late_events(
    event, detector, cache, monkeypatch
):
    incoming = [
        event(occurred_at=1000, success=False),
        event(occurred_at=1005),
        event(occurred_at=980, country="FR"),
        event(occurred_at=974),
        event(occurred_at=1050, kind="signup"),
        event(occurred_at=1020, success=False),
        event(occurred_at=700),
    ]
    monkeypatch.setattr(settings, "batch_size", 2)
    for item in incoming:
        await ingest([item])
        await process_run(LIVE_RUN, detector, cache)
    live = await decisions(LIVE_RUN)
    assert [row["status"] for row in live] == [
        "scored",
        "scored",
        "scored",
        "late",
        "scored",
        "scored",
        "late",
    ]
    assert live[2]["features"]["actor_requests_60s"] == 1
    assert live[5]["features"]["actor_requests_60s"] == 4
    assert live[3]["features"] == {}
    assert live[3]["alert"] is False
    replay = await start_replay(detector.identity)
    await cache.redis.flushdb()
    await drain(replay["id"], detector, cache)
    assert await decisions(replay["id"]) == live
    assert (await run_state(replay["id"]))["status"] == "completed"


async def test_completed_replay_does_not_follow_later_events(event, detector, cache):
    await ingest([event()])
    replay = await start_replay(detector.identity)
    await ingest([event(occurred_at=1001)])
    assert await process_run(replay["id"], detector, cache) == 1
    assert await process_run(replay["id"], detector, cache) == 0
    assert (await run_state(replay["id"]))["cursor"] == 1
    assert len(await decisions(replay["id"])) == 1
    assert await process_run(LIVE_RUN, detector, cache) == 2


async def test_empty_replay_completes_without_decisions(detector, cache):
    replay = await start_replay(detector.identity)
    assert await process_run(replay["id"], detector, cache) == 0
    assert (await run_state(replay["id"]))["status"] == "completed"
    assert await decisions(replay["id"]) == []


@pytest.mark.parametrize("damage", ["flush", "stale", "corrupt", "outage"])
async def test_cache_loss_does_not_change_features_or_decisions(
    event, detector, cache, damage, monkeypatch
):
    await ingest([event(occurred_at=1000, success=False), event(occurred_at=1005)])
    await process_run(LIVE_RUN, detector, cache)
    if damage == "flush":
        await cache.redis.flushdb()
    elif damage == "stale":
        await cache.redis.delete(cache.key(LIVE_RUN))
        await cache.save(LIVE_RUN, 1, detector.identity, [])
    elif damage == "corrupt":
        await cache.redis.set(cache.key(LIVE_RUN), "{broken json")
    else:

        async def unavailable(*args, **kwargs):
            raise RedisConnectionError("Redis is temporarily unavailable")

        monkeypatch.setattr(cache.redis, "get", unavailable)
        monkeypatch.setattr(cache.redis, "eval", unavailable)
    await ingest([event(occurred_at=1010, success=False)])
    assert await process_run(LIVE_RUN, detector, cache) == 1
    live = await decisions(LIVE_RUN)
    assert live[-1]["features"]["actor_requests_60s"] == 3
    assert live[-1]["features"]["actor_failures_300s"] == 2
    replay = await start_replay(detector.identity)
    await drain(replay["id"], detector, cache)
    assert await decisions(replay["id"]) == live


async def test_cache_reconstruction_preserves_allowed_out_of_order_window(event, detector, cache):
    await ingest([event(occurred_at=701, success=False), event(occurred_at=1000)])
    await process_run(LIVE_RUN, detector, cache)
    await cache.redis.flushdb()
    await ingest([event(occurred_at=980)])
    await process_run(LIVE_RUN, detector, cache)
    latest = (await decisions(LIVE_RUN))[-1]
    assert latest["features"]["actor_failures_300s"] == 1
    assert latest["features"]["actor_requests_60s"] == 1


async def test_competing_worker_skips_the_locked_run(event, detector, cache, monkeypatch):
    await ingest([event()])
    entered, release = asyncio.Event(), asyncio.Event()
    original_load = cache.load

    async def pause(*args):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=10)
        return await original_load(*args)

    monkeypatch.setattr(cache, "load", pause)
    first = asyncio.create_task(process_run(LIVE_RUN, detector, cache))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert await asyncio.wait_for(process_run(LIVE_RUN, detector, cache), timeout=2) == 0
    finally:
        release.set()
        assert await first == 1
    assert len(await decisions(LIVE_RUN)) == 1


async def test_failure_before_commit_rolls_back_decisions_and_cursor(
    event, detector, cache, monkeypatch
):
    await ingest([event(), event(occurred_at=1001)])
    original_execute = processing.execute

    async def fail_before_cursor_update(conn, sql, **params):
        if sql.startswith("UPDATE runs SET cursor="):
            # Решения уже вставлены в этой транзакции, но ни одно ещё не должно быть видно другим запросам.
            assert await decisions(LIVE_RUN) == []
            raise RuntimeError("simulated worker crash before commit")
        return await original_execute(conn, sql, **params)

    monkeypatch.setattr(processing, "execute", fail_before_cursor_update)
    with pytest.raises(RuntimeError, match="simulated worker crash"):
        await process_run(LIVE_RUN, detector, cache)
    assert await decisions(LIVE_RUN) == []
    assert (await run_state())["cursor"] == 0
    assert await cache.redis.get(cache.key(LIVE_RUN)) is None
    monkeypatch.setattr(processing, "execute", original_execute)
    assert await process_run(LIVE_RUN, detector, cache) == 2
    assert len(await decisions(LIVE_RUN)) == 2


async def test_committed_decisions_survive_failure_before_cache_save(
    event, detector, cache, monkeypatch
):
    await ingest([event(), event(occurred_at=1001)])
    original_save = cache.save

    async def fail(*args):
        raise RuntimeError("simulated process exit after commit")

    monkeypatch.setattr(cache, "save", fail)
    with pytest.raises(RuntimeError, match="after commit"):
        await process_run(LIVE_RUN, detector, cache)
    committed = await decisions(LIVE_RUN)
    assert len(committed) == 2
    assert (await run_state())["cursor"] == 2
    monkeypatch.setattr(cache, "save", original_save)
    assert await process_run(LIVE_RUN, detector, cache) == 0
    assert await decisions(LIVE_RUN) == committed
    await ingest([event(occurred_at=1002)])
    await process_run(LIVE_RUN, detector, cache)
    assert (await decisions(LIVE_RUN))[-1]["features"]["actor_requests_60s"] == 3


async def test_incompatible_detector_does_not_advance_a_run(event, detector, cache):
    await ingest([event()])
    await process_run(LIVE_RUN, detector, cache)
    original = await decisions(LIVE_RUN)
    await ingest([event(occurred_at=1001)])
    detector.identity = "different-model"
    with pytest.raises(RuntimeError, match="different detector version"):
        await process_run(LIVE_RUN, detector, cache)
    assert (await run_state())["cursor"] == 1
    assert await decisions(LIVE_RUN) == original
