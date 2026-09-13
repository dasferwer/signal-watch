import asyncio

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import DBAPIError

from signalwatch.catalog import ingest, review, start_replay
from signalwatch.config import LIVE_RUN
from signalwatch.db import engine, execute, one
from signalwatch.processing import process_run
from signalwatch.schemas import Review

pytestmark = pytest.mark.usefixtures("clean_state")


async def test_concurrent_ingest_of_one_uuid_creates_one_event_and_one_outbox(event):
    incoming = event()
    replies = await asyncio.gather(*[ingest([incoming]) for _ in range(20)])
    receipts = [reply["events"][0] for reply in replies]
    assert {receipt["sequence"] for receipt in receipts} == {1}
    assert sum(not receipt["duplicate"] for receipt in receipts) == 1
    async with engine.connect() as conn:
        counts = await one(
            conn,
            "SELECT (SELECT count(*) FROM events) AS events,(SELECT count(*) FROM outbox) AS outbox,(SELECT sequence FROM catalog_state WHERE id=1) AS sequence",
        )
    assert dict(counts) == {"events": 1, "outbox": 1, "sequence": 1}


async def test_conflict_rolls_back_every_new_event_in_a_batch(event):
    existing = event()
    await ingest([existing])
    fresh = event(occurred_at=1001)
    conflict = existing.model_copy(update={"success": False})
    with pytest.raises(HTTPException) as error:
        await ingest([fresh, conflict])
    assert error.value.status_code == 409
    async with engine.connect() as conn:
        assert (await one(conn, "SELECT count(*) AS n FROM events"))["n"] == 1
        assert (await one(conn, "SELECT sequence FROM catalog_state WHERE id=1"))["sequence"] == 1
        assert (await one(conn, "SELECT count(*) AS n FROM outbox"))["n"] == 1
    assert (await ingest([fresh]))["events"][0]["sequence"] == 2


async def test_duplicate_inside_batch_does_not_create_a_gap(event):
    first, second = event(), event()
    receipts = (await ingest([first, first, second]))["events"]
    assert [item["sequence"] for item in receipts] == [1, 1, 2]
    assert [item["duplicate"] for item in receipts] == [False, True, False]


async def test_concurrent_batches_receive_contiguous_committed_ranges(event):
    batches = [[event() for _ in range(3)] for _ in range(10)]
    replies = await asyncio.gather(*[ingest(batch) for batch in batches])
    ranges = [[receipt["sequence"] for receipt in reply["events"]] for reply in replies]
    assert all(values == list(range(values[0], values[0] + 3)) for values in ranges)
    assert sorted(value for values in ranges for value in values) == list(range(1, 31))
    async with engine.connect() as conn:
        assert list(
            (await execute(conn, "SELECT sequence FROM events ORDER BY sequence")).scalars()
        ) == list(range(1, 31))


async def test_event_is_not_accepted_without_a_durable_outbox(event):
    async with engine.begin() as conn:
        await execute(
            conn, "ALTER TABLE outbox ADD CONSTRAINT fail_test_publish CHECK(false) NOT VALID"
        )
    try:
        with pytest.raises(DBAPIError):
            await ingest([event()])
        async with engine.connect() as conn:
            assert (await one(conn, "SELECT count(*) AS n FROM events"))["n"] == 0
            assert (await one(conn, "SELECT sequence FROM catalog_state WHERE id=1"))[
                "sequence"
            ] == 0
    finally:
        async with engine.begin() as conn:
            await execute(conn, "ALTER TABLE outbox DROP CONSTRAINT fail_test_publish")


async def test_replay_freezes_the_current_upper_bound(event, detector):
    await ingest([event()])
    replay = await start_replay(detector.identity)
    await ingest([event()])
    assert replay["upper_bound"] == 1
    assert replay["model_version"] == detector.identity
    async with engine.connect() as conn:
        assert (
            await one(conn, "SELECT count(*) AS n FROM outbox WHERE run_id=:run", run=replay["id"])
        )["n"] == 1


@pytest.mark.parametrize("table", ["events", "decisions", "reviews"])
@pytest.mark.parametrize("action", ["update", "delete"])
async def test_journals_reject_mutation(table, action, event, detector, cache):
    await ingest([event()])
    await process_run(LIVE_RUN, detector, cache)
    await review(LIVE_RUN, 1, Review(verdict="needs_context", note="Нужна проверка события."), 0)
    statement = (
        {
            "events": "UPDATE events SET occurred_at=0",
            "decisions": "UPDATE decisions SET alert=false",
            "reviews": "UPDATE reviews SET version=99",
        }[table]
        if action == "update"
        else f"DELETE FROM {table}"
    )
    with pytest.raises(DBAPIError, match="append-only"):
        async with engine.begin() as conn:
            await execute(conn, statement)
    async with engine.connect() as conn:
        assert (await one(conn, f"SELECT count(*) AS n FROM {table}"))["n"] == 1


async def test_review_retry_is_idempotent_and_changed_body_conflicts(event, detector, cache):
    await ingest([event()])
    await process_run(LIVE_RUN, detector, cache)
    note = Review(verdict="false_positive", note="Ожидаемая активность пользователя.")
    original = await review(LIVE_RUN, 1, note, 0)
    assert await review(LIVE_RUN, 1, note, 0) == original
    with pytest.raises(HTTPException) as error:
        await review(
            LIVE_RUN, 1, Review(verdict="confirmed", note="Проверка подтвердила подозрение."), 0
        )
    assert error.value.status_code == 409
    async with engine.connect() as conn:
        assert (await one(conn, "SELECT count(*) AS n FROM reviews"))["n"] == 1


async def test_concurrent_review_edits_cannot_silently_replace_each_other(event, detector, cache):
    await ingest([event()])
    await process_run(LIVE_RUN, detector, cache)
    results = await asyncio.gather(
        *[
            review(
                LIVE_RUN,
                1,
                Review(verdict="needs_context", note=f"Комментарий проверяющего {index}."),
                0,
            )
            for index in range(10)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert (
        sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results)
        == 9
    )


async def test_review_of_missing_decision_returns_not_found():
    with pytest.raises(HTTPException) as error:
        await review(LIVE_RUN, 99, Review(verdict="needs_context", note="Нужна проверка."), 0)
    assert error.value.status_code == 404
