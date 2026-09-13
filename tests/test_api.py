import time
from uuid import uuid4

import pytest

from signalwatch.catalog import ingest
from signalwatch.config import LIVE_RUN
from signalwatch.db import engine, execute
from signalwatch.processing import process_run

pytestmark = pytest.mark.usefixtures("clean_state")


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/admin/status"),
        ("POST", "/admin/replays"),
        ("GET", f"/admin/runs/{LIVE_RUN}"),
        ("GET", f"/admin/runs/{LIVE_RUN}/decisions"),
        ("GET", f"/admin/runs/{LIVE_RUN}/digest"),
        ("GET", "/admin/drift"),
        ("PUT", f"/admin/runs/{LIVE_RUN}/decisions/1/review"),
    ],
)
async def test_administrative_routes_require_admin_token(client, ingest_headers, method, path):
    for headers in [{}, {"X-Admin-Token": "wrong"}, ingest_headers]:
        response = await client.request(
            method,
            path,
            headers={**headers, "If-Match": "0"},
            json={"verdict": "needs_context", "note": "Нужна проверка."}
            if method == "PUT"
            else None,
        )
        assert response.status_code == 403


async def test_ingestion_uses_its_own_token(client, event, admin_headers, ingest_headers):
    body = {"events": [event().model_dump(mode="json")]}
    for headers in [{}, {"X-Ingest-Token": "wrong"}, admin_headers]:
        assert (await client.post("/events", headers=headers, json=body)).status_code == 403
    first = await client.post("/events", headers=ingest_headers, json=body)
    retry = await client.post("/events", headers=ingest_headers, json=body)
    assert first.status_code == retry.status_code == 202
    assert first.json()["events"][0]["sequence"] == retry.json()["events"][0]["sequence"]
    assert first.json()["events"][0]["duplicate"] is False
    assert retry.json()["events"][0]["duplicate"] is True


async def test_conflicting_uuid_returns_409_and_batch_is_not_partly_accepted(
    client, event, ingest_headers, admin_headers
):
    original = event().model_dump(mode="json")
    assert (
        await client.post("/events", headers=ingest_headers, json={"events": [original]})
    ).status_code == 202
    response = await client.post(
        "/events",
        headers=ingest_headers,
        json={"events": [event().model_dump(mode="json"), {**original, "success": False}]},
    )
    assert response.status_code == 409
    status = (await client.get("/admin/status", headers=admin_headers)).json()
    assert status["sequence"] == 1
    assert status["outbox"]["pending"] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"source_ip": "not-an-ip"},
        {"actor_id": "bad actor"},
        {"country": "de"},
        {"kind": "purchase"},
        {"success": "true"},
        {"bytes_sent": True},
        {"occurred_at": 4102444800},
        {"model_alert": False},
    ],
)
async def test_ingest_rejects_invalid_event_fields(client, event, ingest_headers, changes):
    body = {**event().model_dump(mode="json"), **changes}
    response = await client.post("/events", headers=ingest_headers, json={"events": [body]})
    assert response.status_code == 422


@pytest.mark.parametrize("size", [0, 201])
async def test_ingest_batch_size_is_bounded(client, event, ingest_headers, size):
    response = await client.post(
        "/events",
        headers=ingest_headers,
        json={"events": [event().model_dump(mode="json") for _ in range(size)]},
    )
    assert response.status_code == 422


async def test_future_event_cannot_move_the_watermark(
    client, event, ingest_headers, detector, cache, admin_headers
):
    current = event(occurred_at=time.time()).model_dump(mode="json")
    future = {**current, "event_id": str(uuid4()), "occurred_at": time.time() + 3600}
    assert (
        await client.post("/events", headers=ingest_headers, json={"events": [future]})
    ).status_code == 422
    assert (
        await client.post("/events", headers=ingest_headers, json={"events": [current]})
    ).status_code == 202
    await process_run(LIVE_RUN, detector, cache)
    row = (await client.get(f"/admin/runs/{LIVE_RUN}", headers=admin_headers)).json()
    assert row["watermark"] == current["occurred_at"]
    assert row["stats"]["late"] == 0


async def test_decision_pagination_has_no_gaps_and_alert_filter_is_explicit(
    client, event, detector, cache, admin_headers
):
    await ingest([event(occurred_at=1000 + index) for index in range(5)])
    await process_run(LIVE_RUN, detector, cache)
    path = f"/admin/runs/{LIVE_RUN}/decisions"
    first = (await client.get(path, headers=admin_headers, params={"limit": 2})).json()
    second = (
        await client.get(
            path, headers=admin_headers, params={"limit": 2, "after": first["next_after"]}
        )
    ).json()
    third = (
        await client.get(
            path, headers=admin_headers, params={"limit": 2, "after": second["next_after"]}
        )
    ).json()
    assert [row["event_sequence"] for page in [first, second, third] for row in page["items"]] == [
        1,
        2,
        3,
        4,
        5,
    ]
    alerts = (await client.get(path, headers=admin_headers, params={"alerts": "true"})).json()
    assert [row["event_sequence"] for row in alerts["items"]] == [4, 5]
    assert all(row["alert"] for row in alerts["items"])
    empty = (await client.get(path, headers=admin_headers, params={"after": 5})).json()
    assert empty == {"items": [], "next_after": 5}


async def test_digest_compares_decisions_without_run_id_or_processing_time(
    client, event, detector, cache, admin_headers
):
    await ingest([event(), event(occurred_at=1001, success=False)])
    await process_run(LIVE_RUN, detector, cache)
    replay = (await client.post("/admin/replays", headers=admin_headers)).json()
    await process_run(replay["id"], detector, cache)
    original = (await client.get(f"/admin/runs/{LIVE_RUN}/digest", headers=admin_headers)).json()
    repeated = (
        await client.get(f"/admin/runs/{replay['id']}/digest", headers=admin_headers)
    ).json()
    assert original == repeated
    assert original["count"] == 2
    assert len(original["sha256"]) == 64
    prefix = (
        await client.get(
            f"/admin/runs/{LIVE_RUN}/digest", headers=admin_headers, params={"through": 1}
        )
    ).json()
    assert prefix["count"] == prefix["through"] == 1
    assert prefix["sha256"] != original["sha256"]


async def test_review_requires_version_and_returns_the_latest_review(
    client, event, detector, cache, admin_headers
):
    await ingest([event()])
    await process_run(LIVE_RUN, detector, cache)
    path = f"/admin/runs/{LIVE_RUN}/decisions/1/review"
    body = {"verdict": "needs_context", "note": "Нужен контекст пользовательского запроса."}
    assert (await client.put(path, headers=admin_headers, json=body)).status_code == 422
    first = await client.put(path, headers={**admin_headers, "If-Match": "0"}, json=body)
    assert first.status_code == 200
    assert first.json()["version"] == 1
    updated = {"verdict": "false_positive", "note": "Это ожидаемый импорт данных."}
    assert (
        await client.put(path, headers={**admin_headers, "If-Match": "0"}, json=updated)
    ).status_code == 409
    second = await client.put(path, headers={**admin_headers, "If-Match": "1"}, json=updated)
    assert second.status_code == 200
    assert second.json()["version"] == 2
    row = (await client.get(f"/admin/runs/{LIVE_RUN}/decisions", headers=admin_headers)).json()[
        "items"
    ][0]
    assert row["review_version"] == 2
    assert row["review"] == updated


async def test_readiness_requires_matching_model_and_recent_worker_heartbeat(
    client, detector, cache
):
    assert (await client.get("/health")).status_code == 200
    assert (await client.get("/ready")).status_code == 503
    await process_run(LIVE_RUN, detector, cache)
    assert (await client.get("/ready")).status_code == 503
    async with engine.begin() as conn:
        await execute(conn, "INSERT INTO heartbeats(name) VALUES('worker')")
    assert (await client.get("/ready")).status_code == 200
    detector.identity = "incompatible-model"
    assert (await client.get("/ready")).status_code == 503
    detector.identity = "test-model"
    async with engine.begin() as conn:
        await execute(
            conn, "UPDATE heartbeats SET seen_at=now()-interval '30 seconds' WHERE name='worker'"
        )
    assert (await client.get("/ready")).status_code == 503


async def test_drift_uses_only_scored_decisions(client, event, detector, cache, admin_headers):
    await ingest([event(occurred_at=1000), event(occurred_at=900)])
    await process_run(LIVE_RUN, detector, cache)
    response = await client.get("/admin/drift", headers=admin_headers)
    assert response.status_code == 200
    assert response.json()["sample_count"] == 1
    detector.identity = "incompatible-model"
    assert (await client.get("/admin/drift", headers=admin_headers)).status_code == 503


async def test_missing_run_returns_not_found(client, admin_headers):
    missing = uuid4()
    assert (await client.get(f"/admin/runs/{missing}", headers=admin_headers)).status_code == 404
    assert (
        await client.get(f"/admin/runs/{missing}/digest", headers=admin_headers)
    ).status_code == 404
