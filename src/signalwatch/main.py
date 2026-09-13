import asyncio
import hashlib
import secrets
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from .cache import WindowCache
from .catalog import ingest, review, start_replay
from .config import LIVE_RUN, settings
from .db import canonical, engine, execute, one
from .detector import Detector
from .schemas import EventBatch, Review

LAG = Gauge("signalwatch_unprocessed_events", "Events waiting for a live decision")
PENDING = Gauge(
    "signalwatch_pending_notifications", "Outbox rows waiting for publisher confirmation"
)
DECISIONS = Gauge("signalwatch_saved_decisions", "Durable live decisions", ["status"])


async def admin(x_admin_token: str = Header(default="")):
    if not secrets.compare_digest(x_admin_token.encode(), settings.admin_token.encode()):
        raise HTTPException(403, "Administrator token required")


async def producer(x_ingest_token: str = Header(default="")):
    if not secrets.compare_digest(x_ingest_token.encode(), settings.ingest_token.encode()):
        raise HTTPException(403, "Ingestion token required")


@asynccontextmanager
async def lifespan(app):
    app.state.detector = await asyncio.to_thread(Detector, settings.artifact_path)
    app.state.cache = WindowCache()
    yield
    await app.state.cache.close()
    await engine.dispose()


app = FastAPI(
    title="SignalWatch",
    version="0.1.0",
    lifespan=lifespan,
    description="Activity events, causal time windows, anomaly scores, analyst reviews and reproducible replay.",
)


@app.get("/health", tags=["Operations"])
async def health():
    async with engine.connect() as conn:
        await execute(conn, "SELECT 1")
    return {"status": "ok"}


@app.get("/ready", tags=["Operations"])
async def ready():
    async with engine.connect() as conn:
        row = await one(conn, "SELECT model_version,error FROM runs WHERE id=:id", id=LIVE_RUN)
        heartbeat = await one(
            conn,
            "SELECT seen_at>now()-interval '20 seconds' AS alive FROM heartbeats WHERE name='worker'",
        )
    if (
        row is None
        or row["model_version"] != app.state.detector.identity
        or row["error"]
        or not heartbeat
        or not heartbeat["alive"]
    ):
        raise HTTPException(503, "Detector worker is not ready")
    return {"status": "ready", "model_version": app.state.detector.identity}


@app.post("/events", status_code=202, dependencies=[Depends(producer)], tags=["Events"])
async def events(batch: EventBatch):
    return await ingest(batch.events)


@app.get("/admin/status", dependencies=[Depends(admin)], tags=["Operations"])
async def status():
    async with engine.connect() as conn:
        state = await one(conn, "SELECT * FROM catalog_state WHERE id=1")
        runs = list((await execute(conn, "SELECT * FROM runs ORDER BY created_at")).mappings())
        outbox = await one(
            conn, "SELECT count(*) AS pending FROM outbox WHERE published_at IS NULL"
        )
        beats = list(
            (
                await execute(
                    conn,
                    "SELECT name,seen_at,seen_at>now()-interval '20 seconds' AS alive FROM heartbeats ORDER BY name",
                )
            ).mappings()
        )
    return {
        "sequence": state["sequence"],
        "live_run_id": LIVE_RUN,
        "runs": runs,
        "outbox": outbox,
        "heartbeats": beats,
        "loaded_model": app.state.detector.identity,
    }


@app.post("/admin/replays", status_code=202, dependencies=[Depends(admin)], tags=["Replay"])
async def replay():
    return await start_replay(app.state.detector.identity)


@app.get("/admin/runs/{run_id}", dependencies=[Depends(admin)], tags=["Replay"])
async def get_run(run_id: UUID):
    async with engine.connect() as conn:
        run = await one(conn, "SELECT * FROM runs WHERE id=:id", id=str(run_id))
        if run is None:
            raise HTTPException(404, "Run not found")
        stats = await one(
            conn,
            "SELECT count(*) AS decisions,count(*) FILTER(WHERE alert) AS alerts,count(*) FILTER(WHERE status='late') AS late FROM decisions WHERE run_id=:id",
            id=str(run_id),
        )
    return {**run, "stats": stats}


@app.get("/admin/runs/{run_id}/decisions", dependencies=[Depends(admin)], tags=["Decisions"])
async def decisions(
    run_id: UUID,
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=200),
    alerts: bool = False,
):
    async with engine.connect() as conn:
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT d.*,e.event_id,e.body AS event,r.version AS review_version,r.body AS review FROM decisions d JOIN events e ON e.sequence=d.event_sequence LEFT JOIN LATERAL (SELECT version,body FROM reviews WHERE run_id=d.run_id AND event_sequence=d.event_sequence ORDER BY version DESC LIMIT 1) r ON true WHERE d.run_id=:run AND d.event_sequence>:after AND (NOT :alerts OR d.alert) ORDER BY d.event_sequence LIMIT :limit",
                    run=str(run_id),
                    after=after,
                    limit=limit,
                    alerts=alerts,
                )
            ).mappings()
        )
    return {"items": rows, "next_after": rows[-1]["event_sequence"] if rows else after}


@app.get("/admin/runs/{run_id}/digest", dependencies=[Depends(admin)], tags=["Replay"])
async def digest(run_id: UUID, through: int | None = Query(default=None, ge=0)):
    checksum, count = hashlib.sha256(), 0
    async with engine.connect() as conn:
        run = await one(conn, "SELECT cursor FROM runs WHERE id=:id", id=str(run_id))
        if run is None:
            raise HTTPException(404, "Run not found")
        bound = run["cursor"] if through is None else min(through, run["cursor"])
        rows = (
            await execute(
                conn,
                "SELECT event_sequence,status,features,result,alert FROM decisions WHERE run_id=:id AND event_sequence<=:bound ORDER BY event_sequence",
                id=str(run_id),
                bound=bound,
            )
        ).mappings()
        for row in rows:
            # Время вычисления и ID прогона различаются при повторе; сравниваем именно сохранённые решения.
            checksum.update((canonical(dict(row)) + "\n").encode())
            count += 1
    return {"sha256": checksum.hexdigest(), "count": count, "through": bound}


@app.put(
    "/admin/runs/{run_id}/decisions/{sequence}/review",
    dependencies=[Depends(admin)],
    tags=["Decisions"],
)
async def put_review(run_id: UUID, sequence: int, body: Review, if_match: int = Header(ge=0)):
    return await review(str(run_id), sequence, body, if_match)


@app.get("/admin/drift", dependencies=[Depends(admin)], tags=["Monitoring"])
async def drift(limit: int = Query(default=1000, ge=100, le=5000)):
    async with engine.connect() as conn:
        run = await one(conn, "SELECT model_version FROM runs WHERE id=:id", id=LIVE_RUN)
        if run["model_version"] != app.state.detector.identity:
            raise HTTPException(503, "Detector version does not match the live run")
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT features FROM decisions WHERE run_id=:id AND status='scored' ORDER BY event_sequence DESC LIMIT :limit",
                    id=LIVE_RUN,
                    limit=limit,
                )
            ).mappings()
        )
    return await asyncio.to_thread(app.state.detector.drift, [row["features"] for row in rows])


@app.get("/metrics", tags=["Operations"])
async def metrics():
    async with engine.connect() as conn:
        lag = await one(
            conn,
            "SELECT c.sequence-r.cursor AS lag FROM catalog_state c CROSS JOIN runs r WHERE r.id=:id",
            id=LIVE_RUN,
        )
        pending = await one(conn, "SELECT count(*) AS n FROM outbox WHERE published_at IS NULL")
        counts = list(
            (
                await execute(
                    conn,
                    "SELECT status,count(*) AS n FROM decisions WHERE run_id=:id GROUP BY status",
                    id=LIVE_RUN,
                )
            ).mappings()
        )
    LAG.set(lag["lag"])
    PENDING.set(pending["n"])
    values = {row["status"]: row["n"] for row in counts}
    for label in ("scored", "late"):
        DECISIONS.labels(label).set(values.get(label, 0))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
