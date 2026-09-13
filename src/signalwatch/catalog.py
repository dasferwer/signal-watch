import hashlib
from uuid import uuid4

from fastapi import HTTPException

from .config import LIVE_RUN
from .db import canonical, engine, execute, one


async def ingest(events):
    receipts = []
    async with engine.begin() as conn:
        state = await one(conn, "SELECT sequence FROM catalog_state WHERE id=1 FOR UPDATE")
        sequence = state["sequence"]
        for event in events:
            body = event.model_dump(mode="json")
            digest = hashlib.sha256(canonical(body).encode()).hexdigest()
            previous = await one(
                conn,
                "SELECT sequence,body_hash FROM events WHERE event_id=:id",
                id=str(event.event_id),
            )
            if previous:
                if previous["body_hash"] != digest:
                    raise HTTPException(409, "Event ID already belongs to a different payload")
                receipts.append(
                    {
                        "event_id": str(event.event_id),
                        "sequence": previous["sequence"],
                        "duplicate": True,
                    }
                )
                continue
            sequence += 1
            await execute(
                conn,
                "INSERT INTO events(sequence,event_id,occurred_at,body_hash,body) VALUES(:seq,:id,:time,:hash,CAST(:body AS jsonb))",
                seq=sequence,
                id=str(event.event_id),
                time=event.occurred_at,
                hash=digest,
                body=canonical(body),
            )
            receipts.append(
                {"event_id": str(event.event_id), "sequence": sequence, "duplicate": False}
            )
        if sequence != state["sequence"]:
            # Номер растёт под блокировкой: порядок журнала совпадает с порядком commit, пропущенных событий между пачками нет.
            await execute(conn, "UPDATE catalog_state SET sequence=:seq WHERE id=1", seq=sequence)
            await execute(conn, "INSERT INTO outbox(run_id) VALUES(:run)", run=LIVE_RUN)
    return {"events": receipts}


async def start_replay(identity):
    async with engine.begin() as conn:
        state = await one(conn, "SELECT sequence FROM catalog_state WHERE id=1 FOR UPDATE")
        count = await one(conn, "SELECT count(*) AS n FROM runs WHERE kind='replay'")
        if count["n"] >= 20:
            raise HTTPException(409, "Twenty replays retained; archive completed runs first")
        run_id = str(uuid4())
        run = await one(
            conn,
            "INSERT INTO runs(id,kind,upper_bound,model_version) VALUES(:id,'replay',:bound,:model) RETURNING *",
            id=run_id,
            bound=state["sequence"],
            model=identity,
        )
        await execute(conn, "INSERT INTO outbox(run_id) VALUES(:run)", run=run_id)
        return run


async def review(run_id, sequence, body, expected):
    async with engine.begin() as conn:
        decision = await one(
            conn,
            "SELECT alert FROM decisions WHERE run_id=:run AND event_sequence=:seq FOR UPDATE",
            run=run_id,
            seq=sequence,
        )
        if decision is None:
            raise HTTPException(404, "Decision not found")
        current = await one(
            conn,
            "SELECT version,body FROM reviews WHERE run_id=:run AND event_sequence=:seq ORDER BY version DESC LIMIT 1",
            run=run_id,
            seq=sequence,
        )
        version = current["version"] if current else 0
        if expected != version:
            if current and expected == version - 1 and current["body"] == body.model_dump():
                return {"version": version, **current["body"]}
            raise HTTPException(409, "Review changed; read the current version")
        await execute(
            conn,
            "INSERT INTO reviews(run_id,event_sequence,version,body) VALUES(:run,:seq,:version,CAST(:body AS jsonb))",
            run=run_id,
            seq=sequence,
            version=version + 1,
            body=canonical(body.model_dump()),
        )
        return {"version": version + 1, **body.model_dump()}
