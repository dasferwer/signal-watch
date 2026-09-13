import asyncio
import logging
import time

from .config import settings
from .db import canonical, engine, execute, one
from .features import ALLOWED_LATENESS_SECONDS, WINDOW_SECONDS, extract_features

log = logging.getLogger(__name__)


async def process_run(run_id, detector, cache):
    snapshot = None
    async with engine.begin() as conn:
        lock = await one(
            conn,
            "SELECT pg_try_advisory_xact_lock(hashtextextended(:run,23)) AS acquired",
            run=str(run_id),
        )
        if not lock["acquired"]:
            return 0
        run = await one(conn, "SELECT * FROM runs WHERE id=:run FOR UPDATE", run=run_id)
        if run is None or run["status"] == "completed":
            return 0
        if run["model_version"] is not None and run["model_version"] != detector.identity:
            raise RuntimeError("Run requires a different detector version")
        state = await one(conn, "SELECT sequence FROM catalog_state WHERE id=1")
        bound = state["sequence"] if run["upper_bound"] is None else run["upper_bound"]
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT * FROM events WHERE sequence>:cursor AND sequence<=:bound ORDER BY sequence LIMIT :limit",
                    cursor=run["cursor"],
                    bound=bound,
                    limit=settings.batch_size,
                )
            ).mappings()
        )
        cursor, watermark = run["cursor"], run["watermark"]
        history = await cache.load(str(run_id), cursor, detector.identity)
        if history is None:
            # Redis хранит только копию. После потери кеша восстанавливаем окно из уже принятых решений этого прогона.
            history = [
                row["body"]
                for row in (
                    await execute(
                        conn,
                        "SELECT e.body FROM events e JOIN decisions d ON d.event_sequence=e.sequence WHERE d.run_id=:run AND d.status='scored' AND e.sequence<=:cursor AND e.occurred_at>:cutoff ORDER BY e.sequence",
                        run=run_id,
                        cursor=cursor,
                        cutoff=watermark - WINDOW_SECONDS - ALLOWED_LATENESS_SECONDS,
                    )
                ).mappings()
            ]
        for row in rows:
            started = time.perf_counter()
            body = row["body"]
            occurred = body["occurred_at"]
            if occurred < watermark - ALLOWED_LATENESS_SECONDS:
                status, features = "late", {}
                result = {
                    "reason": "Событие пришло позже допустимого срока; прежние решения сохранены",
                    "model_version": detector.identity,
                }
                alert = False
            else:
                status = "scored"
                features = extract_features(history, body)
                result = await asyncio.to_thread(detector.evaluate, features)
                alert = result["alert"]
                watermark = max(watermark, occurred)
                history.append(body)
                history = [
                    event
                    for event in history
                    if event["occurred_at"] > watermark - WINDOW_SECONDS - ALLOWED_LATENESS_SECONDS
                ]
            await execute(
                conn,
                "INSERT INTO decisions(run_id,event_sequence,status,features,result,alert,processing_ms) VALUES(:run,:seq,:status,CAST(:features AS jsonb),CAST(:result AS jsonb),:alert,:duration)",
                run=run_id,
                seq=row["sequence"],
                status=status,
                features=canonical(features),
                result=canonical(result),
                alert=alert,
                duration=(time.perf_counter() - started) * 1000,
            )
            cursor = row["sequence"]
        if rows and settings.worker_before_commit_delay:
            log.warning(
                "Crash check: decisions inserted, commit delayed; run=%s sequence=%s",
                run_id,
                cursor,
            )
            await asyncio.sleep(settings.worker_before_commit_delay)
        completed = run["kind"] == "replay" and cursor == bound
        await execute(
            conn,
            "UPDATE runs SET cursor=:cursor,watermark=:watermark,model_version=:model,error=NULL,status=:status,completed_at=CASE WHEN :completed THEN now() ELSE completed_at END WHERE id=:run",
            cursor=cursor,
            watermark=watermark,
            model=detector.identity,
            status="completed" if completed else "running",
            completed=completed,
            run=run_id,
        )
        if rows:
            snapshot = (str(run_id), cursor, detector.identity, history)
    if snapshot:
        # Сохраняем кеш после commit. При сбое между этими действиями несовпавший курсор заставит восстановить окно из БД.
        await cache.save(*snapshot)
    return len(rows)
