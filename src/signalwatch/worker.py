import asyncio
import contextlib
import json
import logging
from uuid import UUID

from .broker import connect
from .cache import WindowCache
from .config import settings
from .db import engine, execute
from .detector import Detector
from .operations import heartbeat
from .processing import process_run

log = logging.getLogger(__name__)


async def attempt(run_id, detector, cache):
    try:
        return await process_run(str(run_id), detector, cache)
    except Exception as error:
        log.exception("Run failed; decisions and cursor remain at the last commit")
        with contextlib.suppress(Exception):
            async with engine.begin() as conn:
                await execute(
                    conn,
                    "UPDATE runs SET error=:error WHERE id=:id",
                    id=str(run_id),
                    error=type(error).__name__,
                )
        raise


async def consume(detector, cache):
    while True:
        connection = None
        try:
            connection, _, queue = await connect()
            async with queue.iterator() as messages:
                async for message in messages:
                    try:
                        run_id = UUID(json.loads(message.body)["run_id"])
                        while await attempt(run_id, detector, cache):
                            pass
                        await message.ack()
                    except (ValueError, KeyError, TypeError):
                        log.exception("Malformed notification moved to the dead-letter queue")
                        await message.reject(requeue=False)
                    except Exception:
                        await message.reject(requeue=False)
        except Exception:
            log.exception("RabbitMQ unavailable; database polling continues")
            await asyncio.sleep(2)
        finally:
            if connection:
                await connection.close()


async def sweep(detector, cache):
    while True:
        progressed = False
        try:
            async with engine.connect() as conn:
                runs = list(
                    (
                        await execute(
                            conn, "SELECT id FROM runs WHERE status='running' ORDER BY created_at"
                        )
                    ).scalars()
                )
            for run_id in runs:
                with contextlib.suppress(Exception):
                    progressed |= bool(await attempt(run_id, detector, cache))
        except Exception:
            log.exception("Cannot read pending runs")
        # Если уведомление потерялось или брокер недоступен, этот опрос всё равно запустит обработку.
        await asyncio.sleep(0.05 if progressed else 1)


async def run():
    detector = await asyncio.to_thread(Detector, settings.artifact_path)
    cache = WindowCache()
    tasks = [
        asyncio.create_task(heartbeat("worker")),
        asyncio.create_task(consume(detector, cache)),
        asyncio.create_task(sweep(detector, cache)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await cache.close()
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
