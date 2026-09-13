import asyncio
import logging

from .db import engine, execute

log = logging.getLogger(__name__)


async def heartbeat(name):
    while True:
        try:
            async with engine.begin() as conn:
                await execute(
                    conn,
                    "INSERT INTO heartbeats(name) VALUES(:name) ON CONFLICT(name) DO UPDATE SET seen_at=now()",
                    name=name,
                )
        except Exception:
            log.exception("Cannot save heartbeat")
        await asyncio.sleep(3)
