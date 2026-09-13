import asyncio
import contextlib
import logging

import aio_pika

from .broker import QUEUE, connect
from .config import settings
from .db import canonical, engine, execute
from .operations import heartbeat

log = logging.getLogger(__name__)


async def dispatch(channel):
    async with engine.begin() as conn:
        rows = list(
            (
                await execute(
                    conn,
                    "SELECT id,run_id FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED",
                )
            ).mappings()
        )
        for row in rows:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=canonical({"run_id": str(row["run_id"])}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(row["id"]),
                ),
                routing_key=QUEUE,
                mandatory=True,
                timeout=5,
            )
            if settings.dispatcher_after_publish_delay:
                log.warning(
                    "Crash check: broker confirmed notification, commit delayed; outbox=%s",
                    row["id"],
                )
                await asyncio.sleep(settings.dispatcher_after_publish_delay)
            # Подтверждение брокера и commit БД могут разойтись при падении. Повторное уведомление не создаёт повторного решения.
            await execute(conn, "UPDATE outbox SET published_at=now() WHERE id=:id", id=row["id"])
    return len(rows)


async def run():
    pulse = asyncio.create_task(heartbeat("dispatcher"))
    try:
        while True:
            connection = None
            try:
                connection, channel, _ = await connect()
                while True:
                    if not await dispatch(channel):
                        await asyncio.sleep(0.5)
            except Exception:
                log.exception("Cannot publish wake-up notifications; the outbox is retained")
                await asyncio.sleep(2)
            finally:
                if connection:
                    await connection.close()
    finally:
        pulse.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pulse
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
