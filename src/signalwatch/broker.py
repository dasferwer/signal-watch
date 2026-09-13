import aio_pika

from .config import settings

QUEUE = "signalwatch.jobs"
DEAD_QUEUE = "signalwatch.dead"


async def connect():
    connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=3, heartbeat=10)
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    await channel.set_qos(prefetch_count=1)
    await channel.declare_queue(DEAD_QUEUE, durable=True)
    queue = await channel.declare_queue(
        QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": DEAD_QUEUE},
    )
    return connection, channel, queue
