import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from .config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=10,
    connect_args={"server_settings": {"timezone": "UTC"}},
)


async def execute(conn, sql, **params):
    return await conn.execute(text(sql), params)


async def one(conn, sql, **params):
    return (await execute(conn, sql, **params)).mappings().first()


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
