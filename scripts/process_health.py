import asyncio
import sys

from signalwatch.db import engine, one


async def main():
    async with engine.connect() as conn:
        row = await one(
            conn,
            "SELECT seen_at>now()-interval '20 seconds' AS alive FROM heartbeats WHERE name=:name",
            name=sys.argv[1],
        )
    await engine.dispose()
    return 0 if row and row["alive"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
