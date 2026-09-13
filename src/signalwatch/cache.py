import hashlib
import json

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import settings
from .db import canonical
from .schemas import Event

# Поздно завершившийся worker не должен затереть более свежую копию окна.
STORE = """
local old = redis.call('GET', KEYS[1])
if old then
    local ok, value = pcall(cjson.decode, old)
    if ok and type(value) == 'table' then
        local cursor = tonumber(value.cursor)
        if cursor and cursor > tonumber(ARGV[1]) then return 0 end
    end
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', 3600)
return 1
"""


class WindowCache:
    def __init__(self):
        self.redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=0.3,
            socket_timeout=0.3,
        )

    @staticmethod
    def key(run_id):
        return f"signalwatch:window:{run_id}"

    async def load(self, run_id, cursor, identity):
        try:
            raw = await self.redis.get(self.key(run_id))
            if raw is None:
                return None
            value = json.loads(raw)
            history = value["history"]
            if (
                value["cursor"] != cursor
                or value["model"] != identity
                or value["sha256"] != hashlib.sha256(canonical(history).encode()).hexdigest()
            ):
                return None
            if not isinstance(history, list) or any(
                not isinstance(event, dict)
                or not {
                    "event_id",
                    "occurred_at",
                    "actor_id",
                    "source_ip",
                    "country",
                    "kind",
                    "success",
                    "bytes_sent",
                }
                <= event.keys()
                for event in history
            ):
                return None
            return [Event.model_validate(event).model_dump(mode="json") for event in history]
        except (RedisError, ValueError, KeyError, TypeError):
            return None

    async def save(self, run_id, cursor, identity, history):
        value = {
            "cursor": cursor,
            "model": identity,
            "history": history,
            "sha256": hashlib.sha256(canonical(history).encode()).hexdigest(),
        }
        try:
            return bool(await self.redis.eval(STORE, 1, self.key(run_id), cursor, canonical(value)))
        except RedisError:
            return False

    async def close(self):
        await self.redis.aclose()
