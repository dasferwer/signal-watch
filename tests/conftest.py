from urllib.parse import urlparse
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.engine import make_url

from signalwatch.cache import WindowCache
from signalwatch.config import LIVE_RUN, settings
from signalwatch.db import engine, execute
from signalwatch.schemas import Event


class FakeDetector:
    identity = "test-model"

    def evaluate(self, features):
        score = features["actor_requests_60s"] + features["actor_failures_300s"]
        rules = features["source_signups_300s"]
        return {
            "model_score": float(score),
            "model_threshold": 3.0,
            "model_alert": bool(score > 3),
            "policy": "model",
            "alert": bool(score > 3),
            "rule_score": float(rules),
            "rule_threshold": 2.0,
            "rule_alert": bool(rules > 2),
            "reasons": ["В тестовом окне превышен порог."] if score > 3 else [],
            "model_version": self.identity,
        }

    def drift(self, rows):
        return {
            "status": "insufficient_data",
            "sample_count": len(rows),
            "model_version": self.identity,
        }


@pytest.fixture
def detector():
    return FakeDetector()


@pytest.fixture
def event():
    def make(**changes):
        return Event(
            **{
                "event_id": uuid4(),
                "occurred_at": 1000.0,
                "actor_id": "actor-1",
                "source_ip": "192.0.2.1",
                "country": "DE",
                "kind": "api",
                "success": True,
                "bytes_sent": 100,
                **changes,
            }
        )

    return make


@pytest.fixture
async def cache():
    database = make_url(settings.database_url)
    redis = urlparse(settings.redis_url)
    rabbit = urlparse(settings.rabbitmq_url)
    # Проверяем адреса до первого запроса: очистка разрешена только в отдельном тестовом стенде.
    if not (
        settings.testing
        and database.host == "test-db"
        and database.database == "signal"
        and redis.hostname == "test-redis"
        and redis.port == 6379
        and redis.path == "/0"
        and rabbit.hostname == "test-rabbitmq"
        and rabbit.port == 5672
    ):
        pytest.fail(
            "Tests require TESTING=true and isolated test-db/test-redis/test-rabbitmq hosts"
        )
    instance = WindowCache()
    yield instance
    await instance.close()


@pytest.fixture
async def clean_state(cache):
    await cache.redis.flushdb()
    async with engine.begin() as conn:
        await execute(
            conn,
            "TRUNCATE reviews,decisions,outbox,events,runs,heartbeats,catalog_state RESTART IDENTITY",
        )
        await execute(conn, "INSERT INTO catalog_state(id) VALUES(1)")
        await execute(conn, "INSERT INTO runs(id,kind) VALUES(:run,'live')", run=LIVE_RUN)
    yield


@pytest.fixture
async def client(cache, detector):
    from signalwatch.main import app

    # Lifespan здесь не запускается: проверяем API с известной моделью, без фоновых процессов.
    app.state.detector = detector
    app.state.cache = cache
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as session:
        yield session


@pytest.fixture
def admin_headers():
    return {"X-Admin-Token": settings.admin_token}


@pytest.fixture
def ingest_headers():
    return {"X-Ingest-Token": settings.ingest_token}


@pytest.fixture(scope="session", autouse=True)
async def close_pool():
    yield
    await engine.dispose()
