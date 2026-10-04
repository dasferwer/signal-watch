"""Проверка выделенных тестовых ресурсов до импорта приложения и любых записей."""

import os
from collections.abc import Mapping
from urllib.parse import urlsplit

DATABASES: dict[str, tuple[str, str, int, str, str]] = {
    "DATABASE_URL": ("postgresql+asyncpg", "test-db", 5432, "/signal", "signal")
}
ENDPOINTS: dict[str, tuple[str, str, int, tuple[str, ...]]] = {
    "REDIS_URL": ("redis", "test-redis", 6379, ("/0",)),
    "RABBITMQ_URL": ("amqp", "test-rabbitmq", 5672, ("/",)),
}


class UnsafeTestEnvironment(RuntimeError):
    """Нельзя доказать, что команда обращается только к выделенным тестовым ресурсам."""


def refuse(variable: str) -> None:
    # Сам URL не печатаем: в ошибочно выбранном окружении он может содержать секрет.
    raise UnsafeTestEnvironment(
        f"Небезопасное тестовое окружение: проверьте {variable}. "
        "Используйте отдельный профиль Compose с TESTING=true."
    )


def valid_url(
    raw: str, scheme: str, host: str, port: int, paths: tuple[str, ...], user: str | None = None
) -> bool:
    try:
        url = urlsplit(raw)
        valid = bool(raw) and not any(ord(char) <= 32 or ord(char) == 127 for char in raw)
        valid = valid and "\\" not in raw and "?" not in raw and "#" not in raw
        valid = valid and url.scheme == scheme and url.hostname == host
        valid = valid and url.port == port and url.path in paths
        if user is not None:
            valid = valid and url.username == user and bool(url.password)
            valid = valid and url.netloc.count("@") == 1
        elif scheme in {"http", "redis"}:
            valid = valid and url.username is None and url.password is None
        return bool(valid)
    except ValueError:
        return False


def ensure_test_environment(environ: Mapping[str, str] | None = None) -> None:
    env = os.environ if environ is None else environ
    names = {"TESTING", "MIGRATION_DATABASE_URL", *DATABASES, *ENDPOINTS}
    # Pydantic читает env без учёта регистра; неоднозначное имя переменной не допускаем.
    for key in env:
        if key.upper() in names and key != key.upper():
            refuse(key.upper())
        if key.upper().startswith("PG"):
            refuse(key)
    if env.get("TESTING") != "true":
        refuse("TESTING")
    databases = DATABASES.copy()

    if "MIGRATION_DATABASE_URL" in env:
        refuse("MIGRATION_DATABASE_URL")
    for variable, (driver, host, port, path, user) in databases.items():
        if not valid_url(env.get(variable, ""), driver, host, port, (path,), user):
            refuse(variable)
    for variable, (scheme, host, port, paths) in ENDPOINTS.items():
        if not valid_url(env.get(variable, ""), scheme, host, port, paths):
            refuse(variable)


if __name__ == "__main__":
    try:
        ensure_test_environment()
    except UnsafeTestEnvironment as exc:
        raise SystemExit(str(exc)) from None
