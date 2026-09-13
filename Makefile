.PHONY: up down check test smoke recovery evaluate logs
up:
	docker compose up --build -d --wait --wait-timeout 180
down:
	docker compose down
check:
	uv sync --extra dev --frozen
	uv run ruff check .
	uv run ruff format --check .
test:
	docker compose --profile test build test
	docker compose --profile test run --rm test
smoke:
	docker compose exec -T api python scripts/smoke.py
recovery:
	uv run python scripts/recovery_smoke.py
evaluate:
	uv run python scripts/train_evaluate.py
logs:
	docker compose logs -f --tail=100
