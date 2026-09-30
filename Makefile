UV ?= uv
UV_CACHE_DIR ?= /tmp/vectorlake-uv-cache
export UV_CACHE_DIR

.PHONY: sync test lint format build run

sync:
	$(UV) sync --locked --dev

test:
	$(UV) run --locked pytest

lint:
	$(UV) run --locked ruff check src services tests
	$(UV) run --locked ruff format --check src services tests
	$(UV) run --locked mypy src services tests

format:
	$(UV) run --locked ruff check --fix src services tests
	$(UV) run --locked ruff format src services tests

build:
	$(UV) build
	docker build -t vectorlake:dev .

run:
	$(UV) run --locked uvicorn services.api.main:app --host 127.0.0.1 --port 8000
