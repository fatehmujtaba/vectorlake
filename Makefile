UV ?= uv
UV_CACHE_DIR ?= /tmp/vectorlake-uv-cache
export UV_CACHE_DIR

.PHONY: sync test lint format build run bridge bridge-once proto infra-up infra-down bootstrap smoke probe-milvus e2e-m2

sync:
	$(UV) sync --locked --dev

test:
	$(UV) run --locked pytest

lint:
	$(UV) run --locked ruff check src services infrastructure tests
	$(UV) run --locked ruff format --check src services infrastructure tests
	$(UV) run --locked mypy src services infrastructure tests

format:
	$(UV) run --locked ruff format src services infrastructure tests
	$(UV) run --locked ruff check --fix src services infrastructure tests

build:
	$(UV) build
	docker build -t vectorlake:dev .

run:
	$(UV) run --locked uvicorn services.api.main:app --host 127.0.0.1 --port 8000

bridge:
	$(UV) run --locked python -m services.event_bridge.main

bridge-once:
	$(UV) run --locked python -c 'from vectorlake.bridge import EventBridge; print(EventBridge().poll_once())'

proto:
	$(UV) run --locked python -m grpc_tools.protoc -I proto --python_out=src --pyi_out=src proto/vectorlake/events/v1/events.proto

infra-up:
	docker compose up -d --wait floci redpanda

infra-down:
	docker compose down

bootstrap:
	$(UV) run --locked python -m infrastructure.bootstrap

smoke:
	$(UV) run --locked python -m infrastructure.smoke

probe-milvus:
	$(UV) run --locked python -m infrastructure.probe_milvus

e2e-m2:
	$(UV) run --locked python -m infrastructure.e2e_m2
