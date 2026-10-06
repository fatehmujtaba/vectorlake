UV ?= uv
UV_CACHE_DIR ?= /tmp/vectorlake-uv-cache
export UV_CACHE_DIR
TORCH_HOME ?= $(CURDIR)/.artifacts/torch
export TORCH_HOME

.PHONY: sync test test-inference lint format build run vector-service bridge bridge-once processor processor-up processor-down inference archive-writer semantic-indexer proto infra-up infra-down bootstrap smoke probe-milvus e2e-m2 e2e-m3 e2e-m4 e2e-m4-revoke e2e-m5-inference e2e-m5-archive e2e-m5-search seed-search evaluate-m5-search evaluate-m4 verify-worker-m3 audit-dedup reconcile-outcomes

sync:
	$(UV) sync --locked --dev

test:
	$(UV) run --locked pytest

test-inference:
	$(UV) run --locked --extra inference pytest

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
	$(UV) run --locked --extra inference uvicorn services.api.main:app --host 127.0.0.1 --port 8000 --workers 1

vector-service:
	$(UV) run --locked uvicorn services.vector_service.main:app --host 127.0.0.1 --port 8001 --workers 1

bridge:
	$(UV) run --locked python -m services.event_bridge.main

bridge-once:
	$(UV) run --locked python -c 'from vectorlake.bridge import EventBridge; print(EventBridge().poll_once())'

processor:
	$(UV) run --locked python -m services.processor.main

inference:
	$(UV) run --locked --extra inference python -m services.inference_worker.main

archive-writer:
	$(UV) run --locked --extra archive python -m services.archive_writer.main

semantic-indexer:
	$(UV) run --locked python -m services.semantic_indexer.main

processor-up:
	docker compose --profile workers up -d --build --wait processor

processor-down:
	docker compose --profile workers stop processor

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

e2e-m3:
	$(UV) run --locked python -m infrastructure.e2e_m3

e2e-m4:
	$(UV) run --locked python -m infrastructure.e2e_m4

e2e-m4-revoke:
	$(UV) run --locked python -m infrastructure.e2e_m4_revocation

e2e-m5-inference:
	$(UV) run --locked --extra inference python -m infrastructure.e2e_m5_inference

e2e-m5-archive:
	$(UV) run --locked --extra inference --extra archive python -m infrastructure.e2e_m5_archive

SEARCH_TEST_IMAGE ?= .artifacts/m4/kaggle/extracted/dataset/animal/animal_evening_time_frame_7817.jpg

e2e-m5-search:
	$(UV) run --locked --extra inference --extra archive python -m infrastructure.e2e_m5_search --image "$(SEARCH_TEST_IMAGE)"

seed-search:
	$(UV) run --locked --extra inference --extra archive python -m infrastructure.seed_image_search --image "$(SEARCH_TEST_IMAGE)"

evaluate-m5-search:
	$(UV) run --locked --extra inference python -m infrastructure.evaluate_image_search --manifest .artifacts/m4/kaggle/manifest.jsonl --output .artifacts/m5/search_calibration_report.json

evaluate-m4:
	$(UV) run --locked python -m infrastructure.evaluate_near_copy

verify-worker-m3:
	$(UV) run --locked python -m infrastructure.verify_worker_m3

audit-dedup:
	$(UV) run --locked python -m infrastructure.audit_dedup_policy

reconcile-outcomes:
	$(UV) run --locked python -m infrastructure.reconcile_outcomes
