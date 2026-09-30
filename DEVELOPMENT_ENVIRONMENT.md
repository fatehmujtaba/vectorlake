# Local development environment

Checked on an Apple Silicon Mac on 2026-09-30. The project uses `uv` for Python
and a Docker-compatible engine for service containers. Python packages live
in the project's `.venv`, not in the system Python installation.

| Dependency | Checked version | Status |
|---|---|---|
| uv | 0.12.9 | Installed |
| Python for VectorLake | 3.12.14 (arm64) | Installed with `uv` and pinned by `.python-version` |
| Docker CLI / Compose | 29.3.1 / 5.1.1 | Installed |
| Container engine | Colima 0.10.3, Docker Engine 29.5.2 | Running with 4 CPU, 8 GB RAM, 40 GB disk |
| Homebrew / Make / Git | 7.0.4 / 3.81 / 2.50.1 | Installed |
| AWS CLI | 2.34.23 | Installed for local smoke tests |
| Protocol Buffers compiler | 36.2 | Installed at `/opt/homebrew/bin/protoc` |
| Floci | 2.1.0 | Image pulled and health checked; stopped after verification |

Docker Desktop is installed but its daemon did not start during this check, so
Colima supplies the working Docker context. The shell's first `protoc` is an
older Anaconda x86_64 binary; use `/opt/homebrew/bin/protoc` for the native
compiler. No change to the user's shell profile is required.

The `uv.lock` file fixes the M0 application and developer dependencies:
FastAPI, Uvicorn, Pydantic Settings, OpenTelemetry, pytest, Ruff, mypy, and
HTTPX. `uv pip check` confirmed the installed environment is consistent.

Service components are installed as versioned project dependencies when their
milestone introduces runnable configuration:

- M1: Floci-backed S3, SQS, DynamoDB, MSK/Redpanda, and PostgreSQL. The Floci
  image is already present, but the broker/database operations still need
  end-to-end smoke tests.
- M4: PDQ, OpenCV, and scikit-image are installed with `uv`. A bounded,
  scope-keyed DynamoDB scan is the local candidate adapter because the live
  Milvus Lite binary-vector probe rejected `BinaryVector`. Production Milvus
  binary Hamming indexing still needs separate validation. The worker's
  Linux ARM image builds PDQ in a compiler-only builder stage.
- M5: torchvision/ResNet-18, PyArrow, and semantic Milvus indexing.
- M6: OpenTelemetry Collector, Prometheus, Grafana, and Jaeger containers.

Do not start the entire stack on a 16 GB laptop before its Compose resource
limits and service health checks are defined. The M1 bootstrap will pin its
service images and install the required containers through Compose.
