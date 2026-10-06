from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryFile
from typing import BinaryIO

import httpx

from vectorlake.common.local import LocalInfrastructureSettings


@dataclass(frozen=True)
class SearchStack:
    api_url: str
    settings: LocalInfrastructureSettings


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _ready(process: subprocess.Popen[bytes], url: str, log: BinaryIO) -> None:
    deadline = time.monotonic() + 30
    with httpx.Client(timeout=1, trust_env=False) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log.seek(0)
                raise RuntimeError(f"search service exited: {log.read().decode()[-4000:]}")
            try:
                if client.get(f"{url}/readyz").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise TimeoutError(f"search service did not become ready: {url}")


@contextmanager
def local_search_stack(database: Path) -> Iterator[SearchStack]:
    """Start isolated HTTP services and always stop them after an integration drill."""
    root = Path(__file__).resolve().parents[1]
    vector_port = _port()
    api_port = _port()
    while api_port == vector_port:
        api_port = _port()
    local = LocalInfrastructureSettings()
    local = local.model_copy(
        update={
            "semantic_db": str(database),
            "vector_service_url": f"http://127.0.0.1:{vector_port}",
        }
    )
    environment = os.environ | {
        "VECTORLAKE_ENVIRONMENT": "local",
        "VECTORLAKE_LOCAL_VECTOR_SERVICE_URL": local.vector_service_url,
        "VECTORLAKE_LOCAL_SEMANTIC_DB": local.semantic_db,
        "VECTORLAKE_LOCAL_VECTOR_SERVICE_TOKEN": local.vector_service_token,
        "VECTORLAKE_LOCAL_API_TOKEN": local.api_token,
        "TORCH_HOME": os.environ.get("TORCH_HOME", str(root / ".artifacts/torch")),
    }
    processes: list[subprocess.Popen[bytes]] = []
    with TemporaryFile() as vector_log, TemporaryFile() as api_log:
        try:
            for module, port, log in (
                ("services.vector_service.main", vector_port, vector_log),
                ("services.api.main", api_port, api_log),
            ):
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        f"{module}:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--workers",
                        "1",
                    ],
                    cwd=root,
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                processes.append(process)
                _ready(process, f"http://127.0.0.1:{port}", log)
            yield SearchStack(f"http://127.0.0.1:{api_port}", local)
        finally:
            for process in reversed(processes):
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
