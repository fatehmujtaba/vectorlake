from fastapi.testclient import TestClient

from vectorlake.api import create_app


def test_health_and_readiness() -> None:
    with TestClient(create_app()) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/readyz").json() == {"status": "ready"}
