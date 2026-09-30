from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="VectorLake", version="0.1.0")

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readiness() -> dict[str, str]:
        return {"status": "ready"}

    return app
