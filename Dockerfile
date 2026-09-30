FROM python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e

COPY --from=ghcr.io/astral-sh/uv:0.12.9@sha256:8b940d3a9d65bed080436972241af2e21c84b5e8c9193f7014ed71479ee795ff /uv /uvx /bin/

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
COPY services/ services/
RUN uv sync --locked --no-dev --no-editable
RUN useradd --system --home-dir /app --no-create-home vectorlake

EXPOSE 8000
USER vectorlake
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
