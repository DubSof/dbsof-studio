# Build stage: install dependencies
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/
WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --python /usr/local/bin/python3

# Runtime stage
FROM python:3.14-slim
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY main.py ./
COPY data/ ./data/

EXPOSE 5757
CMD ["/app/.venv/bin/fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "5757"]
