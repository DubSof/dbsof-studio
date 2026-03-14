FROM ubuntu:24.04
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
WORKDIR /app

ENV UV_PYTHON_DOWNLOADS=auto
ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY main.py ./
COPY data/ ./data/

EXPOSE 5757
CMD ["uv", "run", "fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "5757"]
