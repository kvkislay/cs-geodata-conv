FROM ghcr.io/astral-sh/uv:python3.14-alpine
WORKDIR /app

ENV UV_NO_DEV=1

WORKDIR /app

COPY pyproject.toml uv.lock /app/

RUN uv sync --locked

COPY . /app/


# RUN addgroup -S appgroup && adduser -S -D -G appgroup appuser
# USER app

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]
