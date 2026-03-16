FROM ghcr.io/astral-sh/uv:python3.14-alpine
WORKDIR /app

COPY . /app/

ENV UV_NO_DEV=1

WORKDIR /app

RUN uv sync --locked

# RUN addgroup -S appgroup && adduser -S -D -G appgroup appuser
# USER app

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
