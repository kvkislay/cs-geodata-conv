from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI

from src.app.models import IDConversionRequest, LayerConversionRequest
from src.conversion.id import handle_id
from src.conversion.layers import handle_layers
from src.utils.checks import check_redis_connection, check_worker_status
from src.work.work_queue import get_status

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    # Startup tasks
    yield
    # Shutdown tasks


@app.get(path="/")
async def read_root() -> dict[str, str]:
    if not check_redis_connection():
        return {
            "status": "error",
            "message": "Redis connection or worker status check failed",
        }
    workers = check_worker_status()
    if not all(workers.values()):
        return {
            "status": "error",
            "message": "Worker status check failed",
        }
    if len(workers["id"]) == 0:
        return {
            "status": "error",
            "message": "No ID workers running",
        }
    if len(workers["layers"]) == 0:
        return {
            "status": "error",
            "message": "No layers workers running",
        }
    return {
        "status": "ok",
        "message": f"All systems connected! ID workers: {len(workers['id'])} Layers workers: {len(workers['layers'])}",
    }


@app.post(path="/v1/layers")
def create_layer(request: LayerConversionRequest) -> dict[str, str]:
    return handle_layers(request)


@app.post(path="/v1/ids")
def create_mws(request: IDConversionRequest) -> dict[str, str]:
    return handle_id(request)


@app.get(path="/v1/status")
def get_jobstatus(task_id: str) -> dict[str, str]:
    return get_status(task_id)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
