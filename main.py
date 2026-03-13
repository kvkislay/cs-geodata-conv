from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI

from src.app.models import IDConversionRequest, LayerConversionRequest
from src.conversion.id import handle_id
from src.conversion.layers import handle_layers
from src.work.work_queue import get_status

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    # Startup tasks
    yield
    # Shutdown tasks


@app.get(path="/")
async def read_root() -> dict[str, str]:
    return {"status": "ok"}


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
