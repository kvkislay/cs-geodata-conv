from loguru import logger

from src.app.models import LayerConversionRequest
from src.utils.rand_funcs import sim_work
from src.work.work_queue import lq


def handle_layers(request: LayerConversionRequest) -> dict:
    logger.info("Handling layers")
    tid = lq.enqueue(layer_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid._status,
    }


def layer_conversion(request: LayerConversionRequest) -> None:
    logger.info("Converting layers to a different format")
    sim_work()
    logger.info(request.model_dump())
