from loguru import logger

from src.app.models import IDConversionRequest
from src.utils.rand_funcs import sim_work
from src.work.work_queue import iq


def handle_id(request: IDConversionRequest) -> dict:
    logger.info("Handling ids")
    tid = iq.enqueue(id_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def id_conversion(request: IDConversionRequest) -> None:
    match request.type:
        case "mws":
            conv_algo_mws(request)
        case _:
            raise ValueError("Unsupported type")


def conv_algo_mws(request: IDConversionRequest) -> None:
    sim_work()
    logger.info(request.model_dump())
