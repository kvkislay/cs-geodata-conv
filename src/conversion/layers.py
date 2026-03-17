from loguru import logger

from src.app.models import LayerConversionRequest
from src.utils.rand_funcs import sim_work
from src.work.work_queue import lq


def handle_layers(request: LayerConversionRequest) -> dict:
    logger.info("Handling layers")
    tid = lq.enqueue(layer_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def layer_conversion(request: LayerConversionRequest) -> None:
    try:
        match request.hierarchy:
            case "mws":
                conv_algo_mws(request)
            case _:
                raise ValueError(f"Unsupported hierarchy: {request.hierarchy}")
    except Exception as e:
        logger.error(f"Error in layer conversion: {e}")
        raise


def conv_algo_mws(request: LayerConversionRequest) -> None:
    match request.resolution:
        case "fortnightly":
            create_fortnightly()
        case "annual":
            create_annual()
        case _:
            raise ValueError("Unsupported resolution")


def create_fortnightly() -> None:
    sim_work()
    logger.info("Creating fortnightly")


def create_annual() -> None:
    sim_work()
    logger.info("Creating annual")
