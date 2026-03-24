from loguru import logger

from src.app.models import LayerConversionRequest
from src.utils.rand_funcs import sim_work
from src.work.work_queue import lq
from src.conversion import clean_parquet


def handle_layers(request: LayerConversionRequest) -> dict:
    logger.info("Handling layers")
    tid = lq.enqueue(layer_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def layer_conversion(request: LayerConversionRequest) -> None:
    # If request.folder_path is the string path to your config JSON:
    config_file = request.folder_path

    try:
        logger.info(f"Starting conversion using config: {config_file}")

        # Ensure clean_parquet2.run is set up to take this string path
        clean_parquet.run(config_file)

        sim_work()
        logger.info(f"Finished processing {config_file}")

    except Exception as e:
        logger.error(f"Error in layer_conversion for {config_file}: {e}")
        raise e
