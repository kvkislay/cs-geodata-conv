from loguru import logger
from redis import Redis
from rq import Queue

from ..utils.configs import sysconfig

try:
    lq = Queue(
        "layers",
        connection=Redis(
            host=sysconfig.get("redis", "host"), port=sysconfig.getint("redis", "port")
        ),
    )

    iq = Queue(
        "id",
        connection=Redis(
            host=sysconfig.get("redis", "host"), port=sysconfig.getint("redis", "port")
        ),
    )
except Exception as e:
    logger.error("Failed to connect to Redis: %s", e)
    exit(1)


def get_status(task_id: str) -> dict[str, str]:
    task = lq.fetch_job(task_id)
    if not task:
        task = iq.fetch_job(task_id)
    if not task:
        return {"status": "not found"}
    return {"status": task.get_status().name}
