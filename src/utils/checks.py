# import requests
from collections.abc import Awaitable

from redis import Redis
from rq import Worker

from src.utils.configs import sysconfig
from src.work.work_queue import iq, lq

client = Redis(
    host=sysconfig.get("redis", "host"),
    port=sysconfig.getint("redis", "port"),
    socket_connect_timeout=3,
)


def check_redis_connection() -> Awaitable[bool] | bool:
    res = client.ping()
    return res


def check_worker_status() -> dict:
    workers = {}
    workers[iq.name] = Worker.all(queue=iq)
    workers[lq.name] = Worker.all(queue=lq)
    return workers
