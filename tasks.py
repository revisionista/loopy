from redis import Redis
from rq import Queue
from surt import surt


PREFIX = "loopy"
SINCE_ID_KEY = f"{PREFIX}:since_id"
SURT_SORTED_SET_KEY = f"{PREFIX}:surt"

redis_conn = Redis()


def surt_task(url):
    surted = surt(url)
    response = redis_conn.zincrby(SURT_SORTED_SET_KEY, 1, surted)
    return response
