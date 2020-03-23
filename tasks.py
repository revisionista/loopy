import logging

import savepagenow
from redis import Redis
from rq import Queue
from surt import surt


PREFIX = "loopy"
SINCE_ID_KEY = f"{PREFIX}:since_id"
SURT_SORTED_SET_KEY = f"{PREFIX}:surt"

redis_conn = Redis()

log = logging.getLogger(__name__)


def surt_task(url):
    surted = surt(url)
    response = redis_conn.zincrby(SURT_SORTED_SET_KEY, 1, surted)

    """
        Saves a URL with the Wayback Machine.
        """

    # Sent URL to Internet Archive
    # This is required so we throw an error if it fails
    log.debug(f"Archiving {url} to IA")
    try:
        ia_url, ia_captured = savepagenow.capture_or_cache(
            url, user_agent="revisionista (https://revisionista.pt)"
        )
        log.info(f"Saving {url} memento URL {ia_url}")
    except Exception as e:
        log.error(e)

    return response
