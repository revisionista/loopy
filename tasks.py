import argparse
import codecs
import json
import logging
import os
import random
import signal
import sys
from collections import Counter
from datetime import datetime

from surt import surt
from twarc.client import interruptible_sleep
from twarc.client import Twarc
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError


# Add your API key here
consumer_key = os.getenv("TWITTER_CONSUMER_KEY")

# Add your API secret key here
consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")

access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

log = logging.getLogger("loopy")


def main():
    parser = get_argparser()
    args = parser.parse_args()

    logging.basicConfig(
        filename=args.log,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # get the output filehandle
    if args.output:
        fh = codecs.open(args.output, "wb", "utf8")
    else:
        fh = sys.stdout

    # catch ctrl-c so users don't see a stack trace
    signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(0))

    # Don't validate the keys if skipped explicitly
    if args.skip_key_validation:
        validate_keys = False
    else:
        validate_keys = True

    t = Twarc(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        connection_errors=args.connection_errors,
        http_errors=args.http_errors,
        tweet_mode=args.tweet_mode,
        protected=args.protected,
        validate_keys=validate_keys,
    )

    since_id = args.since_id

    surt_counter = Counter()
    while True:
        # Up to 800 Tweets are obtainable on the home timeline: 200 Tweets * 4 pages
        kwargs = {"max_id": args.max_id, "since_id": since_id, "max_pages": 4}
        things = t.timeline(**kwargs)

        line_count = 0
        for thing in things:
            line_count += 1

            # ready to output

            kind_of = type(thing)
            if kind_of == str:
                # user or tweet IDs
                print(thing, file=fh)
                log.info("archived %s" % thing)
            elif "id_str" in thing:
                # tweets and users
                print(json.dumps(thing, ensure_ascii=False), file=fh)
                log.info("archived %s", thing["id_str"])
                try:
                    tweet = Tweet(thing)
                    surt_counter.update(map(surt, tweet.most_unrolled_urls))

                    # set since_id to the first tweet id for next crawl
                    if line_count == 1:
                        since_id = thing["id_str"]
                except (json.JSONDecodeError, NotATweetError):
                    pass
            elif "woeid" in thing:
                # places
                print(json.dumps(thing, ensure_ascii=False), file=fh)
            elif "tweet_volume" in thing:
                # trends
                print(json.dumps(thing, ensure_ascii=False), file=fh)
            elif "limit" in thing:
                # rate limits
                t = datetime.datetime.utcfromtimestamp(
                    float(thing["limit"]["timestamp_ms"]) / 1000
                )
                t = t.isoformat("T") + "Z"
                log.warning("%s tweets undelivered at %s", thing["limit"]["track"], t)
                if args.warnings:
                    print(json.dumps(thing, ensure_ascii=False), file=fh)
            elif "warning" in thing:
                # other warnings
                log.warning(thing["warning"]["message"])
                if args.warnings:
                    print(json.dumps(thing, ensure_ascii=False), file=fh)

            if line_count:
                n = 3.907  # wait at least 2 ** 3.907 = 15s
        else:  # things is empty (exponential backoff)
            interruptible_sleep(
                min((2 ** n) + (random.randint(0, 1000) / 1000), 900)
            )  # max 15 minutes
            n = n + 1


def get_argparser():
    """
    Get the command line argument parser.
    """

    parser = argparse.ArgumentParser("loopy")
    parser.add_argument("--log", dest="log", default="loopy.log", help="log file")
    parser.add_argument(
        "--warnings", action="store_true", help="Include warning messages in output"
    )
    parser.add_argument(
        "--connection_errors",
        type=int,
        default="0",
        help="Number of connection errors before giving up",
    )
    parser.add_argument(
        "--http_errors",
        type=int,
        default="0",
        help="Number of http errors before giving up",
    )
    parser.add_argument(
        "--max_id", dest="max_id", help="maximum tweet id to search for"
    )
    parser.add_argument("--since_id", dest="since_id", help="smallest id to search for")
    parser.add_argument(
        "--lang",
        dest="lang",
        action="append",
        default=[],
        help="limit to ISO 639-1 language code",
    ),
    parser.add_argument(
        "--tweet_mode",
        action="store",
        default="extended",
        dest="tweet_mode",
        choices=["compat", "extended"],
        help="set tweet mode",
    )
    parser.add_argument(
        "--protected",
        dest="protected",
        action="store_true",
        help="include protected tweets",
    )
    parser.add_argument(
        "--output",
        action="store",
        default=None,
        dest="output",
        help="write output to file path",
    )
    parser.add_argument(
        "--skip_key_validation",
        action="store_true",
        help="skip checking keys are valid on startup",
    )

    return parser


if __name__ == "__main__":
    main()
