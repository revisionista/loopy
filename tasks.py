import json
import os
from collections import Counter

from dotenv import find_dotenv
from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session
from surt import surt
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError

load_dotenv(find_dotenv())

# Add your API key here
consumer_key = os.getenv("TWITTER_CONSUMER_KEY")

# Add your API secret key here
consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")

access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# Make the request
oauth = OAuth1Session(
    client_key=consumer_key,
    client_secret=consumer_secret,
    resource_owner_key=access_token,
    resource_owner_secret=access_token_secret,
)

params = {
    "count": 200,
    "exclude_replies": "true",
    "include_entities": "true",
    "include_ext_alt_text": "true",
}

response = oauth.get(
    "https://api.twitter.com/1.1/statuses/home_timeline.json", params=params
)
# response.raise_for_status()

# Turn response into JSON
if response.encoding is None:
    response.encoding = "utf-8"
for data in response.iter_lines(decode_unicode=True):
    if data:
        jdata = json.loads(data)

counter = Counter()
for tweet_dict in jdata:
    try:
        tweet = Tweet(tweet_dict)
    except (json.JSONDecodeError, NotATweetError):
        pass
    print(tweet.created_at_string, tweet.id, tweet.text)
    print("  - ", tweet.most_unrolled_urls)
    for url in tweet.most_unrolled_urls:
        o = surt(url)
        counter.update([o])

for url, count in counter.most_common(n=5):
    print(url, count)
