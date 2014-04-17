#!/usr/bin/python
import requests
from requests_oauthlib import OAuth1Session
import json
from collections import defaultdict
import re
import sys
import sqlite3 as lite

## self import
from secrets import *

'''
A script to collect tweets data using Twitter streaming API
usage: python script.py amount_of_tweets_needed
'''

## helper functions
def cleanSource(source):
    if source == 'web':
        return source
    else:
        match = re.search(r'^<.+?>(.+?)<\/a>$', source)
        if match:
            return match.group(1)
        else:
            return source

# urls and parameters used to query Twitter API
STREAM_URL = 'https://stream.twitter.com/1.1/statuses/sample.json'
STREAM_KEYS = {'language':'en', 'filter_level':'medium'}

# http request object
oauth = OAuth1Session(APP_KEY, client_secret=APP_SECRET,
                          resource_owner_key=ACCESS_TOKEN,
                          resource_owner_secret=ACCESS_TOKEN_SECRET)
tstream = oauth.get(STREAM_URL, stream=True, params=STREAM_KEYS)

count = 0
with lite.connect("tweets.db") as conn:
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS cleaned (id INTEGER PRIMARY KEY, screen_name TEXT NOT NULL, content TEXT NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL, coordinates INTEGER NOT NULL, urls INTEGER NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS spam (id INTEGER PRIMARY KEY, screen_name TEXT NOT NULL, content TEXT NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL, coordinates INTEGER NOT NULL, urls INTEGER NOT NULL);")
    for line in tstream.iter_lines():
        if line:
            tweet = json.loads(line)
            text = tweet.get(u'text', False)
            source = tweet.get(u'source', False)
            # ignore the tweet if either source or text does not exist
            if source and text:
                user = tweet.get(u'user', False)
                if user:
                    dtweet = []
                    dtweet.append(user.get(u'screen_name', ''))
                    dtweet.append(text)
                    dtweet.append(cleanSource(source))
                    dtweet.append(tweet.get(u'created_at', ''))
                    # see if the tweet has location info
                    if tweet.get(u'coordinates', False):
                        dtweet.append(1)
                    else:
                        dtweet.append(0)
                    # see if the tweet has links
                    if tweet.get(u'entities', {}).get(u'urls', False):
                        dtweet.append(1)
                    else:
                        dtweet.append(0)
                    #dtweet.append(tweet.get(u'possibly_sensitive', False))
                    # naive logic for bot/spam/retweet detection
                    if tweet.get(u'possibly_sensitive', False) or \
                        user.get(u'friends_count', 0) < 10 or \
                        user.get(u'followers_count', 0) < 10 or \
                        tweet.get(u'retweeted_status', False) or \
                        'RT' in tweet.get(u'text', False):
                        cursor.execute("INSERT INTO spam (screen_name,content,source,created_at,coordinates,urls) VALUES (?,?,?,?,?,?)", dtweet)
                    else:
                        cursor.execute("INSERT INTO cleaned (screen_name,content,source,created_at,coordinates,urls) VALUES (?,?,?,?,?,?)", dtweet)
            count += 1
        # periodic log
        if count%500 == 0:
            print "%d tweets inserted..." % count
        if count == int(sys.argv[1]):
            break
