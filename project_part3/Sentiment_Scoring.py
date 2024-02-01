import sys, json, csv, time, datetime, string, random, nltk
import collections
#
#   import the command line argument parsing object
#
from mele.classes.utilities.Args import Args
#
#   import the objects needed to access user timelines from twit
#
from mele.twit.support import *
from mele.twit.Authorizer import Authorizer
from mele.twit.UserTimeline import UserTimeline
from mele.classes.utilities.StopWords import StopWords
#
#   Here YOU need to add the import statements to access Mongo DB 
#
from mele.classes.db.mongo.Configuration import Configuration
from mele.classes.db.mongo.DB import DB
from mele.classes.db.mongo.Cursor import Cursor
from mele.classes.db.mongo.Collection import Collection

from pymongo import collection
from nltk.sentiment.vader import SentimentIntensityAnalyzer

DATABASE_SETTINGS = {
    'HCDE530': {
        'database_name':"HCDE530",
        'database_protocol':"mongodb",
        'database_driver':"pymongo",
        'database_host':"localhost",
        'database_port':"27017",
        'database_user':"codeaccess",
        'database_pass':"tweetG00dness"
    }
}

SENTIMENT_DOC_TYPE = "vader_scores"
ANALYSIS_DOCUMENT = {
    'doc_type'    : SENTIMENT_DOC_TYPE,    # a VADER score doc
    'created_at'  : "",                    # timestamp for this API score
    'screen_name' : "",                    # twitter screen name of the tweet poster
    'id'          : 0,                     # the id of the tweet that was scored
    'id_str'      : "",                    # the id of the tweet that was scored
    'analysis'    : None                   # a subdocument, a VADER score dict
}

print("Opening the Mongo DB connection")
config = Configuration(DATABASE_SETTINGS["HCDE530"])
collection = DB(config=config)
c=collection.getCollection("twit2")


def score_tweets(tweet_list=None, vader=None, pos_thresh=0.10, neg_thresh=-0.10):
    result = list()
    # loop through all of the tweets in the list
    for t in tweet_list:
        # create & initialize a dictionary to store our results
        score = dict()
        score['full_text'] = t['full_text']
        score['id_str'] = t['id_str']
        score['raw_scores'] = None
        score['polarity_str'] = None
        score['polarity_int'] = None
        
        # use vader to score the tweet text
        raw = vader.polarity_scores(score['full_text'])
        score['raw_scores'] = raw
        
        if( raw['compound'] >= pos_thresh ):
            score['polarity_str'] = "positive_sentiment"
            score['polarity_int'] = 1
        elif( raw['compound'] <= neg_thresh ):
            score['polarity_str'] = "negative_sentiment"
            score['polarity_int'] = -1
        else:
            score['polarity_str'] = "neutral_sentiment"
            score['polarity_int'] = 0
        
        result.append(score)
    return result

#
# Main
#
def main():
    vader = SentimentIntensityAnalyzer()
    t=collection.getCollection("toxicity")
    tweets = list(t.find())
    #tweet_lists = list()
    cnt = 0
    for tweet in tweets:
        tweet_id = tweet['id']
        #print(tweet_id)
        screen_name = tweet['screen_name']
        id_str = tweet['id_str']

    #print(tweet_list)
        #print(f"finding tweets for ({screen_name})...")
        tweet_list = list(c.find({'id': tweet_id}))
        print(tweet_list)
        #tweet_lists.append(tweet_list)
    #scores = score_tweets(tweet_list, vader)
        scores = score_tweets(tweet_list, vader)
        for score in scores:
            doc = ANALYSIS_DOCUMENT.copy()
            doc['doc_type'] = SENTIMENT_DOC_TYPE
            doc['created_at'] = time.time()
            doc['screen_name'] = screen_name
            doc['id'] = tweet_id
            doc['id_str'] = id_str
            doc['analysis'] = score
            try:
                s = collection.getCollection("sentiment")
                s.insertOne(doc)
                cnt = cnt+1
                print("num of twit scored:",cnt)
            except:
                print("error getting the connection")


if __name__ == '__main__':
    main()   