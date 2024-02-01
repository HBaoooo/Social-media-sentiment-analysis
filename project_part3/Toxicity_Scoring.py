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

#   We're going to explore how we can use these objects. The source code for them
#   is found in the mele/perspective folder of the mele user module
#
from mele.perspective.BaseRequestData import BaseRequestData
from mele.perspective.ENRequestData import ENRequestData
from mele.perspective.PerspectiveAPI import PerspectiveAPI


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

PERSPECTIVE_DOC_TYPE = "perspective_scores"
ANALYSIS_DOCUMENT = {
    'doc_type'    : PERSPECTIVE_DOC_TYPE,  # a Perspective API score doc
    'created_at'  : "",                    # timestamp for this API score
    'screen_name' : "",                    # twitter screen name of the tweet poster
    'id'          : 0,                     # the id of the tweet that was scored
    'id_str'      : "",                    # the id of the tweet that was scored
    'analysis'    : None                   # a subdocument, a Perspective score dict
}

print("Opening the Mongo DB connection")
config = Configuration(DATABASE_SETTINGS["HCDE530"])
collection = DB(config=config)
c=collection.getCollection("twit2")

API_KEY = "AIzaSyAuLwIjQxWpJUW1SDSxi-mvgPRcX5r0Osc"
perspective = PerspectiveAPI()
perspective.setRequestKey(API_KEY)

def insertOne(self, doc=None, bypassValidation=False):
    result = dict()
    result['acknowledged'] = False
    result['inserted_id'] = None
    ins = None

    try:
        ins = self.collection.insert_one(document=doc, bypass_document_validation=bypassValidation)
    except Exception as ex:
        ins = None
        self.debug("Exception during insertOne() operation:")
        self.debug(str(ex))
        raise
        
    if( ins and ins.acknowledged ):
        result['acknowledged'] = ins.acknowledged
        result['inserted_id'] = ins.inserted_id
    else:
        self.debug("insertOne() operation was not completed by server.")
    return result


def get_random_tweets(screen_name):
    tweets = list(c.find({'screen_name': screen_name}))
    #print(tweets)
    tweets_cnt = len(tweets)
    print(tweets_cnt)
    if tweets_cnt > 1200:
        return random.sample(tweets, 1200)
    else:
        return tweets



#
# Main
#
def main():
    #members = list(c.find({}, {'screen_name':1}))
    members = list(c.distinct('screen_name'))
    print(members)
    cnt = 0
    for member in members:
        #member_id = member['screen_name']
        print(f"Scoring tweets for ({member})...")
        tweets = get_random_tweets(member)
        for tweet in tweets:
            if 'full_text' not in tweet:
                continue
            tweet_id = tweet['id']
            tweet_text = tweet['full_text']
            print(tweet_text)
            try:
                request_pl = ENRequestData()
                request_pl.setComment(text=tweet_text)
                request_pl.addScoreAttribute("TOXICITY")
                request_pl.addScoreAttribute("INSULT")
                request_pl.addScoreAttribute("UNSUBSTANTIAL")
                perspective.setRequestPayload(request_pl)
                perspective.makeRequest()
                score = perspective.getMessage()
                analysis_result = json.dumps(score,indent=4,sort_keys=True)
                print(analysis_result)

                doc = ANALYSIS_DOCUMENT.copy()
                doc['doc_type'] = PERSPECTIVE_DOC_TYPE
                doc['created_at'] = time.time()
                doc['screen_name'] = tweet['screen_name']
                doc['id'] = tweet_id
                doc['id_str'] = tweet['id_str']
                doc['analysis'] = analysis_result
                #print(doc)
                try:
                    t = collection.getCollection("toxicity")
                    t.insertOne(doc)
                    cnt = cnt+1
                    print("num of twit scored:",cnt)
                except:
                    print("error getting the connection")
            except:
                print("Error scoring twit")


if __name__ == '__main__':
    main()   
