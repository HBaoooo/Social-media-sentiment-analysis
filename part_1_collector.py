#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 
#   FILE: part_1_collector.py
#   REVISION: 
#   CREATION DATE: 
#   Author: Haocheng Bao
#

#####
#
#   Import Statements
#
import sys, json, csv, time, datetime
from mele.classes.utilities.Args import Args
from mele.twit.support import *
from mele.twit.Authorizer import Authorizer
from mele.twit.UserTimeline import UserTimeline
from mele.classes.db.mongo.Configuration import Configuration
from mele.classes.db.mongo.DB import DB
from mele.classes.db.mongo.Collection import Collection

#####
#
#   CONSTANTS
#
#
TWITTER_APP = "resolve_2021"
TWITTER_USER = "<hvb5223>"
COLLECTION_NAME = "<twit>"
CHAMBER_HOUSE = "house"
CHAMBER_SENATE = "senate"
# 
#CONGRESSIONAL_CHAMBER = "<undefined>"
CONGRESSIONAL_CHAMBER = CHAMBER_SENATE
#CONGRESSIONAL_CHAMBER = CHAMBER_HOUSE


#
#   There are congress members who do not have an official twitter account. These have 'no_official'
#   in the field for the user account (handle). They should be skipped for now. The main() for loop
#   has an if conditional check and skips people with this as the twitter handle
#
MISSING_ACCOUNT = "no_official"


#
#   The collection loop should pause or sleep, just a little, between each collection cycle.
#   This is just to be considerate of the remote servers and the free data they are providing.
#   This also makes sure you do not exceed the rate limits for the UserTimeline() request
#
TIMELINE_SLEEP_DURATION = 1.0
LOOP_SLEEP_DURATION = 5.0
MAX_TIMELINE_COUNT = 200
MAX_TIMELINE_REQUESTS = 25

#MAX_TIMELINE_REQUESTS = 3


#
#   Database settings
#
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


#####
#
#   MONGO DB - DOCUMENT SCHEMAS
#
#   We need to structure the documents in our collection. If we just dump everything in, then
#   we could easily lose track of what is what. For now, let's track three kinds of things:
#
#   1. User information     - a USER_DOC_TYPE stores information about the users we've collected
#   2. Tweet information    - a TWEET_DOC_TYPE stores information about each tweet
#   3. Effort information   - an EFFORT_DOC_TYPE stores information about when we made a collection
#                             effort or attempt - for each person/user
#
#   Having a good schema for structuring the documents in your DB is important so you can keep
#   track of what you have and what you might need. This will also facilitate many analyses you
#   might want to do based on the data.
#
USER_DOC_TYPE = "user_data"
TWEET_DOC_TYPE = "tweet_data"
EFFORT_DOC_TYPE = "effort_info"

#
#   We distill down just a few of the fields that tell us about the Twitter user.
#   The majority of these fields can be extracted from a tweet, any tweet, made by the
#   user. The last five fields come from the CSV data for that congress person.
#
TWEET_USER_DATA = {
    'doc_type'          : USER_DOC_TYPE,
    'collection_tag'    : "",
    'created_at'        : "",
    'description'       : "",
    'favourites_count'  : 0,
    'followers_count'   : 0,
    'friends_count'     : 0,
    'geo_enabled'       : False,
    'id'                : 0,
    'id_str'            : "",
    'location'          : "",
    'name'              : "",
    'screen_name'       : "",
    'statuses_count'    : 0,
    'time_zone'         : None,
    'url'               : "",
    'verified'          : False,
    'party'             : "",
    'state'             : "",
    'first_name'        : "",
    'last_name'         : "",
    'legislative_body'  : "senate"
}


#
#   We are not keeping the complete JSON/dictionary information with every tweet. We keep just
#   a few things that might help in possible analyses of the tweets.
#
#   Tweets are reformatted from the UserTimeline() response into this format by making a copy
#   of the template and copying over the specific fields.
#
TWEET_TWEET_DATA = {
    'doc_type'      : TWEET_DOC_TYPE,
    'collection_tag': "",
    'created_at'    : "",
    'full_text'     : "",
    'geo'           : None,
    'id'            : 0,
    'id_str'        : "",
    'lang'          : "",
    'place'         : None,
    'source'        : "",
    'truncated'     : False,
    'screen_name'   : None
}


#
#   This represents a collection effort for one person - their timeline at a specific point 
#   in time. We keep a timestamp of when the data was collected - but also we have a 
#   "collection_tag" that allows us to name each collection effort. For this class we'll 
#   leave the "collection_tag" empty. The set of these collection records represents some key 
#   meta data about Who, what, and when of a collection effort, 
#
EFFORT_RECORD = {
    'doc_type'      : EFFORT_DOC_TYPE,
    'collection_tag': "",
    'created_at'    : "",
    'screen_name'   : None,
    'count'         : 0
}




#####
#
#   PROCEDURES/FUNCTIONS
#


#####
#
#
def load_text_csv(filename=None):
    congress_members = list()
    #
    #   The list of senators or house members is in a CSV (comma separated values) file. 
    #
    #   You need to:
    #       open the CSV file
    #       read each CSV row 
    #       add the resulting information to the list of congress members
    #       close the file
    #       return the congress members
    #   
    #
    with open(filename, 'r') as csvfile:
        readcsv = csv.DictReader(csvfile)
        for row in readcsv:
            congress = {
                'First_Name': row['First_Name'],
                'Last_Name': row['Last_Name'],
                'Twitter_Handle': row['Twitter_Handle'],
                'Party': row['Party']
            }
            congress_members.append(congress)
    return congress_members # this line should return a list of dictionaries, 



#####
#
#   This function should try and solve Step 3.
#
#
#   The parameter "requester" is a UserTimeline object that should already be authorized and ready
#   to make requests.
#
#   The parameter "max_id" is the tweet ID of the oldest tweet in the prior request, this allows
#   you to page backward in time, through the users tweets
#
def make_timeline_request(requester=None, max_id=0):
    tweets = list()
    #
    #   You need to:
    #       set the max_id of the request 
    #       make the request
    #       get the response
    #       return the response
    #
    try:
        requester.setMaxID(max_id)
        requester.makeRequest()
        response = requester.getMessage()
        for tweet in response:
            tweets.append(tweet)
    except Exception as e:
        print("Error getting usertimeline: {}".format(str(e)))
    return tweets # this line should return a list of tweets, dictionaries, 



#####
#
#   This function is a little complex it is provided for FREE to handle the somewhat complex
#   paging through tweet IDs when collecting a user timeline. You should carefully read through
#   this function to make sure you understand most of what is happening.
#
#
#   The parameter "requester" is a UserTimeline object that should already be authorized and ready
#   to make requests.
#
#   The parameter "user" is the twitter username or handle of the user being requested
#
#   You should have a working - and tested - make_timeline_request() function completed in
#   Step 3. If that's not working - this won't work either.
#
#   This function is called from the main() for-loop
#
#
def collect_user_timeline(requester=None, user=None):
    #
    #   initialize a return variable, this list will hold all of the tweets we collect
    tweets = list()
    #
    #   Before we do anything with the UserTimeline object, let's make sure we have one
    #
    if not requester:
        print("ERROR: in collect_user_timeline(), the UserTimeline object was empty.")
        return tweets
    #
    #   A little message to let us know we're getting started with the request
    print(f"\tRequesting timeline for: {user}")
    requester.setUsername(user)
    #
    #   Variables to keep track of the pages - beginning and ending - for each of the pages
    #   that we request. We need to know the end of each page to make the next request as
    #   we move through the timeline. The Twitter API does not do that automatically, we
    #   have to do it with our program.
    #
    repeat_response = 1
    request_count = 1
    first_id_str = ""
    first_created = ""
    cur_last_id_str = ""
    last_created = ""
    prior_last_id_str = ""
    #
    #   This try block is to protect the timeline collection loop
    try:
        #
        #   The very first call is without a max_id because we're just starting a timeline
        #
        response = make_timeline_request(requester)
    except:
        print("EXCEPTION in function 'make_timeline_request()'")
        raise()
    
    #
    #   Now, while there is some data in a response, we're going to loop
    #   We will stop when there is no more data returned from a request, or if we max out our
    #   number of requests
    #
    while response and (request_count < MAX_TIMELINE_REQUESTS):
        #
        # make sure we have a *different* response from the last response
        #
        if (first_id_str == response[0]['id_str']) and (prior_last_id_str == response[-1]['id_str']):
            print(f"WARNING: The data in this response is the same as the prior response!")
            print(f"WARNING: Exiting collect_user_timeline() with currently collected tweets.")
            time.sleep(TIMELINE_SLEEP_DURATION)
            repeat_response = repeat_response + 1
            response = list()
            continue
        
        #
        # get creation date and tweet ID from the first - newest - tweet in the response list
        #
        first_id_str = response[0]['id_str']
        first_created = response[0]['created_at']
        #
        # get this data from the last - oldest - tweet in the list
        #
        cur_last_id_str = response[-1]['id_str']
        last_created = response[-1]['created_at']
        #
        # some debugging that can be commented out after testing
        #print(f"{first_created =} {first_id_str =}")
        #print(f"{last_created =} {cur_last_id_str =}")
        #
        # the way we have paging working the last tweet of the prior responnse *should* be
        # the very *first* tweet in the next response - we don't want to collect that tweet
        # twice, so we to trim, remove, that one tweet from the start of the response list
        #
        if first_id_str == prior_last_id_str:
            response = response[1:]
        #
        #
        # we only need to update the list if there is something in the response after we remove
        # the duplicate tweet. Also, note if the response length is zero, then our loop will stop
        #
        if len(response) > 0:
            #
            # Now add the current response onto our growing list of tweets
            #
            tweets.extend(response)
            #
            # Output just a little bit of status
            #
            print(f"\t\t{request_count:2}: Retrieved {len(response)} NEW tweets for {user}, have {len(tweets)} total tweets.")
            #
            # Update the 'prior_last_id_str' variable to represent what it is now (cur_last_id_str),
            # so that the NEXT time through the loop our tests will make sense, next time through
            # the loop it is/will be the prior one
            #
            prior_last_id_str = cur_last_id_str
            cur_last_id_str = ""
            #
            #   This try block is to protect the timeline collection loop
            try:
                #
                #   Now that we know what the last id_str was - we can use that to tell the
                #   requester object to only get the stuff older than that last id_str
                #
                response = make_timeline_request(requester, prior_last_id_str)
            except:
                print("EXCEPTION in function 'make_timeline_request()'")
                raise()
            # this causes the computer to wait just a few seconds - we're using
            # sleep to make sure we don't overrun the rate limits for these requests
            time.sleep(TIMELINE_SLEEP_DURATION)
            request_count = request_count + 1

    return tweets # this line should return a list of tweets we collected





#####
#
#   This procedure should try and solve Step 5.
#
#   This procedure should extract the user data and create a new TWEET_USER_DATA dictionary as
#   its result. The resulting TWEET_USER_DATA dictionary will be what is eventually inserted into
#   a Mongo DB collection.
#   
#   The parameter tweet is just one single tweet from the list of user timeline tweets
#
#   The parameter csv_rec is the dictionary record for this user from the CSV file
#
def extract_user_data(tweet=None,csv_rec=None):
    user = TWEET_USER_DATA.copy()
    user['doc_type'] = USER_DOC_TYPE
    user['created_at'] = tweet['user']['created_at']
    user['description'] = tweet['user']['description']
    user['favourites_count'] = tweet['user']['favourites_count']
    user['followers_count'] = tweet['user']['followers_count']
    user['friends_count'] = tweet['user']['friends_count']
    user['geo_enabled'] = tweet['user']['geo_enabled']
    user['id'] = tweet['user']['id']
    user['id_str'] = tweet['user']['id_str']
    user['location'] = tweet['user']['location']
    user['name'] = tweet['user']['name']
    user['screen_name'] = tweet['user']['screen_name']
    user['statuses_count'] = tweet['user']['statuses_count']
    user['time_zone'] = tweet['user']['time_zone']
    user['url'] = tweet['user']['url']
    user['verified'] = tweet['user']['verified']
    user['party'] = csv_rec['Party']
    user['first_name'] = csv_rec['First_Name']
    user['last_name'] = csv_rec['Last_Name']
    return user





#####
#
#   This procedure should try and solve Step 6.
#
#   This procedure performs the insert on the list of reformatted tweets
#   
#   The parameter collection is the collection object that you got from the Mongo DB - it's
#   the python object that represents the collection you are using to store your tweets
#
#   The parameter reformatted_tweets is the list of tweets you created
#
def reformat_tweets(tweet_list=None):
    new_list = list()
    for tweet in tweet_list:
        format = TWEET_TWEET_DATA.copy()
        format['doc_type'] = TWEET_DOC_TYPE
        #format['collection_tag'] = tweet['collection_tag']
        format['created_at'] = tweet['created_at']
        format['full_text'] = tweet['full_text']
        format['id'] = tweet['id']
        format['id_str'] = tweet['id_str']
        format['lang'] = tweet['lang']
        format['geo'] = tweet['geo']
        format['source'] = tweet['source']
        format['truncated'] = tweet['truncated']
        format['screen_name'] = tweet['user']['screen_name']
        new_list.append(format)
    return new_list
    



#####
#
#   This procedure should try and solve Step 8.
#
#   This procedure performs the insert on the list of reformatted tweets
#   
#   The parameter collection is the collection object that you got from the Mongo DB - it's
#   the python object that represents the collection you are using to store your tweets
#
#   The parameter reformatted_tweets is the list of tweets you created
#
def insert_tweets(collection=None, reformatted_tweets=None):
    for tweet in reformatted_tweets:
        collection.insertOne(tweet)
    return
    




#####
#
#   This procedure should try and solve Step 10.
#
#   This procedure generates a table that lists the users collected
#   
#   The parameter effort_list is the list of effort dictionaries
#
#
def print_effort_summary(effort_list=None):
    print("Summary of Data Collection Effort")
    if not effort_list:
        print("'effort_list' was empty")
        return
    #   your loop could go here
    print(f"| {'Twitter Username':20} | {'Count':>15} | {'Timestamp':30} |")
    for effort in effort_list:
        handle = effort['screen_name']
        count = effort['count']
        created_at = effort['created_at']
        print(f"| {handle:20} | {count:>15} | {created_at:30} |")
    #print()
    return
    




#####
#
#   A template for the parameters that we are going to collect from the command line
#   
#   We need a CSV file of congress persons, one person per row. We'll use the -members parameter
#   key to indicate a members file is next. The '-debug' key is kept in case we need it for
#   turning on some debuggng. Right now, this template has no real debugging in it.
#
PARAMETER_KEYS = {
    "-members" : 
        {   "required"  : True,         # wether or not this flag is required
            "single"    : False,        # is this just a single item (True), or is there additonal (False)
            "ptype"	    : str,          # the value type, used to convert to what we need
            "notes"     : "<csv_file>", # a note to describe what the key expects to get
            "value"     : ""            # a possible default value
            },

    # an optional flag - not being used right now, but I always keep this in the KEYS case I need it
    "-debug" : 
        {   "required"  : False,        # wether or not this flag is required
            "single"    : True,         # a single value flag (True)
            "ptype"	    : bool,         # this is a boolean flag, it's either there or not
            "notes"     : "",           # 
            "value"     : False         # the default value
            }
    }
    


#####
#
#   The main() function (or procedure)
#
#   
#
def main(argv):
    #
    #   Get the command line arguments - This is sort of a freebie. The 'params' variable behaves 
    #   like a dictionary. It should have two values params['members'] and params['debug'] if you 
    #   keep the PARAMETER_KEYS defined above
    #
    print("Parsing the command line parameters")
    params = Args(name="tweet_collector_template",flags=PARAMETER_KEYS)
    params.parse(argv)
    
    #
    #   Here you need to load the csv file of the congress people
    #
    print(f"Loading the CSV file: '{params['members']}'")
    congress = load_text_csv(params['members'])
        
    #
    #   Now set up the authorization object
    #
    print("Initializaing the authorization object")
    #
    #   The creation of the Authorizer() object and the initialization is like the examples
    #

    #   YOU NEED SOME CODE HERE TO INSTANTIATE AN Authorization() object to use with a UserTimeline()
    app_keys = TWITTER_APP_OAUTH_PAIR(app=TWITTER_APP)
    auth = Authorizer(name="auth for UserTimeline", app_name=TWITTER_APP, app_user=TWITTER_USER)
    auth.setConsumerKey(app_keys['consumer_key'])
    auth.setConsumerSecret(app_keys['consumer_secret'])
    auth.authorize()
    #
    #   Now set up a UserTimeline object
    #
    print("Initializing the UserTimeline() object")
    #
    #   Creating and setting up a UserTimeline object is like the examples you've seen
    #

    #
    #   YOU NEED SOME CODE HERE TO INSTANTIATE A UserTimeline() object
    requester = UserTimeline()    
    requester.setAuthObject(obj=auth)
    requester.setThrottling(True)        
    requester.setCount(100)  
    #
    #   Now you want to connect to the DB and get the collection you need
    #
    print("Opening the Mongo DB connection")
    #
    #   Connecting to the DB is almost the same as the examples you've seen
    #

    #   YOU NEED SOME CODE HERE TO INSTANTIATE A MONGO DB() object
    #   And get a collection to use for inserting new data
    config = Configuration(DATABASE_SETTINGS["HCDE530"])
    collection = DB(config=config)
    c=collection.getCollection("twit2")

    #
    #   These are to help track what is happening in the main for-loop
    #
    skipped_list = list()   # this keeps track of congress members that have no tweets
    effort_list = list()    # to store all of the effort records, for later summary
    user_count = 1          # to keep track of how many twitter users we've collected
    #
    #   Now we start collecting each congressional member, one member at a time
    #   using the dictionary records from the CSV file
    #
    for congressional_member in congress:
        #
        #   Check to see if this congress member has an official twitter account
        #
        if congressional_member['Twitter_Handle'] == MISSING_ACCOUNT:
            # note the reason
            congressional_member['reason'] = "no official twitter account"
            skipped_list.append(congressional_member)
            print(f"{user_count:3}: {congressional_member['First_Name']} {congressional_member['Last_Name']} has no official twitter account, skipping.")
            user_count = user_count+1
            print(f"Sleeping for {LOOP_SLEEP_DURATION} seconds.")
            time.sleep(LOOP_SLEEP_DURATION)
            continue
        
        #
        #   Get the twitter handle and trim off the preceeding '@' sign
        #
        handle = congressional_member['Twitter_Handle'][1:]
        print(f"{user_count:3}: Starting processing loop for user: {handle}")
        
        #
        #   Now collect the whole user timeline
        #
        tweets = collect_user_timeline(requester,handle)
        #
        #   If there were no tweets collected then we add this to the "skipped" list
        #
        if not tweets:
            # note the reason
            congressional_member['reason'] = "there were no tweets returned from 'collect_user_timeline()'"
            skipped_list.append(congressional_member)
            print(f"{user_count:3}: requests for user '{handle}' returned no tweets on the timeline, skipping.")
            user_count = user_count+1
            print(f"Sleeping for {LOOP_SLEEP_DURATION} seconds.")
            time.sleep(LOOP_SLEEP_DURATION)
            continue
        
        #
        #   Extract the user data to create a tweet_user document (dictionary)
        #
        tweet_user = extract_user_data(tweet=tweets[0],csv_rec=congressional_member)
        #   Make sure the correct legislative body is set
        tweet_user['legislative_body'] = CONGRESSIONAL_CHAMBER
        #print(json.dumps(tweet_user,indent=4))
        
        #
        #   Now we reformat all of the tweets
        #
        new_tweets = reformat_tweets(tweet_list=tweets)
        #print(json.dumps(new_tweets[0],indent=4))
        
        #
        #   This creates a string timestamp to record when we did this collection
        #   this is used as the 'created_at' timestamp in the EFFORT_RECORD
        timestamp = str(datetime.datetime.now()).partition('.')[0]
        #
        #   Step 7 - This is FREE code
        #   Create a dictionary to summarize the collection effort for this congress member
        #   
        effort = EFFORT_RECORD.copy()
        effort['created_at'] = timestamp
        effort['screen_name'] = handle
        effort['count'] = len(new_tweets)
        effort_list.append(effort)
        
        #
        #   If there are tweets, insert them into the MongoDB
        #
        if len(new_tweets) > 0:
            #
            #   YOU NEED TO ADD CODE TO DO THE INSERTIONS
            #
            #   - insert the tweet_user
            #   - insert the new_tweets
            #   - insert the effort
            #
            c.insertOne(tweet_user)
            #c.insertOne(effort_list)
            insert_tweets(c, new_tweets)
        else:
            #   No tweets, no insertions, add user to the skipped list with a reason
            #
            congressional_member['reason'] = "the list of reformmated tweets was empty, nothing to insert into the DB"
            skipped_list.append(congressional_member)
            print(f"WARNING: There were no reformatted tweets to insert for user '{handle}'.")
            
        #
        # this code causes the computer to wait just a few seconds - we're using
        # sleep to make sure we don't overrun the rate limits for these requests
        #
        print(f"Sleeping for {LOOP_SLEEP_DURATION} seconds.")
        time.sleep(LOOP_SLEEP_DURATION)
        user_count = user_count+1
        
    #
    #   Close the database
    #
    print("Closing the Mongo DB connection")
    #   A statement like this is important to closing the DB when you're done
    #db.close()
    #
    #   Now print a well formatted summary of this data collection
    #
    print_effort_summary(effort_list)
    #
    #   If we had to skip a person, print that out in case we can do a repair
    #
    if len(skipped_list) > 0:
        print("The following congress members time lines were skipped.")
        print(json.dumps(skipped_list,indent=4))
    #
    return


#
#   This is required to get the command line arguments into the main() procedure
#
if __name__ == '__main__':
    main(sys.argv)   

