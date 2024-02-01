import sys, json, csv, time, datetime, string, random, nltk
import collections
from prettytable import PrettyTable 
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
#
#   Your database settings as a python dictionary go here  You could probably copy and paste 
#   from an example you saw in class. You may need to change some values in some fields.
#
}

#
# Step 1 - Query a Mongo collection for users
#
def query_user_data(collection, party=None, legislative_body=None):
    query = {}
    if party:
        query['party'] = party
    if legislative_body:
        query['legislative_body'] = legislative_body

    cursor = Cursor(cursorable=collection.find(query))
    results = list(cursor)

    return results

#
# Step 2 - Term frequency calculation
#

def count_frequency(collection, term_freq):
    sw = StopWords()
    trans = str.maketrans('', '', string.punctuation)
    tokens = collection.lower().translate(trans).split()
    for word in tokens:
        if word and word not in sw:
            term_freq[word] += 1
    return term_freq


#
# Step 3 - Self-Presentation analysis
#
def description_freq(tweets):
    term_freq = collections.Counter()
    for tweet in tweets:
        description = tweet["description"]
        count_frequency(description, term_freq)
    return term_freq

#
# Step 4 - Top-n terms
#
def get_top_terms(term_freq, number):
    sort = sorted(term_freq.items(), key=lambda x: x[1], reverse=True)
    top_terms = dict(sort[:number])
    return top_terms

#
#Step 5 - Self-Presentation comparison
#
def print_table(term_freq):
    for term, count in term_freq.items():
        print(f"{term:20} {count:>15}")

def self_comparison(collection):
    senate_tweets = query_user_data(collection, legislative_body="senate")
    democrats_tweets = query_user_data(collection, party="D", legislative_body="senate")
    republicans_tweets = query_user_data(collection, party="R", legislative_body="senate")
    
    senate_term_freq = description_freq(senate_tweets)
    d_term_freq = description_freq(democrats_tweets)
    r_term_freq = description_freq(republicans_tweets)
    
    senate_top_terms = get_top_terms(senate_term_freq, 27)
    d_top_terms = get_top_terms(d_term_freq, 27)
    r_top_terms = get_top_terms(r_term_freq, 27)
    
    print("Whole Senate:")
    print_table(senate_top_terms)
    print(f"{'-'*30:<30}")
    
    print("Senate Democrats:")
    print_table(d_top_terms)
    print(f"{'-'*30:<30}")
    
    print("Senate Republicans:")
    print_table(r_top_terms)


#
# Step 6 - Generate tweet sample
#

def get_sample_tweets(collection, party, legislative_body):
    user_data = query_user_data(collection, party, legislative_body)
    user_ids = [user['id'] for user in user_data]
    tweet = []
    for user_id in user_ids:
        user_tweets = list(collection.find({'user.id': user_id}, {'full_text': 1}))
        #print(user_tweets)
        user_tweet_count = user_tweets.count()
        if user_tweet_count > 0:
            random_index = random.randint(0, user_tweet_count - 1)
            random_tweet = user_tweets[random_index]
            tweet.append(random_tweet)

    return tweet

#
# Step 7 - Similarity list
#
def find_similarity(full_text, tweet_list):
    similarity = []
    for tweet in tweet_list:
        text = tweet["full_text"]
        token1 = set(nltk.word_tokenize(full_text.lower()))
        token2 = set(nltk.word_tokenize(text.lower()))
        sim = 1 - nltk.jaccard_distance(token1, token2)
        similarity.append(sim)
    return similarity


#
# Step 8 - Intra-group similarity
#
def intragroup_similarity(group1, group2):
    similarity = []
    for user1 in group1:
        sim_user1 = []
        for user2 in group2:
            sim = find_similarity(user1["full_text"], [user2])
            sim_user1.append(sim)
        similarity.append({"user": user1, "similarity_list": sim_user1, 
                                        "avg_similarity": sum(sim_user1)/len(sim_user1)})
    return similarity


#
# Step 10 - Create intra-group similarity clusters
#
def create_similarity_clusters(user_list):
    clusters = {i: [] for i in range(15)}
    for user in user_list:
        avg_similarity = user['avg_similarity'] * 100.0
        if avg_similarity > 6.5:
            clusters[14].append(user['screen_name'])
        elif avg_similarity > 6.0:
            clusters[13].append(user['screen_name'])
        elif avg_similarity > 5.5:
            clusters[12].append(user['screen_name'])
        elif avg_similarity > 5.0:
            clusters[11].append(user['screen_name'])
        elif avg_similarity > 4.5:
            clusters[10].append(user['screen_name'])
        elif avg_similarity > 4.0:
            clusters[9].append(user['screen_name'])
        elif avg_similarity > 3.5:
            clusters[8].append(user['screen_name'])
        elif avg_similarity > 3.0:
            clusters[7].append(user['screen_name'])
        elif avg_similarity > 2.5:
            clusters[6].append(user['screen_name'])
        elif avg_similarity > 2.0:
            clusters[5].append(user['screen_name'])
        elif avg_similarity > 1.5:
            clusters[4].append(user['screen_name'])
        elif avg_similarity > 1.0:
            clusters[3].append(user['screen_name'])
        elif avg_similarity > 0.5:
            clusters[2].append(user['screen_name'])
        elif avg_similarity > 0.0:
            clusters[1].append(user['screen_name'])
        else:
            clusters[0].append(user['screen_name'])
    return clusters

#
# Main
#
def main():
    print("Opening the Mongo DB connection")
    config = Configuration(DATABASE_SETTINGS["HCDE530"])
    collection = DB(config=config)
    c=collection.getCollection("twit")
    self_comparison(c)

    sample1 = get_sample_tweets(c, party="D", legislative_body="senate")
    sample2 = get_sample_tweets(c, party="D", legislative_body="senate")
    sim_list = intragroup_similarity(sample1, sample2)
    cluster = create_similarity_clusters(sim_list)
    #table = create_table(cluster)
    #print(table)
    return


if __name__ == '__main__':
    main()   
