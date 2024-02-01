import csv,sys,json
from mele.classes.utilities.Args import Args
from mele.classes.db.mongo.Configuration import Configuration
from mele.classes.db.mongo.DB import DB
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
}


print("Opening the Mongo DB connection")
config = Configuration(DATABASE_SETTINGS["HCDE530"])
collection = DB(config=config)
s = collection.getCollection("sentiment")
t = collection.getCollection("toxicity")

merged_scores = {}
for doc in t.find():
    tweet_id = doc['id_str']
    if tweet_id not in merged_scores:
        analysis = json.loads(doc['analysis'])
        #print(analysis)

        try:
            merged_scores[int(tweet_id)] = {'toxicity_score': analysis['attributeScores']['TOXICITY']['summaryScore']['value']}
        except:
            print(tweet_id)
#print(merged_scores)
for doc in s.find():
    tweet_id = int(doc['id_str'])
    if tweet_id in merged_scores:
        merged_scores[tweet_id]['sentiment_score'] = doc['analysis']['raw_scores']['compound']
        #print(merged_scores)


with open('Sentiment_vs_Toxicity.csv', 'w', newline='') as csvfile:
    fieldnames = ['tweet_id', 'sentiment_score', 'toxicity_score']
    file = csv.DictWriter(csvfile, fieldnames=fieldnames)
    file.writeheader()
    for tweet_id, doc in merged_scores.items():

        row = {'tweet_id': tweet_id, 'sentiment_score': doc['sentiment_score'], 'toxicity_score': doc['toxicity_score']}
        file.writerow(row)

