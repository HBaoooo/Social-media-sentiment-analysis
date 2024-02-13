# Twitter data mining and sentiment-analysis

## Overview
This project focuses on analyzing data gathered from Twitter, specifically tweets from U.S. senators. The objective is to perform various analyses, including lexical analysis, 
sentiment analysis, and toxicity detection, to gain insights into the communication patterns and behaviors of senators on social media platforms.

## Project Details
This project includes three parts:
* Performed data mining with Python and Twitter API to collect tweets from U.S. senators; cleaned raw data
collected and stored the data in MongoDB
* Extracted self-descriptions of all senators to perform basic analytics include lexical analysis and word frequencies
* Used the Perspective API’s machine learning model to detect toxicity in over 120,000 tweets; applied the
VADER model for sentiment analysis of the corresponding tweets and stored the toxicity and sentiment scores for each tweet

## Dependencies
* Python 3.x
* Twitter API
* MongoDB
* NumPy
* Perspective API
* VADER (Valence Aware Dictionary and sEntiment Reasoner)
* Scikit-learn
## Result
<img width="680" alt="Screenshot 2024-02-12 at 5 33 17 PM" src="https://github.com/HBaoooo/Social-media-sentiment-analysis/assets/137658727/7dc9e74e-35d7-4c46-bba8-f4baa57d6181">

The graph above is a scatter plot showing all the tweets scored for us senate members. The x-axis represents the sentiment score of the tweets, 
and the y-axis represents the toxicity score of the tweets. Each data point represents unique tweets identified by the tweet id.

As we can observe from the scatter plot, there tweets with more toxicity scores usually have a negative sentiment score. Tweets with positive sentiment scores 
are more likely to have lower toxicity. The negative linear regression line also shows that negative sentiment can potentially lead to higher toxicity in tweets.

In addition to the insights mentioned in the previous answer, the scatter plot also provides us with some interesting observations regarding the distribution of sentiment and toxicity scores among the tweets. We can observe that a lot of tweets have a sentiment score close to zero, indicating that they are relatively neutral in tone. Also, we can observe that the distribution of toxicity scores is also heavily skewed towards lower values. 
This suggests that most of the tweets about the US Senate members are not highly toxic, although there are some tweets with very high toxicity scores.
