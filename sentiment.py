from searchtweets import gen_rule_payload, load_credentials, collect_results
import pandas as pd
import re
import yaml
import sqlite3
from datetime import datetime


def main():
    ETL()
    sentiment_analysis()
    visualisation()


def ETL():
    ETL_twitter()
    ETL_amazon()


def ETL_twitter():
    safemode = True
    if safemode:
        data = pd.read_csv('data.csv')
    else:
        data = extracting_tweets()
    data_cleaned = cleaning_tweets(data)
    loading_to_database(data_cleaned)


def ETL_amazon():
    """Elena's code here"""
    pass


def extracting_tweets():
    # Authentication parameters.
    API_KEY = '4PunwNYcSSB03TTBzks3cIquM'
    API_SECRET_KEY = 'RWkiKcaMvDqHLJwaLRBgm7NNElYmlrziGzlRpEM7bZJ3kPwOxK'
    DEV_ENVIRONMENT_LABEL = 'newprototype'
    API_SCOPE = 'fullarchive'

    # Search parameters.
    RESULTS_PER_CALL = 100  # 100 for sandbox, 500 for paid tiers
    TO_DATE = '2020-05-31'  # format YYYY-MM-DD HH:MM (hour and minutes optional)
    FROM_DATE = '2020-05-01'  # format YYYY-MM-DD HH:MM (hour and minutes optional)
    MAX_RESULTS = 100

    # JSON file for dumping.
    FILENAME = 'twitter_data.json'

    # Making twitter_keys.yaml for load_credentials.
    config = dict(
        search_tweets_api=dict(
            account_type='premium',
            endpoint=f"https://api.twitter.com/1.1/tweets/search/{API_SCOPE}/{DEV_ENVIRONMENT_LABEL}.json",
            consumer_key=API_KEY,
            consumer_secret=API_SECRET_KEY
        )
    )

    with open('twitter_keys.yaml', 'w') as config_file:
        yaml.dump(config, config_file, default_flow_style=False)

    # Authentication.
    premium_search_args = load_credentials("twitter_keys.yaml",
                                           yaml_key="search_tweets_api",
                                           env_overwrite=False)
    # Reading terms.
    path_to_terms = 'terms.txt'
    terms = reading_terms(path_to_terms)

    # Extracting tweets.
    n = 50  # number of parsed tweets
    tw_per_product = 20  # number of tweets required per produ
    i = 1

    data = pd.DataFrame(columns=['SID', 'Product', 'User', 'Date', 'Message', 'Sentiment'])

    # Extracting filtered data from tweets.
    # for term in terms:
    for term in terms:
        rule = gen_rule_payload("Garmin Vivoactive" + " lang:en " + "-has:links", results_per_call=100,
                                from_date='2020-06-12', to_date='2020-06-30')
        tweets = collect_results(rule, max_results=n, result_stream_args=premium_search_args)
        j = 1
        for tweet in tweets:
            # Removing retweets.
            if (not "retweeted_status" in tweet) and j <= tw_per_product:
                data.at[i - 1, 'SID'] = i
                data.at[i - 1, 'Product'] = term
                data.at[i - 1, 'User'] = tweet['user']['name']
                data.at[i - 1, 'Date'] = datetime.strftime(datetime.strptime(
                    tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'), '%d.%m.%Y')
                try:
                    data.at[i - 1, 'Message'] = tweet['extended_tweet']['full_text']
                except:
                    data.at[i - 1, 'Message'] = tweet['text']
                i += 1
                j += 1

    return data


def reading_terms(path_to_terms):
    txt = open(path_to_terms, 'r')
    terms = []

    for line in txt:
        terms.append(re.split(',', line)[0])

    return terms


def cleaning_tweets(data):
    for i in range(len(data.Message)):
        data.at[i, "Message"] = data.Message[i].encode('ascii', 'ignore').decode('ascii')
        data.at[i, "Message"] = re.sub(r"(@|#).\w*", r"", data.Message[i])

    return data


def loading_to_database(cleanded_data):
    with sqlite3.connect('Sentiment.sqlite') as conn:
        c = conn.cursor()
        for item in cleanded_data.values:
            c.execute("INSERT INTO DATA VALUES (?, ?, ?, ?, ?, ?)",
                        (item))
        conn.commit()
        c.close()


def sentiment_analysis():
    """Sarah's code here"""
    pass


def visualisation():
    """Tim's code here"""
    pass


if __name__ == "__main__":
    main()
