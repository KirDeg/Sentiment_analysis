import tweepy as tw
from searchtweets import gen_rule_payload, load_credentials, collect_results
import pandas as pd
import re
import yaml
import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup

def main():
    clean()
    sentiment_analysis()
    visualisation()

def clean():
    #clean out the DATA table before running the program
    conn=sqlite3.connect('Sentiment.sqlite')
    clear_data="DELETE from DATA"
    conn.execute(clear_data)
    clear_sid="DELETE from SQLITE_SEQUENCE where NAME='DATA'"
    conn.execute(clear_sid)
    conn.commit()

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
    #define headers in order to simulate page access through a browser
    headers= {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'text/plain;charset=UTF-8',
    'Cookie': 'session-id=133-3374249-3871367; session-id-time=2082787201l; lc-main=en_US; sp-cdn="L5Z9:DE"; x-wl-uid=1MHEFKyyeqHgh7WLjP4lIWtBfyNAxlCdg6dj40pmfGNapSsjgBF6BalrxL88qK+lqPzxvSjGtj04=; ubid-main=132-6608445-2866219; session-token=l3C1g1rVeq0jH2J4Qd52Q1SDO5sLh2IQdEKt64TqjfRJPFIgjTXIev/VSyqs6R6sDPF4+9nRdH7SGpaPaohgTOxNSRbLi13Yax0o87z67zmyayZi5tXZ4k0vk1HMvgz0aVr1WoO0wXYX5utcZd2hAnnJWLHpO7gYKWC6VhaP0XnEPGNmcCs0QNLi8KDTI8ee; i18n-prefs=USD; skin=noskin',
    'Connection': 'keep-alive'}

    #this loop is accessing the links to the selected products on Amazon one by one
    with open('terms.txt', 'r') as my_file:
        for line in my_file:
            #here I am separating every line into the URL and the name of the product
            url=line.split(',')[-1]
            product = line.split(',')[0]

            #accessing and loading the html text of the product page
            html = requests.get(url, headers=headers)
            soup = BeautifulSoup(html.text, 'html.parser')

            #from that page I need to follow the link "See all reviews" and load its html text
            link = soup.find("a", {'data-hook': "see-all-reviews-link-foot"})["href"]
            link="https://www.amazon.com"+link
            html_reviews=requests.get(link,headers=headers)

            #preparing for the next loop: setting the page counter to 1 and the number of reviews to 0
            number_reviews=0
            page_counter=1

            #this loop is scraping the data from a page with reviews and then moves on to the next page
            while True:
                #emptying all the lists
                reviews = []
                users = []
                dates = []
                products = []
                review_block = []

                #accessing a page with reviews and scraping its entire contents
                page_no_link=link+'&pageNumber=' + str(page_counter)
                page_no_reviews = requests.get(page_no_link, headers=headers)
                bsObj = BeautifulSoup(page_no_reviews.text, 'html.parser')

                #the loop keeps running as long as there are new reviews on the page
                if bsObj.find("div",{"class":"a-section a-spacing-top-large a-text-center no-reviews-section"})==None:
                    review_block = bsObj.find(class_="reviews-content")

                    #load and append the reviews, removing the space in the beginning and end of each review
                    for review in review_block.find_all("span",{"data-hook":"review-body"}):
                        reviews.append(review.text[1:-1])
                        #append the product name so that it repeats for every review
                        products.append(product)

                    #load and append the user names
                    for user in review_block.find_all("span",{"class":"a-profile-name"}):
                        users.append(user.text)

                    #load, clean and append the dates of the reviews
                    for date in review_block.find_all("span", {"data-hook": "review-date"}):
                        date_clean_1 = str(date).split('on ')[-1]
                        date_clean_2=date_clean_1.split('<')[0]

                        #"translate" the dates to datetime format and then transform them to yyyy-mm-dd
                        date_time_obj = datetime.datetime.strptime(date_clean_2, '%B %d, %Y')
                        dates.append(date_time_obj.date())

                    #after the necessary data has been extracted from the page, it is loaded into a dataframe
                    rev = {'Product': products, 'User': users, 'Date': dates, 'Message': reviews}
                    review_data = pd.DataFrame.from_dict(rev)

                    #filter only for reviews that were written in May 2020
                    review_data_new = review_data[(review_data['Date'] >= datetime.date(2020, 5, 1)) & (review_data['Date'] <= datetime.date(2020, 5, 31))]
                    review_data_new = review_data_new[(review_data_new['Message']!="")]

                    #drop any possible duplicated reviews
                    review_data_new = review_data_new.drop_duplicates("Message")

                    #update the counter for the number of saved reviews for this product
                    number_reviews = number_reviews + (len(review_data_new))
                    
                    #append to the DATA sqlite table
                    conn = sqlite3.connect('Sentiment.sqlite')
                    review_data_new.to_sql('DATA', conn, if_exists='append', index=False)
                    conn.commit()

                    #if there are still less then 20 reviews, the program is moving on to the next page with reviews
                    if number_reviews<20:
                        #update page counter in order to move to the next page
                        page_counter+=1

                    #once we've got 20 reviews for one product, we move on to the next product
                    else:
                        break

                #once the page is reached that is informing that there are no more reviews, the data is exported, the loop stops and moves onto the next product
                else:
                    break

            else:
                continue

    pass


def extracting_tweets():
    # Authentication parameters.
    API_KEY = '4PunwNYcSSB03TTBzks3cIquM'
    API_SECRET_KEY = 'RWkiKcaMvDqHLJwaLRBgm7NNElYmlrziGzlRpEM7bZJ3kPwOxK'
    DEV_ENVIRONMENT_LABEL = 'newprototype'
    API_SCOPE = 'fullarchive'

    # Search parameters.
    RESULTS_PER_CALL = 50  # 100 for sandbox
    TO_DATE = '2020-05-31'  # format YYYY-MM-DD
    FROM_DATE = '2020-05-01'  # format YYYY-MM-DD
    MAX_RESULTS = 100
    TW_PER_PRODUCT = 20  # number of tweets required per produ

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
    i = 1

    data = pd.DataFrame(columns=['SID', 'Product', 'User', 'Date', 'Message', 'Sentiment'])

    # Extracting filtered data from tweets.
    # for term in terms:
    for term in terms:
        rule = gen_rule_payload("Garmin Vivoactive" + " lang:en " + "-has:links", results_per_call=RESULTS_PER_CALL,
                                from_date=FROM_DATE, to_date=TO_DATE)
        tweets = collect_results(rule, max_results=MAX_RESULTS, result_stream_args=premium_search_args)
        j = 1
        for tweet in tweets:
            # Removing retweets.
            if (not "retweeted_status" in tweet) and j <= TW_PER_PRODUCT:
                data.at[i - 1, 'SID'] = i
                data.at[i - 1, 'Product'] = term
                data.at[i - 1, 'User'] = tweet['user']['name']
                data.at[i - 1, 'Date'] = datetime.strftime(datetime.strptime(
                    tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d')
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
    # Automating the sentiment analysis: ETLs have to be performed first
    ETL()

    # Sentiment Analysis
    # URL of the API, needed in order to access it
    url = "https://japerk-text-processing.p.rapidapi.com/sentiment/"
    # Headers, using my individual RapidAPI key that is linked to my account
    headers = {
        'x-rapidapi-host': "japerk-text-processing.p.rapidapi.com",
        'x-rapidapi-key': "d6be6d336fmsh7f6fb5b7c8d7249p17a225jsn569fa9d8ab35",
        'content-type': "application/x-www-form-urlencoded"
    }

    # Connecting to the DATA table provided by twitter ETL and amazon ETL
    connection = sqlite3.connect('Sentiment.sqlite')
    cursor = connection.cursor()
    connection.commit()
    # Selecting the relevant data: SID and Messages only
    cursor.execute("SELECT SID, Message FROM DATA")
    rows = cursor.fetchall()

    # For loop iterating through the DATA table
    # Determining the sentiment for each message
    # Updating the DATA table accordingly
    for sid, text in rows:
        # Language set to English, text will be inserted here
        payload = "language=english&text=" + str(text) + ""
        # Request link for API
        response = requests.request("POST", url, data=payload, headers=headers)
        sentiment = response.json()
        # Using the dictionary nature of the json response to filter for the label only (neg, pos OR neutral)
        label = sentiment["label"]

        # Updating the DATA table sentiment column by using an if statement
        if label == 'neg':
            script = "UPDATE DATA SET Sentiment=? WHERE sid=?;"
            cursor.execute(script, (label, sid))
            connection.commit()
        elif label == 'pos':
            script = "UPDATE DATA SET Sentiment=? WHERE sid=?;"
            cursor.execute(script, (label, sid))
            connection.commit()
        elif label == 'neutral':
            script = "UPDATE DATA SET Sentiment=? WHERE sid=?;"
            cursor.execute(script, (label, sid))
            connection.commit()

    connection.close()

    pass


def visualisation():
    """Tim's code here"""
    pass


if __name__ == "__main__":
    main()
