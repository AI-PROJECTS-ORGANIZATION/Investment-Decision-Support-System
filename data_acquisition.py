import twint
import pandas as pd
import pandas_datareader as pdr
import os
import glob

# use your path
PATH_STOCK_MARKET_DATA = r'data/raw_data/stock_market_data'
PATH_USERNAMES = r'data/twint_search_parameters'
PATH_TERMS = r'data/twint_search_parameters'
PATH_UNLABELED_TWEETS_BY_USERNAMES = r'data/raw_data/unlabeled_tweets/unlabeled_tweets_by_usernames'
PATH_UNLABELED_TWEETS_BY_DATES = r'data/raw_data/unlabeled_tweets/unlabeled_tweets_by_dates'

# Collecting stock market data.
def collecting_stock_market_data(ticker,start_date,end_date):
    print(f'Collecting stock market data in progress...')
    ticker = ticker
    df = pdr.DataReader(ticker, 'yahoo', start_date, end_date)
    df.to_csv(os.path.join(PATH_STOCK_MARKET_DATA, 'stock_market_data.csv'))
    

# Generate a Twint search string.
def generate_search_string(path):
    df = pd.read_csv(path)
    search_terms = df['terms'].tolist()
    # Twint search string format: "term_1 OR term_2 OR ... OR term_n"
    search_string = ""
    for i in range(len(search_terms)):
        search_string += search_terms[i]
        if(i == len(search_terms)-1):
            break
        search_string += " OR "
    return search_string

# Get Twitter usernames.
def get_twitter_usernames(path):
    df = pd.read_csv(path)
    df.sort_values(by='usernames', inplace=True)
    usernames = df['usernames'].tolist()
    return usernames

# Collecting unlabeled tweets by username.
def collecting_unlabeled_tweets_by_usernames(start_date,end_date,usernames_path,terms_path):
    for username in get_twitter_usernames(usernames_path):
        print(f'Collecting tweets from a user with a username \"{username}\" in progress...')
        # Configure
        c = twint.Config()
        c.Username = username
        c.Search = generate_search_string(terms_path)
        # yyyy-mm-dd format
        c.Since = start_date
        c.Until = end_date
        c.Hide_output = True
        c.Lang = "en"
        c.Pandas = True
        # Run
        twint.run.Search(c)
        df = twint.storage.panda.Tweets_df
        # If there are no rows in DataFrame don't create a .csv file.
        if df.shape[0] == 0:
            continue
        # Only necessary columns ('date','tweet','username','name','link','nlikes','nreplies' and 'nretweets').
        df.drop(df.columns.difference(['date','tweet','username','name','link','nlikes','nreplies','nretweets']), axis=1,inplace=True)
        print("Dataframe length at the beginning:",len(df))
        df.drop_duplicates(inplace=True)
        print("Dataframe length after the complete duplicates drop:",len(df))
        # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
        df.drop_duplicates(subset=['tweet'], keep=False, inplace=True)
        print("Dataframe length after the partial duplicates drop:",len(df))
        df.reset_index(drop=True, inplace=True)
        df.to_csv(os.path.join(PATH_UNLABELED_TWEETS_BY_USERNAMES, f'{username}.csv'))

# Check transformation: from tweets aggregated by users to tweets aggregated by dates. 
# Data must retain the same shape, same number of rows and columns.
def check_transformation():           
    # advisable to use os.path.join as this makes concatenation OS independent
    all_files_usernames = glob.glob(os.path.join(PATH_UNLABELED_TWEETS_BY_USERNAMES, "*.csv"))
    rows = 0
    cols = 0
    for filename in all_files_usernames:
        # Read file
        df = pd.read_csv(filename)
        # Update file (remove duplicates)
        rows = rows + df.shape[0]
        cols = cols + df.shape[0]*df.shape[1]
        df.drop(['Unnamed: 0'],axis='columns', inplace=True)
        df.drop_duplicates(inplace=True)
        # Write file
        df.reset_index(drop=True, inplace=True)
        df.to_csv(filename)
    print(f'Shape of collected tweets by usernames:({rows},{int(cols/rows)})')
              
    # advisable to use os.path.join as this makes concatenation OS independent
    all_files_dates = glob.glob(os.path.join(PATH_UNLABELED_TWEETS_BY_DATES, "*.csv"))
    rows = 0
    cols = 0
    for filename in all_files_dates:
        # Read file
        df = pd.read_csv(filename)
        # Update file (remove duplicates)
        rows = rows + df.shape[0]
        cols = cols + df.shape[0]*df.shape[1]
        df.drop(['Unnamed: 0'],axis='columns', inplace=True)
        df.drop_duplicates(inplace=True)
        # Write file
        df.reset_index(drop=True, inplace=True)
        df.to_csv(filename)
    print(f'Shape of collected tweets by dates:({rows},{int(cols/rows)})')

# Aggregating tweets from different users by date.
def aggregating_tweets_by_date():
    # advisable to use os.path.join as this makes concatenation OS independent                     
    all_files = glob.glob(os.path.join(PATH_UNLABELED_TWEETS_BY_USERNAMES, "*.csv"))     
    for filename in all_files:
        print(filename)
        df = pd.read_csv(filename)
        print(df.shape)
        df.sort_values(by='date', inplace=True)
        # You have tweets by users 
        # but you need tweets by date to create a temporal correlation with stock market data.
        dates = set()
        # Get the unique values of 'date' column
        for date in df['date'].unique().tolist():
            # Aggregating tweets from different users by date.
            # 2020-12-27 13:30:00 becomes 2020-12-27
            # 2020-12-27 23:35:41 becomes 2020-12-27
            dates.add(date.split()[0])
        for date in dates:
            # Drop a row if it is equal to a certain value. 
            # Drop rows from the DataFrame based on certain conditions applied to a column.
            # Drop all rows for which the date is not equal to specific date.
            temp = df.drop(df[~df['date'].str.contains(date)].index)
            # Drop 'Unnamed: 0'column
            temp.drop(['Unnamed: 0'],axis='columns', inplace=True)
            if(os.path.exists(os.path.join(PATH_UNLABELED_TWEETS_BY_DATES, f'{date}.csv')) == True):
                # Remove header duplicates. header=False
                temp.to_csv(os.path.join(PATH_UNLABELED_TWEETS_BY_DATES, f'{date}.csv'), mode='a', header=False)
            else:
                temp.to_csv(os.path.join(PATH_UNLABELED_TWEETS_BY_DATES, f'{date}.csv'))
    # Data sanitization.
    check_transformation()

if __name__ == '__main__':
    ticker = 'AAPL'
    # yyyy-mm-dd format
    # '2010-01-01'
    start_date = '2019-01-01'
    # '2021-04-19'
    end_date = '2020-12-31'
    collecting_stock_market_data(ticker,start_date,end_date)
    usernames_path = os.path.join(PATH_USERNAMES, 'twitter_usernames.csv')
    terms_path = os.path.join(PATH_TERMS, 'twitter_search_terms.csv')
    #collecting_unlabeled_tweets_by_usernames(start_date,end_date,usernames_path,terms_path)
    #aggregating_tweets_by_date()
    check_transformation()




