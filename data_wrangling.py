import pandas as pd
# Data wrangling
# 1. Handle Big Data files that don’t fit in RAM (handling large dataframes).
# 2. Remove unimportant columns to save memory. Remove unnecessary columns.
# 3. Drop Null/NA Values from DataFrame
# 4. Drop a row if it contains a certain value. Drop rows from the dataframe based on 
# certain conditions applied to a column.
# 5. Drop Duplicate Rows (complete and partial duplicates)
# 6. Set unique dataset header (text, sentiment).
# 7. Set unique sentiment labels (-1: negative, 0: neutral, 1: positive).
# 8. Inplace transformation reduces memory consumption.
# 9. Read Big Data files in chunk size. 



# Remove the same tweets that are labeled with a different sentiment. 
# Resolving conflict situation where the same tweet is annotated with a different sentiment. 
# df.drop_duplicates(inplace=True) couldn't recognize this case so it is important to be implemented. 
# df.drop_duplicates(inplace=True) look complete duplicates, tweets that have the same text and same sentiment. 
# It is unable to resolve partial duplicates. Partial duplicates are tweets that have the same text, but different sentiment.
def remove_partial_duplicates(df):
    duplicates = {}
    number_of_partial_duplicates = 0
    for key in df['text']:
        if key not in duplicates:
            duplicates[key] = 1
        else:
            duplicates[key] = duplicates.get(key) + 1
    for k, v in duplicates.items():
        if v !=1:
            # Drop all rows for which the text is equal to k keys in duplicates dictionary.
            df.drop(df[df['text'] == k].index, inplace = True)
            number_of_partial_duplicates = number_of_partial_duplicates + 1
            #print(k,v)
            print(v) 
    print('The number of partial duplicates in dataset:', number_of_partial_duplicates)



def data_wrangling_corpus1(path):
    df = pd.read_csv(path)
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Drop all rows for which the sentiment is equal to "not_relevant"
    df.drop(df[df['sentiment'] == "not_relevant"].index, inplace = True)
    print("Dataframe length after the not_relevant drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # 1 = negative = -1 
    # 3 = neutral = 0
    # 5 = positive = 1
    sentiment_labels = []
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == str(1):
            sentiment_labels.append(-1)
            negative +=1
        elif sentiment == str(3):
            sentiment_labels.append(0)
            neutral +=1
        elif sentiment == str(5):
            sentiment_labels.append(1)
            positive += 1
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus1.csv')
    df.to_pickle('data\corpora\corpus1.pkl')

def data_wrangling_corpus2(path):
    df = pd.read_csv(path)
    # Set unique dataset header (text, sentiment).
    df.columns =['sentiment', 'text']
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # negative = -1 
    # neutral = 0
    # positive = 1
    sentiment_labels = []
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == "negative":
            sentiment_labels.append(-1)
            negative +=1
        elif sentiment == "neutral":
            sentiment_labels.append(0)
            neutral +=1
        elif sentiment == "positive":
            sentiment_labels.append(1)
            positive += 1
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus2.csv')
    df.to_pickle('data\corpora\corpus2.pkl')

def data_wrangling_corpus3(path):
    # Read csv file to Dataframe with ; delimiter 
    df = pd.read_csv(path, sep=';')
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # negative = -1 
    # neutral = 0
    # positive = 1
    sentiment_labels = []
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == "negative":
            sentiment_labels.append(-1)
            negative +=1
        elif sentiment == "neutral":
            sentiment_labels.append(0)
            neutral +=1
        elif sentiment == "positive":
            sentiment_labels.append(1)
            positive += 1
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus3.csv')
    df.to_pickle('data\corpora\corpus3.pkl')

def data_wrangling_corpus4(path):
    df = pd.read_csv(path)
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # negative = -1 
    # neutral = 0
    # positive = 1
    sentiment_labels = []
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == "negative":
            sentiment_labels.append(-1)
            negative +=1
        elif sentiment == "neutral":
            sentiment_labels.append(0)
            neutral +=1
        elif sentiment == "positive":
            sentiment_labels.append(1)
            positive += 1
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus4.csv')
    df.to_pickle('data\corpora\corpus4.pkl')

# Inplace transformation reduces memory consumption. 
# The same DataFrame is used for transformation. 
# There is no need for an additional DataFrame.
def corpus5_inplace_transformation(df):
    # Reshaping existing DataFrame:
    # DataFrame transformation transform DataFrame from (row, col) shape to (row*(col-1), 2) shape
    print("DataFrame shape before inplace transformation:", df.shape)
    columns = list(df.columns)
    # Looping through a list starting at index 2 because 
    # the first two columns will be anchor columns. 
    # Data from other columns will be added to those two columns.
    for i in range(2,len(columns)):
        # Temporary DataFrame that is going to be concatenated to existing DataFrame.
        temp = df[[columns[0], columns[i]]].copy(deep=True)
        temp.dropna(inplace=True)
        temp.columns =[columns[0], columns[1]]
        df = pd.concat([df,temp]).reset_index(drop=True)
        df.drop(columns[i], axis='columns', inplace=True)
    df.columns =['sentiment', 'text']
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    print("DataFrame shape after inplace transformation:", df.shape)
    return df

# Not Inplace transformation increases memory consumption. 
# The additional DataFrame is used for transformation. 
# There is a need for an additional DataFrame which leads to increased memory consumption.
def corpus5_transformation(df):
    print("DataFrame shape before transformation:", df.shape)
    df1 = pd.DataFrame()
    df1 = pd.DataFrame(columns = ['sentiment', 'text'])
    for col in df.columns:
        if col == df.columns[0]:
            continue
        temp = df[[df.columns[0], col]]
        temp.columns =['sentiment', 'text']
        df1 = pd.concat([df1,temp]).reset_index(drop=True)
        df.drop(col, axis='columns', inplace=True)
    df1.dropna(inplace=True)
    # Only two necessary columns (text and sentiment).
    df1 = df1[['text','sentiment']]
    print("DataFrame shape after transformation:", df1.shape)
    return df1


def data_wrangling_corpus5(path):
    df = pd.read_csv(path)
    # Remove Date column from DataFrame
    df.drop('Date', axis='columns', inplace=True)
    # Inplace transformation reduces memory consumption. Both transformations result 
    # in the same transformed DataFrame but Inplace transformation is more memory saving.
    #df = corpus5_inplace_transformation(df)
    df = corpus5_transformation(df)
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # 0 = -1
    # 1 = 1
    sentiment_labels = []
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == 0:
            sentiment_labels.append(-1)
            negative +=1
        elif sentiment == 1:
            sentiment_labels.append(1)
            positive += 1
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus5.csv')
    df.to_pickle('data\corpora\corpus5.pkl')

def data_wrangling_corpus6(path):
    # Read Big Data files in chunk size
    chunk_list = []  
    for chunk in pd.read_csv(path, chunksize = 1000):
        chunk = chunk[['headline','sentimentClass']]
        chunk_list.append(chunk)
    df = pd.concat(chunk_list)
    # Only two necessary columns (text and sentiment).
    df.columns =['text', 'sentiment']
    print(df.head())
    print(df.tail())
    print("Dataframe length at the beginning:",len(df))
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print("Dataframe length after the NA drop:",len(df))
    # Remove complete duplicates (same text same sentiment). Drop duplicate rows in place keeping the first one.
    df.drop_duplicates(inplace=True)
    print("Dataframe length after the complete duplicates drop:",len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    #remove_partial_duplicates(df)
    df.drop_duplicates(subset=['text'], keep=False, inplace=True)
    print("Dataframe length after the partial duplicates drop:",len(df))
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # negative = -1 
    # neutral = 0
    # positive = 1
    positive = 0
    negative = 0
    neutral = 0
    total = 0
    for sentiment in df['sentiment']:
        total +=1
        if sentiment == -1:
            negative +=1
        elif sentiment == 0:
            neutral +=1
        elif sentiment == 1:
            positive += 1
        else:
            print("Unknown sentiment label.")

    print(df.head())
    print(df.tail())
    print("Dataframe length at the end:",len(df))
    print('The number of tweets in dataset:',total)
    print('The number of negative tweets in dataset:',negative)
    print('The number of neutral tweets in dataset:',neutral) 
    print('The number of positive tweets in dataset:',positive)
    df.to_csv('data\corpora\corpus6.csv')
    df.to_pickle('data\corpora\corpus6.pkl')

if __name__ == '__main__':
    path1 = r'data\raw_data\labeled_tweets\Apple-Twitter-Sentiment-DFE.csv'
    data_wrangling_corpus1(path1)
    path2 = r'data\raw_data\labeled_tweets\all-data.csv'
    data_wrangling_corpus2(path2)
    path3 = r'data\raw_data\labeled_tweets\tweets_labelled_09042020_16072020.csv'
    data_wrangling_corpus3(path3)
    path4 = r'data\raw_data\labeled_tweets\twt_sample.csv'
    data_wrangling_corpus4(path4)
    path5 = r'data\raw_data\labeled_tweets\Combined_News_DJIA.csv'
    data_wrangling_corpus5(path5)
    path6 = r'data\raw_data\labeled_tweets\news_train_from2013.csv'
    data_wrangling_corpus6(path6)

    



