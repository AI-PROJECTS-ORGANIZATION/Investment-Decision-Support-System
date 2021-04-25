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



# Remove the same tweets that are labeled with a different sentiment. 
# Resolving conflict situation where the same tweet is annotated with a different sentiment. 
# df.drop_duplicates(inplace=True) couldn't recognize this case so it is important to be implemented. 
# df.drop_duplicates(inplace=True) look complete duplicates, tweets that have the same text and same sentiment. 
# It is unable to resolve partial duplicates. Partial duplicates are tweets that have the same text, but different sentiment.
def remove_partial_duplicates(df):
    duplicates = {}
    brojac = 0
    for key in df['text']:
        if key not in duplicates:
            duplicates[key] = 1
        else:
            duplicates[key] = duplicates.get(key) + 1
    for k, v in duplicates.items():
        if v !=1:
            # Drop all rows for which the text is equal to k keys in duplicates dictionary.
            df.drop(df[df['text'] == k].index, inplace = True)
            brojac = brojac + 1 
            print(k,v)



def data_wrangling_corpus1(path):
    df = pd.read_csv(path)
    # Only two necessary columns (text and sentiment).
    df = df[['text','sentiment']]
    # Dropping Rows with NA inplace. We can pass inplace=True to change the source DataFrame itself. 
    # It’s useful when the DataFrame size is huge and we want to save some memory.
    df.dropna(inplace=True)
    print(len(df))
    # Drop all rows for which the sentiment is equal to "not_relevant"
    df.drop(df[df['sentiment'] == "not_relevant"].index, inplace = True)
    print(len(df))
    # Remove Duplicate Rows in place (remove complete duplicates = same text same sentiment)
    df.drop_duplicates(inplace=True)
    print(len(df))
    # Remove Duplicate Rows in place (remove partial duplicates = same text different sentiment)
    remove_partial_duplicates(df)
    # Set unique sentiment labels (-1: negative, 0: neutral, 1: positive). 
    # Sentiment labels mapping:
    # 1 = negative = -1 
    # 3 = neutral = 0
    # 5 = positive = 1
    sentiment_labels = []
    for sentiment in df['sentiment']:
        if sentiment == str(1):
            sentiment_labels.append(-1)
        elif sentiment == str(3):
            sentiment_labels.append(0)
        elif sentiment == str(5):
            sentiment_labels.append(1)
        else:
            print("Unknown sentiment label.")

    df['sentiment'] = sentiment_labels
    print(df.head())
    print(df.tail())
    df.to_csv('apple.csv')
    print(len(df))

if __name__ == '__main__':
    path1 = 'data\corpora\Apple-Twitter-Sentiment-DFE.csv'
    data_wrangling_corpus1(path1)