import os
import pandas as pd

###################
BASE_RAW_DATA_FOLDER = "raw_data/"
PROCESSED_DATA_FOLDER = "formatted_data/"
# PROCESSED_DATA_FOLDER = "formatted_data_4h/"

print(PROCESSED_DATA_FOLDER)
################### TRANSFORMATION PARAMETERS ############################
REINDEX_PERIOD = '15T' # 15 minutes
# REINDEX_PERIOD = '240T' # 4 hours
INTERPOLATE_METHOD = 'linear'

ONLY_SOME_FILES = False
SOME_FILES = [] # e.g. ["algo.csv"]

domain_to_identifier_name = {
    "twitter.com": "twitter",
    "reddit.com": "reddit",
    "youtube.com": "youtube",
    "4channel.org": "4chan",
}

specialized_sources = {"financial": ["bloomberg.com", "cnbc.com", "reuters.com", "wsj.com", "ft.com","investing.com","marketwatch.com","seekingalpha.com","followin.io","tradingview.com"],
                       "crypto": ["bitcoinethereumnews.com", "cointelegraph.com", "cryptodaily.co.uk", "decrypt.co" "newsbtc.com", "bitcointalk.org","bitcoinbazis.hu",
                                  "coinpedia.org", "cryptonews.com", "cryptoslate.com", "cryptopotato.com", "cryptovest.com", "cryptonewsz.com", "cryptobriefing.com",
                                  "ambcrypto.com","cryptonews.exchange","thecryptobasic.com","bitpinas.com","coinjournal.net","cryptopotato.com","polygon.com",
                                  "tronweekly.com","nulltx.com","cryptobriefing.com","coinworldstory.com", "coindoo.com","nulltx.com", "usethebitcoin.com", "cryptonewsz.com","bitcoinik.com",
                                  "bitcoinist.com","coincheckup.com","cryptoslate.com","jeuxvideo.com","mastodon.social"]}

def get_file_names(path):
    """
    Identifies files that are present in one list but missing in another, useful for syncing or updating data files.
    
    Parameters:
    - file_names (list): A list of the main file names.
    - prices (list): A list of price-related file names to compare against.
    
    Returns:
    - list: A list of file names that are in `file_names` but not in `prices`.
    """
    try:
        return os.listdir(path)
    except FileNotFoundError:
        return []
    

def transform_dataframe(df):    
    """
    Processes a DataFrame to format and aggregate data specific to different domains and news sources.
    
    Parameters:
    - df (DataFrame): The DataFrame to process.
    
    Returns:
    - DataFrame: A processed DataFrame with time-aggregated and source-separated data, including emotions, volumes, and sentiment.
    """
    # Filter out the emotion columns for renaming
    emotion_columns = [col for col in df.columns if 'emotion_' in col]
    columns_to_rename = ['volume', 'sentiment'] + emotion_columns
    
    # Initialize an empty DataFrame to store the processed data
    processed_df = pd.DataFrame()

    # Process each domain separately and concatenate
    for domain, identifier in domain_to_identifier_name.items():
        domain_df = df[df['domain'] == domain]

        # Rename columns
        renamed_columns = {col: f"{identifier}_{col}" for col in columns_to_rename}
        domain_df = domain_df.rename(columns=renamed_columns)

        # Select only the renamed columns and '__time'
        domain_df = domain_df[['__time'] + list(renamed_columns.values())]

        # Merge with the main processed DataFrame
        if processed_df.empty:
            processed_df = domain_df
        else:
            processed_df = processed_df.merge(domain_df, on='__time', how='outer')

    # Process news data
    news_df = df[df['source_type'] == 'news']
    news_renamed_columns = {col: f"news_{col}" for col in columns_to_rename}
    news_df = news_df.rename(columns=news_renamed_columns)
    news_df = news_df[['__time'] + list(news_renamed_columns.values())]
    processed_df = processed_df.merge(news_df, on='__time', how='outer')

    # Process specialized_sources data
    # Process each specialized source separately and concatenate
    for source_type, sources in specialized_sources.items():
        source_df = df[df['domain'].isin(sources)]
        # print how many rows are there for each source
        print("Found", source_type, "rows", source_df.shape[0], "in the dataframe.")

        # Rename columns
        renamed_columns = {col: f"{source_type}_{col}" for col in columns_to_rename}
        source_df = source_df.rename(columns=renamed_columns)

        # Select only the renamed columns and '__time'
        source_df = source_df[['__time'] + list(renamed_columns.values())]

        # Merge with the main processed DataFrame
        processed_df = processed_df.merge(source_df, on='__time', how='outer')

    # Calculate the total volume
    volume_columns = [col for col in processed_df.columns if 'volume' in col]
    processed_df['total_volume'] = processed_df[volume_columns].sum(axis=1)

    # Fill NaN values with 0
    processed_df = processed_df.fillna(0)

    # Return the processed DataFrame
    return processed_df


# Example usage
# processed_df = process_csv_with_news('path_to_your_csv_file.csv')
# processed_df.to_csv('processed_output.csv', index=False)

####### PROCESSING DATA FILES ########
data_fp_list = get_file_names(BASE_RAW_DATA_FOLDER)
data_fp_list = [os.path.join(BASE_RAW_DATA_FOLDER, file_name) for file_name in data_fp_list]
if ONLY_SOME_FILES:
    data_fp_list = [fp for fp in data_fp_list if os.path.basename(fp) in SOME_FILES]
print("Raw Data File paths =", data_fp_list)

for file_path in data_fp_list:
    print("Processing RAW data file =", file_path)
    # process the file & transform it
    df = pd.read_csv(file_path)
    print("Head of the dataframe before processing:")
    print(df.head())
    processed_df = transform_dataframe(df)
    print(processed_df.head())

    # list all columns
    columns_list = processed_df.columns.tolist()
    # number of columns after:
    print("Number of columns after processing =", len(columns_list))
    # reindex the dataframe by periods of 15 minutes
    # get a new index from datetime
    processed_df['__time'] = pd.to_datetime(processed_df['__time'])
    processed_df = processed_df.set_index('__time')
    processed_df = processed_df.resample(REINDEX_PERIOD).sum()
    # interpolate the missing values
    processed_df = processed_df.interpolate(method=INTERPOLATE_METHOD, limit_direction='forward')
    processed_df = processed_df.reset_index()
    # replace all nan values with 0
    processed_df = processed_df.fillna(0)

    # output the file, same file name, but in the output folder
    file_name = os.path.basename(file_path)
    file_path = os.path.join(PROCESSED_DATA_FOLDER, file_name)
    processed_df.to_csv(file_path, index=False)

    
    # list all columns that have > 90% of NaN values or 00 values
            
    # go:
    df = processed_df
    print(df.head())
    print(df.columns)
    print(df.describe())

    # list all columns that have 99.999999999% of 0 values

    for col in df.columns:
        if "__time" in col:
            continue    
        # COUNT REALLY THE NUMBER OF 0 VALUES in the column
        zero_count = df[df[col] == 0].shape[0]
        if zero_count > 0.999999999*df.shape[0]:
            print("Column", col, "has", zero_count, "zero values out of", df.shape[0], "rows.")

    # print number of emotion columns
    emotion_columns = [col for col in df.columns if 'emotion_' in col]
    print("Number of emotion columns", len(emotion_columns))
    # print number of avg columns
    avg_columns = [col for col in df.columns if 'avg_' in col]
    print("Number of avg columns", len(avg_columns))
    # print number of sum columns
    sum_columns = [col for col in df.columns if 'sum' in col]
    print("Number of sum columns", len(sum_columns))
    # print number of sentiment columns
    sentiment_columns = [col for col in df.columns if 'sentiment' in col]
    print("Number of sentiment columns", len(sentiment_columns))
    # print number of volume columns
    volume_columns = [col for col in df.columns if 'volume' in col]

    # describe in a nicely formatted table string, the number of sentiment columns, list them, same for volume columns
    print("Sentiment columns")
    # list them as comma separated values
    print( ", ".join(sentiment_columns) )
    print("Volume columns")
    # list them as comma separated values
    print( ", ".join(volume_columns) )