from datetime import datetime, timedelta
import pandas as pd
import json
import requests
import os

SUPERSET_API_BASE_URL = "https://superset.exordelabs.com/"
SUPERSET_USER = "mathias@exordelabs.com"
SUPERSET_PASSWORD = "Blind19282&&"

# Output directory for storing CSV files
OUTPUT_RAW_FOLDER = "raw_data/"


SKIP_EXISTING_FILES = True
# SELECT "__time",
#        "value"
# FROM "druid"."crypto-prices"
# WHERE token = 'shiba-inu'
# LIMIT 100

TIME_GRANULARITY = 'PT15M'  # 'PT1M' for 1 minute, 'PT1H' for 1 hour, 'P1D' for 1 day
# Dictionary containing cryptocurrency names as keys and their corresponding topic IDs as values
topics = {
    "btc": {366},
    "eth": {1143},
    "xrp": {3812,2883},
    "matic": {2606},
    "aave": {17, 3942},
    "ada": {525},
    "sol": {3940},
    "link": {576},
    "uni": {3545},
    "iotx": {4098},
    "akt": {82},
    "ape": {156},
    "skl": {4205},
    "btt": {3983},
    "avax" : {248},
    "chia": {3999},
    # meme coins
    "doge": {1003, 1007},
    "shib": {4201},
    "pepe": {2500, 4164},
    "floki": {1248},
    "bonk": {1005,430},
    "algo": {95},
    # stocks
    "bac": {4353},
    "baba": {98},
    "goog": {1391,109},
    "aapl": {163},
    "amzn": {4383},
    "ibm": {1568,1651},
    "msft": {4498},
    "tsla": {3504,3369},
    "nvda": {2336},
    "amd": {51},
    "2222" : {494},
    "paxg" : {4161}
}

kw_map = {
    "pepe": ["pepe", "pepecoin", "$pepe","pepe token"],
    "wif": ["wif","$wif", "wif token", "wif coin", "wif finance"], # not used yet bc we don't have the price data
    "bac": ["$bac", "bank of america", "bank of america stock", "bac stock"],
    "baba": ["$baba", "alibaba", "alibaba stock", "baba stock"],
    "goog": ["$goog", "google", "google stock", "goog stock"],
    "aapl": ["$aapl", "apple", "apple stock", "aapl stock"],
    "amzn": ["$amzn", "amazon", "amazon stock", "amzn stock"],
    "ibm": ["$ibm", "ibm", "ibm stock", "ibm stock"],
    "msft": ["$msft", "microsoft", "microsoft stock", "msft stock"],
    "tsla": ["$tsla", "tesla", "tesla stock", "tsla stock"],
    "nvda": ["$nvda", "nvidia", "nvidia stock", "nvda stock"],
    "2222": ['2222.SR',
            '$2222',
            '$2222.sr',
            'aramco',
            'saudi aramco',
            'saudiaramco',
            'saudi arabian oil company',
            'saudi arabia oil',
            'saudi arabian']
}


class SupersetClient:
    def __init__(self):
        self._jwtToken = None
        self._sessionCookie = None
        self.authenticate()

    def authenticate(self):
        url = f"{SUPERSET_API_BASE_URL}/api/v1/security/login"
        payload = json.dumps({
            "username": SUPERSET_USER,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        })
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            raise Exception(f"Status code: {response.status_code}")
        data = response.json()
        self._jwtToken = data["access_token"]

    def getCsrfToken(self):
        url = f"{SUPERSET_API_BASE_URL}/api/v1/security/csrf_token/"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._jwtToken}",
        }
        response = requests.request("GET", url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Status code: {response.status_code}")
        self._sessionCookie = response.cookies["session"]
        data = response.json()
        return data["result"]

    def executeSyncQuery(self, query: str, database_id: int = 2):
        url = f"{SUPERSET_API_BASE_URL}/api/v1/sqllab/execute/"
        payload = json.dumps({"database_id": database_id, "sql": query})
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self._jwtToken}",
            "X-Csrftoken": self.getCsrfToken(),
            "Referer": SUPERSET_API_BASE_URL,
            "Cookie": f"session={self._sessionCookie}",
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            # print the reason
            print(response.reason)
            raise Exception(f"Failed to execute query: {response.status_code}")
        return json.loads(response.text)


# print how many iterations we are going to do
print(f"Fetching data for {len(topics)} topics: {topics.keys()}")

client = SupersetClient()

for crypto, topic_ids in topics.items():
    if SKIP_EXISTING_FILES and os.path.exists(f"{OUTPUT_RAW_FOLDER}/{crypto}.csv"):
        print(f"Skipping {crypto}.csv")
        continue
    # print what we are doing
    selected_topic_ids_str = ",".join([f"'{str(i)}'" for i in topic_ids])
    query = f"""
        SELECT 
            TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{TIME_GRANULARITY}') AS "__time",
            count(*) as "volume",
            domain as "domain",
            source_type as "source_type",
            avg("sentiment") as "sentiment",
            sum("emotion_excitement") AS "sum_emotion_excitement",
            sum("emotion_anger") AS "sum_emotion_anger",
            sum("emotion_optimism") AS "sum_emotion_optimism",
            sum("emotion_fear") AS "sum_emotion_fear",
            sum("emotion_gratitude") AS "sum_emotion_gratitude",
            sum("emotion_joy") AS "sum_emotion_joy",
            sum("emotion_admiration") AS "sum_emotion_admiration",
            sum("emotion_annoyance") AS "sum_emotion_annoyance",
            sum("emotion_approval") AS "sum_emotion_approval",
            sum("emotion_nervousness") AS "sum_emotion_nervousness",
            sum("emotion_realization") AS "sum_emotion_realization",
            sum("emotion_relief") AS "sum_emotion_relief",
            sum("emotion_love") AS "sum_emotion_love",
            sum("emotion_sadness") AS "sum_emotion_sadness",
            sum("emotion_disgust") AS "sum_emotion_disgust",
            sum("emotion_disapproval") AS "sum_emotion_disapproval",
            sum("emotion_embarrassment") AS "sum_emotion_embarrassment",
            sum("emotion_pride") AS "sum_emotion_pride",
            sum("emotion_caring") AS "sum_emotion_caring",
            sum("emotion_remorse") AS "sum_emotion_remorse",
            sum("emotion_grief") AS "sum_emotion_grief",
            sum("emotion_curiosity") AS "sum_emotion_curiosity",
            sum("emotion_desire") AS "sum_emotion_desire",
            sum("emotion_disappointment") AS "sum_emotion_disappointment"
        FROM "druid"."posts"
        WHERE "__time" >= '2023-10-17 00:00:00.000000'
            AND "topics" IN ({selected_topic_ids_str})
        GROUP BY TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{TIME_GRANULARITY}'), domain, source_type
    """
    if crypto in kw_map:
        # create a string with the OR operator
        selected_topic_ids_str = ",".join([f"'{str(i)}'" for i in topic_ids])
        selected_kw_str =  ",".join([f"'{kw}'" for kw in kw_map[crypto]])
        query = f"""
            SELECT 
                TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{TIME_GRANULARITY}') AS "__time",
                count(*) as "volume",
                domain as "domain",
                source_type as "source_type",
                avg("sentiment") as "sentiment",
                sum("emotion_excitement") AS "sum_emotion_excitement",
                sum("emotion_anger") AS "sum_emotion_anger",
                sum("emotion_optimism") AS "sum_emotion_optimism",
                sum("emotion_fear") AS "sum_emotion_fear",
                sum("emotion_gratitude") AS "sum_emotion_gratitude",
                sum("emotion_joy") AS "sum_emotion_joy",
                sum("emotion_admiration") AS "sum_emotion_admiration",
                sum("emotion_annoyance") AS "sum_emotion_annoyance",
                sum("emotion_approval") AS "sum_emotion_approval",
                sum("emotion_nervousness") AS "sum_emotion_nervousness",
                sum("emotion_realization") AS "sum_emotion_realization",
                sum("emotion_relief") AS "sum_emotion_relief",
                sum("emotion_love") AS "sum_emotion_love",
                sum("emotion_sadness") AS "sum_emotion_sadness",
                sum("emotion_disgust") AS "sum_emotion_disgust",
                sum("emotion_disapproval") AS "sum_emotion_disapproval",
                sum("emotion_embarrassment") AS "sum_emotion_embarrassment",
                sum("emotion_pride") AS "sum_emotion_pride",
                sum("emotion_caring") AS "sum_emotion_caring",
                sum("emotion_remorse") AS "sum_emotion_remorse",
                sum("emotion_grief") AS "sum_emotion_grief",
                sum("emotion_curiosity") AS "sum_emotion_curiosity",
                sum("emotion_desire") AS "sum_emotion_desire",
                sum("emotion_disappointment") AS "sum_emotion_disappointment"
            FROM "druid"."posts"
            WHERE "__time" >= '2023-10-17 00:00:00.000000'
                AND "topics" IN ({selected_topic_ids_str})
                OR top_keywords IN ({selected_kw_str})
            GROUP BY TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{TIME_GRANULARITY}'), domain, source_type
        """
    # print the query
    print(f"\n________\nFetching data for {crypto}, with topic IDs: {topic_ids}")
    print("Query:\n", query, "\n________\n")
    results = client.executeSyncQuery(query)
    # results i a dict with a key called 'data' that contains the results
    # save the data to a pd dataframe
    df = pd.DataFrame(results['data'])
    # columns are in the 'columns' key: 
    # 'columns': [{'name': '__time', 'type': 'STRING', 'is_dttm': False}, {'name': 'volume', 'type': 'INT', 'is_dttm': False}, {'name': 'domain', 'type': 'STRING', 'is_dttm': False}, {'name': 'source_type', 'type': 'STRING', 'is_dttm': False}, {'name': 'sentiment', 'type': 'FLOAT', 'is_dttm': False}, {'name': 'sum_emotion_excitement', 'type': 'FLOAT', 'is_dttm': False}, {'name': 'sum_emotion_anger', 'type': 'FLOAT', 'is_dttm': False}, {'name': 'sum_emotion_optimism', 'type': 'FLOAT', 'is_dttm': False}, {'name': 'sum_emotion_fear', 'type': 'FLOAT', 'is_dttm': False},
    # read all names from the columns
    column_names = [col['name'] for col in results['columns']]
    # rename the columns
    df.columns = column_names
    # print the first 5 rows
    print(df.head())
    # store the data to a csv file
    output_file = f"{OUTPUT_RAW_FOLDER}/{crypto}.csv"
    print(f"Saving to file: {output_file}")
    df.to_csv(output_file, index=False)