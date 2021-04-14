import json

import requests
import pandas as pd
import re
from dateparser.date import DateDataParser
from datetime import timedelta, datetime
import time
import random
import os
import numpy as np

def get_transaction_list(page=1, save=False):
    """ Get the list of transaction on the bscscan.com/tokentxns

    :param page:
    :param save:
    :return:
    """

    path = "./bsc-txns/"
    headers = {
        'authority': 'bscscan.com',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'origin': 'https://bscscan.com',
        'content-type': 'application/x-www-form-urlencoded',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-gpc': '1',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://bscscan.com/tokentxns',
        'accept-language': 'en-US,en;q=0.9',
    }
    data = {
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlRecordsPerPage',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': 'tyaWVZtVIcox53PAgl8Cg7o4rS646MXzyP0MBL24NBXx/igPkQwAgUalGPJ/kcAaFUULU/TFjWJp66Dh2dyRl/lMLEP5UuVosqryUatP7+A=',
        '__VIEWSTATEGENERATOR': 'CBF7936C',
        '__EVENTVALIDATION': 'cej1e9PiZXQnlScJdUIUjpTw0bIsJamvVpvKJ7ZFAZ1uANm06lSzLVsz0Chy9zEqejUFjYNxHWMsb86MBc7aMVZ836Kd1/uRB3S87lrsxszHSDwpuN997C7prJA1AEAuBSmBrSvExpsscrjglOaQDAqK7Zer5pd+kuxPjm7voI1Hj2rBWbK4Fd9ZpwsKCZ1T+z9CpAn4raYBh4woFm7rgQ==',
        'ctl00$ContentPlaceHolder1$ddlRecordsPerPage': '100'
    }

    response = requests.post(f'https://bscscan.com/tokentxns?ps=100&p={page}', headers=headers, data=data)
    df = pd.read_html(response.text)[0]
    df.columns = ["view", "tx_hash", "age", "from", "icon", "to", "value", "token"]
    df = df[["tx_hash", "age", "from", "to", "value", "token"]]
    df["token_symbol"] = df["token"].apply(lambda x: re.search(r'\((.*?)\)',x).group(1))
    ddp = DateDataParser(languages=['en'])
    df["age"] = df["age"].apply(lambda x: pd.to_datetime(ddp.get_date_data(str(x)).__dict__["date_obj"] - timedelta(hours=1)))
    print(f"Found record {len(df)}")

    if save:
        timestamp = int(time.time())
        df.to_json(f"{path}{timestamp}.json", orient="records")

    sleep_time = random.randint(1,4)
    print(f"Sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)
    return df

def retrieve_all_transaction():
    """ Retrieve all the database in json and return it as json

    :return:
    """
    all_jsons = [each for each in os.listdir("./bsc-txns/") if each.endswith(".json")][-10:]
    all_json_df = [pd.read_json(f"./bsc-txns/{each}") for each in all_jsons]
    if len(all_jsons) >= 1:
        df_transactions = pd.concat(all_json_df)
        df_transactions = df_transactions.drop_duplicates(subset=["tx_hash"])
        df_transactions.age = df_transactions.age.apply(lambda x: datetime.fromtimestamp(x/1000))
        return df_transactions
    else:
        return {"message": "no file found"}

def get_tokens_list():
    """ Retrieve the list of all token and statistics

    :return:
    """
    all_transaction = retrieve_all_transaction()
    return json.dumps(list(all_transaction.token.unique()))

def get_tokens_stats():
    """ Retrieve the list of all token statistics

    :return:
    """
    df_transactions = retrieve_all_transaction()
    df_transactions.value = df_transactions.value.apply(lambda x: float(x.replace("...","")) if isinstance(x, str) else  float(x))
    df_group = df_transactions.groupby("token").agg({'age': ['min', 'max'], 'token': ['count'], 'value': ['sum','max']})
    df_group = pd.DataFrame(df_group.to_records())
    df_group.columns = ['token', 'first_seen', 'last_seen', 'tx_count','total_value', 'max_value_in_tx']
    return df_group

def read_token_stats():
    """instead of computing stats on the fly we pre-compute them after each query bsc we do so that it's faster to load
    """
    import os
    print("Reading token stats")
    all_stats_files = sorted([each for each in os.listdir('./bsc-stats/') if each.endswith(".csv")])
    print(f"Found {len(all_stats_files)}")
    latest_stats_file = all_stats_files[-1]
    print(f"Latest File {latest_stats_file}")
    df_tmp = pd.read_csv(f"./bsc-stats/{latest_stats_file}", index_col=0)
    df_tmp.sort_values("last_seen", ascending=False, inplace=True)
    return df_tmp

def save_token_stats():
    """Process the token latest statistics"""
    
    timestamp = int(time.time())
    print("Getting stats")
    token_stats = get_tokens_stats()
    print(f"Found stats. Here {len(token_stats)}")
    token_stats.to_csv(f"./bsc-stats/{timestamp}.csv")
    

def query_bsc():
    """ Query BsCan
    :return:
    """
    page_list_size = 50 # We request the first 50 pages
    for each in range(1,page_list_size+1):
        print(f"fetching page {each}")
        df = get_transaction_list(page=each, save=True)
    print("Finished getting the transactions")
    print("Saving stats")
    save_token_stats()
    return {"retrieved" : len(df)}
