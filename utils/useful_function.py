import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os

def get_metadata(metadata: str) -> pd.DataFrame: # Load Metadata From FCOnline API
    available_metadatas = ['matchtype','spid','seasonid','spposition','division','division-volta']
    if metadata not in available_metadatas:
        raise ValueError(f"Please choose from following options: {available_metadatas}")
    
    headers = {'x-nxopen-api-key' : os.getenv('x-nxopen-api-key')}
    url = f'https://open.api.nexon.com/static/fconline/meta/{metadata}.json'
    request_json = requests.get(url, headers=headers).json()
    data = pd.DataFrame(request_json)
    return data

def pandas_show_all_columns() -> None:
    pd.set_option('display.max_columns',None)