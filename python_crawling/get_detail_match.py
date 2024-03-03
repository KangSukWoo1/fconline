import pandas as pd 
import requests
import os 
from dotenv import load_dotenv, find_dotenv
import pickle
import asyncio 
import aiohttp
load_dotenv(find_dotenv())
headers = {'x-nxopen-api-key' : os.getenv('x-nxopen-api-key'), 
            'user-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'
           }

with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/matches.pickle', 'rb') as file:
    match_list = pickle.load(file)[200000:]

async def fetch_page(semaphore, session, url):
    async with semaphore:
        async with session.get(url, headers=headers) as response:
            await asyncio.sleep(0.5)
            return await response.json()

async def get_match_detail_from_api(semaphore, matchid):
    url = f'https://open.api.nexon.com/fconline/v1/match-detail?matchid={matchid}'
    async with aiohttp.ClientSession() as session:
        response = await fetch_page(semaphore, session, url)
        return response

async def main():
    semaphore = asyncio.Semaphore(100)
    tasks = [get_match_detail_from_api(semaphore, matchid) for matchid in match_list]
    matches_detail = await asyncio.gather(*tasks)
    match_detail_list = [match_detail for match_detail in matches_detail]
    return match_detail_list


match_detail_list = asyncio.run(main())

with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/matches_detail5.pickle', 'wb') as file:
    pickle.dump(match_detail_list, file)

print('Detail Match List Crawling Successfully Ended')