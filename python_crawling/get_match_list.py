import os 
from dotenv import load_dotenv, find_dotenv
import pickle
import asyncio 
import aiohttp
load_dotenv(find_dotenv())
headers = {'x-nxopen-api-key' : os.getenv('x-nxopen-api-key')}

with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/ouid.pickle', 'rb') as file:
    ouid_pickle = pickle.load(file)

# Drop Null Values Due To Changed Nickname
ouid_list = [value for item in ouid_pickle for value in item.values() if 'error' not in item]

async def fetch_page(semaphore, session, url):
    async with semaphore:
        async with session.get(url, headers=headers) as response:
            await asyncio.sleep(0.3)
            return await response.json()

async def get_match_from_api(semaphore, ouid):
    url = f'https://open.api.nexon.com/fconline/v1/user/match?ouid={ouid}&matchtype=50&offset=0&limit=50'
    async with aiohttp.ClientSession() as session:
        response = await fetch_page(semaphore, session, url)
        return response

async def main():
    semaphore = asyncio.Semaphore(30)
    tasks = [get_match_from_api(semaphore, ouid) for ouid in ouid_list]
    matches = await asyncio.gather(*tasks)
    match_dict = set([match for sublist in matches for match in sublist])
    match_list = list(match_dict)
    return match_list


match_list = asyncio.run(main())

with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/matches.pickle', 'wb') as file:
    pickle.dump(match_list, file)

print('Match List Crawling Successfully Ended')