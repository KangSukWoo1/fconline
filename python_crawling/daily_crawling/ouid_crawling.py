import os 
from dotenv import load_dotenv, find_dotenv
import pickle
import asyncio 
import aiohttp
load_dotenv(find_dotenv())

# 초당 100개
with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/nicknames.pickle', 'rb') as file:
    nicknames = pickle.load(file)[:5000]
headers = {'x-nxopen-api-key' : os.getenv('x-nxopen-api-key')}

async def fetch_page(semaphore, session, url):
    async with semaphore:
        async with session.get(url, headers=headers) as response:
            await asyncio.sleep(0.3)
            return await response.json()

async def get_ouid_from_api(semaphore, nickname):
    url = f'https://open.api.nexon.com/fconline/v1/id?nickname={nickname}'
    async with aiohttp.ClientSession() as session:
        response = await fetch_page(semaphore, session, url)
        return response

async def main():
    semaphore = asyncio.Semaphore(30)
    tasks = [get_ouid_from_api(semaphore, nickname) for nickname in nicknames]
    ouids = await asyncio.gather(*tasks)
    ouid_list = [ouid for ouid in ouids]
    return ouid_list

ouid_list = asyncio.run(main())

with open('/Users/kangsukwoo/fconline/python_crawling/picklefile/ouid.pickle', 'wb') as file:
    pickle.dump(ouid_list, file)

print('Ouid Crawling Successfully Ended')