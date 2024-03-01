from bs4 import BeautifulSoup
import asyncio
import aiohttp
import pandas as pd
import pickle

async def fetch_page(session, url):
    async with session.get(url) as response:
        return await response.text()

async def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    elements = soup.find_all('span', class_='name profile_pointer')
    return [element.text for element in elements]

async def fetch_nickname(page):
    url = f'https://fconline.nexon.com/datacenter/rank_inner?n4seasonno=0&n4pageno={page}'
    async with aiohttp.ClientSession() as session:
        html = await fetch_page(session, url)
        nicknames = await parse_page(html)
        return nicknames

async def main():
    pages = range(1, 501)
    tasks = [fetch_nickname(page) for page in pages]
    nicknames = await asyncio.gather(*tasks)
    all_nicknames = [nickname for page_nicknames in nicknames for nickname in page_nicknames]
    return all_nicknames

nicknames = asyncio.run(main())

with open('nicknames.pickle', 'wb') as file: 
    pickle.dump(nicknames, file)

print('Nicknames Crawling Successfully Ended')
