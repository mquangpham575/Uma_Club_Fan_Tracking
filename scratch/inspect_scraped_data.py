import os
import sys
import asyncio
import json

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import CLUBS
from src.chrono_scraper import scrape_club_data

async def main():
    cfg = CLUBS['1'].copy()
    cfg['sdate'] = "2026-05-01"
    print(f"Scraping {cfg['title']} with sdate={cfg['sdate']}...")
    text, code = await scrape_club_data(cfg)
    print(f"Response status: {code}")
    if code == 200:
        data = json.loads(text)
        print("Keys in response:", list(data.keys()))
        history = data.get("club_friend_history") or []
        print(f"Length of club_friend_history: {len(history)}")
        if history:
            print("First item in history:", history[0])
            print("Number of items in history:", len(history))
        else:
            print("History is empty!")
            print("Full response subset:", json.dumps(data)[:1000])

if __name__ == '__main__':
    asyncio.run(main())
