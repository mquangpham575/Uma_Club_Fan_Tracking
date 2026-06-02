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
    for key in ['1', '2', '3', '4']:
        cfg = CLUBS[key].copy()
        cfg['sdate'] = "2026-06-01"
        print(f"Scraping {cfg['title']} for June...")
        text, code = await scrape_club_data(cfg)
        if code == 200:
            data = json.loads(text)
            history = data.get("club_friend_history") or []
            print(f"  {cfg['title']} - June history length: {len(history)}")
        else:
            print(f"  {cfg['title']} failed with status {code}")

if __name__ == '__main__':
    asyncio.run(main())
