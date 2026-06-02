import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
from config.globals import CLUBS
from src.chrono_scraper import scrape_club_data

async def main():
    for key, cfg in list(CLUBS.items())[:5]:
        cfg = cfg.copy()
        cfg['sdate'] = "2026-06-01"
        text, code = await scrape_club_data(cfg)
        if code == 200:
            data = json.loads(text)
            history = data.get("club_friend_history", [])
            daily = data.get("club_daily_history", [])
            print(f"Club {cfg['title']}: friend history len={len(history)}, daily len={len(daily)}")
        else:
            print(f"Club {cfg['title']}: status={code}")

if __name__ == '__main__':
    asyncio.run(main())
