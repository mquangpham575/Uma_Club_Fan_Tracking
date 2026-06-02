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
    cfg = CLUBS['3'].copy()
    if 'sdate' in cfg:
        del cfg['sdate']
    text, code = await scrape_club_data(cfg)
    print('Status:', code)
    if code == 200:
        data = json.loads(text)
        print('Keys:', list(data.keys()))
        hist = data.get('club_daily_history')
        friend_hist = data.get('club_friend_history')
        print('History length:', len(hist) if hist else 0)
        print('Friend History length:', len(friend_hist) if friend_hist else 0)
        if hist:
            print('History Sample:', hist[:2])
        if friend_hist:
            print('Friend History Sample:', friend_hist[:2])

if __name__ == '__main__':
    asyncio.run(main())
