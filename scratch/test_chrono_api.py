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
    # Test May
    cfg_may = CLUBS['1'].copy()
    cfg_may['sdate'] = "2026-05-01"
    print("Testing Chrono API with sdate 2026-05-01...")
    text_may, code_may = await scrape_club_data(cfg_may)
    if code_may == 200:
        data_may = json.loads(text_may)
        history_may = data_may.get("club_friend_history", [])
        print("  May club_friend_history length:", len(history_may))
        if history_may:
            print("  May sample friend entry keys:", list(history_may[0].keys()))
            print("  May sample entry:", history_may[0])
            
    # Test June
    cfg_june = CLUBS['1'].copy()
    cfg_june['sdate'] = "2026-06-01"
    print("\nTesting Chrono API with sdate 2026-06-01...")
    text_june, code_june = await scrape_club_data(cfg_june)
    if code_june == 200:
        data_june = json.loads(text_june)
        history_june = data_june.get("club_friend_history", [])
        print("  June club_friend_history length:", len(history_june))
        if history_june:
            print("  June sample friend entry keys:", list(history_june[0].keys()))
            print("  June sample entry:", history_june[0])

if __name__ == '__main__':
    asyncio.run(main())
