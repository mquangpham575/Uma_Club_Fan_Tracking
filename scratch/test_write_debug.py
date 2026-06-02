import sys
import os
import traceback
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
from config.globals import CLUBS, SHEET_ID
from src.chrono_scraper import scrape_club_data
from src.processing import build_dataframe
from src.sheets import get_gspread_client, export_to_gsheets

async def main():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    cfg = CLUBS['1'].copy()
    cfg['sdate'] = "2026-05-01"
    
    print("Scraping ENDER...")
    text, code = await scrape_club_data(cfg)
    if code != 200:
        print("Scrape failed")
        return
        
    data = json.loads(text)
    df = build_dataframe(data)
    
    print("Fetching green members...")
    from src.sheets import get_green_members
    club_titles = [CLUBS[k]['title'] for k in CLUBS]
    green_members = get_green_members(GC, SHEET_ID, club_titles)
    print(f"Found {len(green_members)} green members: {green_members}")
    
    print("Attempting to write to Google Sheets...")
    try:
        export_to_gsheets(
            GC, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
            data.get("club_daily_history"),
            green_members=green_members
        )
        print("Success!")
    except Exception as e:
        print("Failed with exception:", e)
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
