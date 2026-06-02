import os
import sys
import asyncio
import json

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import CLUBS
from src.chrono_scraper import scrape_club_data
from src.processing import build_dataframe

async def main():
    cfg = CLUBS['1'].copy()
    cfg['sdate'] = "2026-05-01"
    print("Scraping...")
    text, code = await scrape_club_data(cfg)
    if code == 200:
        data = json.loads(text)
        df = build_dataframe(data)
        print("DataFrame shape:", df.shape)
        print("DataFrame columns:", list(df.columns))
        print("First few rows of DataFrame:\n", df.head())
        
        # Check values built in sheets.py export_to_gsheets logic
        dcols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
        print("Day columns:", dcols)
        print("Data rows length:", len(df))

if __name__ == '__main__':
    asyncio.run(main())
