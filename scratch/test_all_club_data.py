import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
from config.globals import CLUBS, SHEET_ID
from src.chrono_scraper import scrape_club_data
from src.processing import build_dataframe
from src.sheets import get_gspread_client, export_all_club_data_to_gsheets, get_green_members

async def main():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    
    # Fetch data for key '1' and '2' (ENDER and ENDCORE)
    successful_clubs = []
    for key in ['1', '2']:
        cfg = CLUBS[key].copy()
        cfg['sdate'] = "2026-05-01"
        title = cfg["title"]
        print(f"Fetching {title}...")
        text, code = await scrape_club_data(cfg)
        if code == 200:
            data = json.loads(text)
            df = build_dataframe(data)
            
            day_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
            member_data = []
            for _, row in df.iterrows():
                perf = row[day_cols].sum() if day_cols else 0.0
                member_data.append({
                    "member_name": row["Member_Name"],
                    "avg_day": row["AVG/d"],
                    "performance": perf
                })
                
            if "(" in title and ")" in title:
                short_name = title.split("(")[0].strip()
                grade = title.split("(")[1].split(")")[0].strip()
            else:
                short_name = title
                grade = ""
                
            rank = ""
            daily_history = data.get("club_daily_history") or []
            if daily_history:
                try:
                    latest_entry = max(daily_history, key=lambda x: int(x.get("actual_date", 0)))
                    rank_val = latest_entry.get("rank")
                    if rank_val is not None:
                        rank = f"#{rank_val}"
                except Exception:
                    rank_val = daily_history[-1].get("rank")
                    if rank_val is not None:
                        rank = f"#{rank_val}"
                        
            club_metadata = {
                "short_name": short_name,
                "grade": grade,
                "rank": rank,
                "members": member_data
            }
            successful_clubs.append(club_metadata)
            print(f"Successfully processed {title} with {len(member_data)} members.")
            
    if successful_clubs:
        print("Fetching green members from worksheets...")
        green_members = get_green_members(GC, SHEET_ID, [CLUBS[k]['title'] for k in CLUBS])
        print(f"Found {len(green_members)} green members: {green_members}")
        
        print("Writing to summary sheet...")
        export_all_club_data_to_gsheets(GC, SHEET_ID, successful_clubs, sdate="2026-05-01", green_members=green_members)
        print("Summary sheet updated successfully!")
    else:
        print("No successful clubs to write.")

if __name__ == '__main__':
    asyncio.run(main())
