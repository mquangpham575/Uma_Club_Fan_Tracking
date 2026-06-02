import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import pandas as pd
from config.globals import CLUBS
from src.chrono_scraper import scrape_club_data
from src.processing import build_dataframe

async def main():
    cfg = CLUBS['1'].copy()
    cfg['sdate'] = "2026-05-01"
    text, code = await scrape_club_data(cfg)
    if code != 200:
        print("Failed to fetch")
        return
        
    data = json.loads(text)
    df = build_dataframe(data)
    
    # 1. Member stats
    # columns are like: Member_ID, Member_Name, AVG/d, Day 1, Day 2, ...
    day_cols = [c for c in df.columns if c.startswith("Day ")]
    print("Number of day columns:", len(day_cols))
    
    # Calculate member total performance
    df["Performance"] = df[day_cols].sum(axis=1)
    
    print("\nMember averages and performances (top 5):")
    for idx, row in df.head(5).iterrows():
        print(f"Name: {row['Member_Name']}, AVG/d: {row['AVG/d']}, Performance: {row['Performance']}")
        
    # 2. Club stats
    # From daily history:
    daily_hist = data.get("club_daily_history", [])
    if daily_hist:
        gains = [entry.get("interpolated_fan_gain", 0) for entry in daily_hist]
        avg_gain = sum(gains) / len(gains) if gains else 0
        total_gain = sum(gains)
        print(f"\nClub Daily History - Total Gain: {total_gain:,}, Avg Gain: {avg_gain:,.1f}")
        
    # From member sum
    member_sum_avg = df["AVG/d"].sum()
    member_sum_perf = df["Performance"].sum()
    member_avg_per_player = df["AVG/d"].mean()
    print(f"\nMember Sums - Total Avg/d: {member_sum_avg:,}, Total Perf: {member_sum_perf:,}, Avg/player: {member_avg_per_player:,.1f}")

if __name__ == '__main__':
    asyncio.run(main())
