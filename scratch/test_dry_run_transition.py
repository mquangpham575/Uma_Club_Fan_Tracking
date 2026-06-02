import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timezone, timedelta
from config.globals import SHEET_ID, CLUBS
from src.sheets import get_gspread_client

def simulate_check():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ss = GC.open_by_key(SHEET_ID)
    summary_ws = ss.worksheet("All Club Data")
    first_row = summary_ws.row_values(1)
    print(f"A1 Cell Content: {first_row[0] if first_row else 'Empty'}")
    
    # 1. Simulating BEFORE reset (e.g., June 2nd, 09:00 UTC) -> Target Date: May 31st
    print("\n--- Simulation 1: Before Reset (June 2nd, 09:00 UTC) ---")
    now_utc = datetime(2026, 6, 2, 9, 0, 0, tzinfo=timezone.utc)
    reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
    target_date = now_utc - timedelta(days=1 if now_utc >= reset_time else 2)
    target_col_name = f"Day {target_date.day}"
    expected_month_str = target_date.strftime("%B %Y").upper()
    print(f"Target Date: {target_date.strftime('%Y-%m-%d')}, Expected Month: {expected_month_str}, Target Col: {target_col_name}")
    if not first_row or expected_month_str not in first_row[0]:
        print(f"Result: --- Month transition detected ({expected_month_str}). Proceeding with update... ---")
    else:
        first_club_title = list(CLUBS.values())[0]['title']
        ws = ss.worksheet(first_club_title)
        headers = ws.row_values(1)
        if target_col_name in headers:
            print(f"Result: --- Skip: Sheet is already up to date with {target_col_name} ---")
        else:
            print("Result: --- Proceeding with update: Day col not found ---")

    # 2. Simulating AFTER reset (e.g., June 2nd, 11:00 UTC) -> Target Date: June 1st
    print("\n--- Simulation 2: After Reset (June 2nd, 11:00 UTC) ---")
    now_utc = datetime(2026, 6, 2, 11, 0, 0, tzinfo=timezone.utc)
    reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
    target_date = now_utc - timedelta(days=1 if now_utc >= reset_time else 2)
    target_col_name = f"Day {target_date.day}"
    expected_month_str = target_date.strftime("%B %Y").upper()
    print(f"Target Date: {target_date.strftime('%Y-%m-%d')}, Expected Month: {expected_month_str}, Target Col: {target_col_name}")
    if not first_row or expected_month_str not in first_row[0]:
        print(f"Result: --- Month transition detected ({expected_month_str}). Proceeding with update... ---")
    else:
        first_club_title = list(CLUBS.values())[0]['title']
        ws = ss.worksheet(first_club_title)
        headers = ws.row_values(1)
        if target_col_name in headers:
            print(f"Result: --- Skip: Sheet is already up to date with {target_col_name} ---")
        else:
            print("Result: --- Proceeding with update: Day col not found ---")

if __name__ == '__main__':
    simulate_check()
