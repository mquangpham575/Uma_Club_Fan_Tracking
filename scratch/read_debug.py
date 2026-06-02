import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import SHEET_ID
from src.sheets import get_gspread_client

def check_sheet(ss, title):
    try:
        ws = ss.worksheet(title)
        all_val = ws.get_all_values()
        print(f"\n{title} values (rows: {len(all_val)}):")
        if all_val:
            for idx in range(min(5, len(all_val))):
                print(f"Row {idx+1}: {all_val[idx]}")
    except Exception as e:
        print(f"Error reading {title}: {e}")

def main():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ss = GC.open_by_key(SHEET_ID)
    check_sheet(ss, "All Club Data")
    check_sheet(ss, "ENDCORE (SS)")
    check_sheet(ss, "ENDER (SS)")

if __name__ == '__main__':
    main()
