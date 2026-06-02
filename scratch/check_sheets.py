import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import SHEET_ID
from src.sheets import get_gspread_client

def check():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ss = GC.open_by_key(SHEET_ID)
    worksheets = ss.worksheets()
    print("Worksheets:", [ws.title for ws in worksheets])
    try:
        ws = ss.worksheet("All Club Data")
        print("All Club Data row 1:", ws.row_values(1))
        print("All Club Data row 2:", ws.row_values(2))
        print("All Club Data row 3:", ws.row_values(3))
        print("All Club Data row 4:", ws.row_values(4))
    except Exception as e:
        print("Error checking All Club Data:", e)

if __name__ == '__main__':
    check()
