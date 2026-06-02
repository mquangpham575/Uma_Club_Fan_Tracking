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
    ws = ss.worksheet("ENDER (SS)")
    print("ENDER columns:", ws.row_values(1))
    print("ENDER row 2:", ws.row_values(2))
    print("ENDER row 3:", ws.row_values(3))

if __name__ == '__main__':
    check()
