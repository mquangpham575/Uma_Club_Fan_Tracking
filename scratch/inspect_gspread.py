import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import SHEET_ID
from src.sheets import get_gspread_client

def inspect_multi_ranges():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ss = GC.open_by_key(SHEET_ID)
    
    worksheets = ss.worksheets()
    titles = [ws.title for ws in worksheets[:10]] # Fetch first 10 sheets
    print("Fetching column B for sheets:", titles)
    
    import urllib.parse
    ranges_list = []
    for t in titles:
        q_range = urllib.parse.quote(f"'{t}'!B1:B100")
        ranges_list.append(f"ranges={q_range}")
    ranges_str = "&".join(ranges_list)
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}?{ranges_str}&includeGridData=true"
    
    res_obj = GC.http_client.session.get(url)
    res = res_obj.json()
    
    sheets = res.get('sheets', [])
    print("Sheets returned:", len(sheets))
    for s in sheets:
        title = s.get('properties', {}).get('title')
        data = s.get('data', [])
        green_names = []
        all_colors = []
        if data:
            row_data = data[0].get('rowData', [])
            for r_idx, r in enumerate(row_data):
                values = r.get('values', [])
                if values:
                    val = values[0]
                    name = val.get('formattedValue')
                    bg = val.get('userEnteredFormat', {}).get('backgroundColor', {})
                    if bg:
                        all_colors.append((name, bg))
                    # Green color check
                    r_val = bg.get('red', 1.0)
                    g_val = bg.get('green', 1.0)
                    b_val = bg.get('blue', 1.0)
                    if bg and g_val > r_val and g_val > b_val and g_val < 0.9:
                        green_names.append(name)
        print(f"Sheet '{title}' green names: {green_names}")
        if all_colors:
            print(f"  First 3 colors in '{title}': {all_colors[:3]}")

if __name__ == '__main__':
    inspect_multi_ranges()
