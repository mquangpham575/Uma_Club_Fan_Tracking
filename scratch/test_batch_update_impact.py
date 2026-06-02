import os
import sys
import asyncio
import json
import gspread
from gspread.utils import rowcol_to_a1

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from config.globals import CLUBS, SHEET_ID
from src.chrono_scraper import scrape_club_data
from src.processing import build_dataframe
from src.sheets import get_gspread_client, get_conditional_format_rules_count

async def main():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ss = GC.open_by_key(SHEET_ID)
    
    cfg = CLUBS['1'].copy()
    cfg['sdate'] = "2026-05-01"
    
    print("Scraping...")
    text, code = await scrape_club_data(cfg)
    data = json.loads(text)
    df = build_dataframe(data)
    
    # Simulating export_to_gsheets logic step-by-step
    sheet_title = "ENDER (SS)"
    ws = ss.worksheet(sheet_title)
    
    print("Clearing and resizing...")
    ws.clear()
    
    # We construct header and values
    header = list(map(str, df.columns))
    data_rows = df.where(pd.notna(df), "").values.tolist() if 'pd' in globals() else df.values.tolist()
    
    # Just a simple values list for test
    values = [header] + data_rows
    
    end_row = len(values)
    end_col = len(header)
    end_a1 = rowcol_to_a1(end_row, end_col)
    
    print(f"Updating sheet with {end_row} rows...")
    ws.update(values, f"A1:{end_a1}")
    
    print("Values immediately after ws.update:")
    vals_after_update = ws.get_all_values()
    print(f"Row count: {len(vals_after_update)}")
    
    # Now simulate the batch update
    sheet_id = int(ws.id)
    num_cf_rules = get_conditional_format_rules_count(GC, SHEET_ID, sheet_title)
    
    requests = [
        {
            "unmergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 200,
                    "startColumnIndex": 0,
                    "endColumnIndex": 50
                }
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 200,
                    "startColumnIndex": 0,
                    "endColumnIndex": 50
                },
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat"
            }
        }
    ]
    for _ in range(num_cf_rules):
        requests.append({
            "deleteConditionalFormatRule": {
                "index": 0,
                "sheetId": sheet_id
            }
        })
        
    print("Running batch update...")
    ws.spreadsheet.batch_update({"requests": requests})
    
    print("Values after batch update:")
    vals_after_batch = ws.get_all_values()
    print(f"Row count: {len(vals_after_batch)}")

if __name__ == '__main__':
    import pandas as pd
    asyncio.run(main())
