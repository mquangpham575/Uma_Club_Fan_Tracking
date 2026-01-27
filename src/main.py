import asyncio
import json
import os
import sys
import re
import time
from pathlib import Path
import pandas as pd
import zendriver as zd
import gspread
from google.oauth2.service_account import Credentials

# Imports and Globals
try:
    if getattr(sys, 'frozen', False):
        # If the application is run as a bundle, the PyInstaller bootloader
        # extends the sys module by a flag frozen=True and sets the app 
        # path into variable _MEIPASS'.
        base_path = sys._MEIPASS
    else:
        # If running purely as a script, file path is inside src/
        # We need the parent directory of src/ to find config/
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Add base_path to sys.path to ensure we can import config
    if base_path not in sys.path:
        sys.path.append(base_path)
        
    from config.globals import CLUBS, SHEET_ID
except ImportError as e:
    print(f"Error: 'globals.py' not found (Base path: {base_path}). Details: {e}")
    sys.exit(1)

# Google Sheets Configuration
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
try:
    # Construct path to credentials.json in config folder
    creds_path = os.path.join(base_path, 'config', 'credentials.json')
    CREDS = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    GC = gspread.authorize(CREDS)
except Exception as e:
    print(f"Config Error: {e}")
    sys.exit(1)


# Helper Functions
def pick_club() -> dict | str:
    print("Select Target Club:")
    print("-" * 30)
    club_keys = list(CLUBS.keys())
    for key in club_keys:
        print(f"[{key}] {CLUBS[key]['title']}")
    print("-" * 30)
    print("[0] Process All (Default)")
    
    choice = input("\nSelection: ").strip()
    
    if choice == "" or choice == "0":
        return "ALL"
    if choice in CLUBS:
        return CLUBS[choice]
    return CLUBS[list(CLUBS.keys())[0]]

# Core Scraping Logic
async def fetch_club_data_browser(club_cfg: dict):
    SEARCH_TERM = club_cfg["SEARCH_TERM"]
    CLUB_ID_STARTING = str(club_cfg["CLUB_ID_STARTING"])
    TITLE = club_cfg["title"]

    REGEX = re.compile(
        rf".*/api/club_profile\?circle_id={CLUB_ID_STARTING}.*", re.IGNORECASE
    )

    RESPONSES = [] 

    async def resp_handler(e: zd.cdp.network.ResponseReceived):
        if REGEX.match(e.response.url):
            RESPONSES.append(e.request_id)

    # Browser Path Setup
    brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
    if not os.path.exists(brave_path):
        brave_path = "C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe"
    
    # OPTIMIZATION
    browser_args = [
        "--mute-audio",
        "--disable-extensions",
        "--window-position=-3000,0",             
        "--disable-background-timer-throttling", 
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--no-first-run",
        "--no-default-browser-check"
    ]

    # Use headless=False so the site doesn't block us, but args hide it
    browser = await zd.start(
        browser="edge", 
        browser_executable_path=brave_path,
        headless=False, 
        arguments=browser_args
    )

    try:
        page = await browser.get("https://chronogenesis.net/")

        club_profile = await page.select_all(".home-menu-item")
        await club_profile[1].click()
        await asyncio.sleep(1)

        page.add_handler(zd.cdp.network.ResponseReceived, resp_handler)

        search_box = await page.select(".club-id-input", timeout=20)
        await search_box.send_keys(SEARCH_TERM)
        await search_box.send_keys(zd.SpecialKeys.ENTER)
        await asyncio.sleep(1)

        try:
            results = await page.select_all(".club-results-row", timeout=3)
            for result in results:
                if SEARCH_TERM in str(result):
                    await result.click()
                    break
        except:
            pass

        # Silent wait (Removed print)
        await asyncio.sleep(3)

        largest_response = None
        largest_size = 0

        if not RESPONSES:
            raise Exception("No API request matched.")

        for request_id in RESPONSES:
            try:
                response_body, _ = await page.send(
                    zd.cdp.network.get_response_body(request_id=request_id)
                )
                
                if isinstance(response_body, bytes) or isinstance(response_body, bytearray):
                      content = response_body.decode('utf-8', errors='replace')
                else:
                      content = str(response_body)

                size = len(content)
                if size > largest_size:
                    largest_size = size
                    largest_response = content
            except Exception:
                continue
        
        await browser.stop()
        
        if largest_response:
            return json.loads(largest_response)
        else:
            raise Exception("Empty response body.")

    except Exception as e:
        try:
            await browser.stop()
        except:
            pass
        raise e

def build_dataframe(data: dict) -> pd.DataFrame:
    df = pd.json_normalize(data.get("club_friend_history") or [])
    for c in ("friend_viewer_id", "friend_name", "actual_date", "adjusted_interpolated_fan_gain"):
        if c not in df.columns:
            df[c] = pd.NA

    df = (
        df.assign(day_col=lambda d: "Day " + d["actual_date"].astype(str))
            .pivot_table(
                index=["friend_viewer_id", "friend_name"],
                columns="day_col",
                values="adjusted_interpolated_fan_gain",
                aggfunc="first"
            )
            .reset_index()
    )
    df.columns.name = None

    def _day_num(x: str):
        if not isinstance(x, str) or not x.startswith("Day "):
            return None
        try:
            return int(x.split(maxsplit=1)[1])
        except Exception:
            return None

    day_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]

    nums = [n for n in map(_day_num, day_cols) if n is not None]
    if nums:
        latest_day = max(nums)
        latest_col = f"Day {latest_day}"
        if latest_col in df.columns:
            df = df[~df[latest_col].isna()].copy()

    day_cols = sorted(day_cols, key=lambda c: (_day_num(c) if _day_num(c) is not None else float("inf")))

    df["AVG/d"] = df[day_cols].mean(axis=1).round(0) if day_cols else 0
    df = df[["friend_viewer_id", "friend_name", "AVG/d"] + day_cols].rename(
        columns={"friend_viewer_id": "Member_ID", "friend_name": "Member_Name"}
    )
    df["Member_ID"] = df["Member_ID"].fillna("").astype(str)
    df["Member_Name"] = df["Member_Name"].fillna("").astype(str)
    for c in df.columns:
        if c not in ("Member_ID", "Member_Name"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(["AVG/d", "Member_Name"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    return df


# Google Sheets Export
def export_to_gsheets(df: pd.DataFrame, spreadsheet_id: str, sheet_title: str, threshold: int):
    from gspread.utils import rowcol_to_a1

    GAP_COL = " "
    dcols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
    df_to_write = df.copy()

    if dcols:
        df_to_write["Total"] = df_to_write[dcols].sum(axis=1, min_count=1)
        gidx = df_to_write.columns.get_loc("Total")
        df_to_write.insert(gidx, GAP_COL, "")
    else:
        gidx = None

    bottom_totals = {}
    for c in df_to_write.columns:
        if c == "Member_Name":
            bottom_totals[c] = "Total"
        elif c in ("Member_ID", GAP_COL):
            bottom_totals[c] = ""
        else:
            total = pd.to_numeric(df_to_write[c], errors="coerce").sum(min_count=1)
            if pd.isna(total):
                bottom_totals[c] = ""
            elif isinstance(total, float):
                bottom_totals[c] = total  
            elif hasattr(total, 'item'):
                bottom_totals[c] = total.item()
            else:
                bottom_totals[c] = total 

    day_avgs = pd.Series("", index=df_to_write.columns, dtype=object)
    if dcols:
        means = df_to_write[dcols].mean(axis=0, skipna=True).round(0)
        for c in dcols:
            day_avgs[c] = means.get(c, "")
    day_avgs["Member_Name"] = "Day AVG"

    header = list(map(str, df_to_write.columns))
    data_rows = df_to_write.where(pd.notna(df_to_write), "").values.tolist()
    totals_row = [("" if pd.isna(v) else v) for v in (bottom_totals.get(c, "") for c in df_to_write.columns)]
    day_avg_row = [day_avgs.get(c, "") for c in df_to_write.columns]

    values = [header] + data_rows + [totals_row, day_avg_row]

    ss = GC.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(sheet_title)
        ss.del_worksheet(ws)
    except gspread.WorksheetNotFound:
        pass
        
    ws = ss.add_worksheet(title=sheet_title, rows=max(len(values) + 50, 120), cols=max(len(header) + 10, 26))

    end_row = len(values)
    end_col = len(header)
    end_a1 = rowcol_to_a1(end_row, end_col)
    ws.update(values, f"A1:{end_a1}")

    sheet_id = ws._properties["sheetId"]
    last_data_row_1based = 1 + len(data_rows) 

    header_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": end_col}
    totals_range = {"sheetId": sheet_id, "startRowIndex": end_row - 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}
    header_plus_data_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": end_col}
    band_left = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": (gidx if gidx is not None else end_col)}
    band_right = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based,
                  "startColumnIndex": (gidx + 1 if gidx is not None else end_col), "endColumnIndex": end_col}
    full_table_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}

    def col_1_based(col_name: str) -> int | None:
        try:
            return header.index(col_name) + 1
        except ValueError:
            return None

    skip_for_number = {"Member_ID", "Member_Name", GAP_COL}
    numeric_cols_1 = [i + 1 for i, c in enumerate(header) if c not in skip_for_number]

    day_cols_1 = [col_1_based(c) for c in dcols]
    day_cols_1 = [c1 for c1 in day_cols_1 if c1 is not None]

    def col_range_rows(start_row_1, end_row_1, col_1):
        return {"sheetId": sheet_id, "startRowIndex": start_row_1 - 1, "endRowIndex": end_row_1,
                "startColumnIndex": col_1 - 1, "endColumnIndex": col_1}

    numeric_ranges_all = [col_range_rows(2, end_row, c1) for c1 in numeric_cols_1]
    numeric_ranges_data_days = [col_range_rows(2, last_data_row_1based, c1) for c1 in day_cols_1]

    avgd_col_1 = col_1_based("AVG/d")
    numeric_ranges_data = list(numeric_ranges_data_days)
    if avgd_col_1 is not None:
        numeric_ranges_data.append(col_range_rows(2, last_data_row_1based, avgd_col_1))

    blue_fill  = {"red": 0.31, "green": 0.51, "blue": 0.74}
    white_font = {"red": 1, "green": 1, "blue": 1}
    red_fill   = {"red": 1.00, "green": 0.78, "blue": 0.81}
    grey_fill  = {"red": 0.75, "green": 0.75, "blue": 0.75}
    band_light = {"red": 0.86, "green": 0.92, "blue": 0.97}
    band_very  = {"red": 0.95, "green": 0.97, "blue": 0.98}
    number_format = {"type": "NUMBER", "pattern": "#,##0"}

    requests = [
        {"setBasicFilter": {"filter": {"range": header_plus_data_range}}},
        {
            "repeatCell": {
                "range": header_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": blue_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        {
            "repeatCell": {
                "range": totals_range,
                "cell": {"userEnteredFormat": {"backgroundColor": blue_fill, "textFormat": {"bold": True, "foregroundColor": white_font}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        },
        *([
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": gidx, "endColumnIndex": gidx + 1},
                    "cell": {"userEnteredFormat": {"backgroundColor": blue_fill}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": gidx, "endIndex": gidx + 1},
                    "properties": {"pixelSize": 40},
                    "fields": "pixelSize"
                }
            }
        ] if gidx is not None else []),
        *([
            {"addBanding": {"bandedRange": {"range": band_left,  "rowProperties": {"firstBandColor": band_light, "secondBandColor": band_very}}}}
        ] if gidx is None or gidx > 0 else []),
        *([
            {"addBanding": {"bandedRange": {"range": band_right, "rowProperties": {"firstBandColor": band_light, "secondBandColor": band_very}}}}
        ] if gidx is not None and gidx + 1 < end_col else []),
        *[
            {"repeatCell": {"range": r, "cell": {"userEnteredFormat": {"numberFormat": number_format}}, "fields": "userEnteredFormat.numberFormat"}}
            for r in numeric_ranges_all
        ],
        *([{
            "addConditionalFormatRule": {
                "rule": {"ranges": numeric_ranges_data,
                         "booleanRule": {"condition": {"type": "NUMBER_LESS",
                                                       "values": [{"userEnteredValue": str(threshold)}]},
                                     "format": {"backgroundColor": red_fill}}},
                "index": 0
            }
        }] if numeric_ranges_data else []),
        *([{
            "addConditionalFormatRule": {
                "rule": {"ranges": numeric_ranges_data_days,
                         "booleanRule": {"condition": {"type": "BLANK"},
                                     "format": {"backgroundColor": grey_fill}}},
                "index": 0
            }
        }] if numeric_ranges_data_days else []),
        {
            "updateBorders": {
                "range": full_table_range,
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        },
    ]

    if "Member_Name" in header:
        name_col_index = header.index("Member_Name")
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": name_col_index, "endIndex": name_col_index + 1},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }
        })

    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    ws.spreadsheet.batch_update({"requests": requests})

# Main Execution
async def process_and_export_club(cfg: dict, pre_fetched_data=None):
    data = await fetch_club_data_browser(cfg) if pre_fetched_data is None else pre_fetched_data
    if isinstance(data, Exception): raise data
    export_to_gsheets(build_dataframe(data), SHEET_ID, cfg['title'], cfg["THRESHOLD"])
    return True

async def main():
    choice = pick_club()
    BATCH_SIZE = 5
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}
    club_keys = list(clubs_to_process.keys())
    batches = [club_keys[i:i + BATCH_SIZE] for i in range(0, len(club_keys), BATCH_SIZE)]

    print(f"\nProcessing {len(club_keys)} clubs...\n")
    
    failed_clubs = []
    
    for batch_idx, batch_keys in enumerate(batches):
        print(f"Batch {batch_idx + 1}/{len(batches)}: Processing {len(batch_keys)} items...")
        
        # Parallel Fetch
        tasks = {key: asyncio.create_task(fetch_club_data_browser(CLUBS[key])) for key in batch_keys}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        results_map = dict(zip(batch_keys, results))

        # Sequential Export
        for key in batch_keys:
            cfg, result = CLUBS[key], results_map[key]
            title = cfg["title"]
            success = False
            
            for attempt in range(MAX_RETRIES):
                try:
                    data = result if (attempt == 0 and not isinstance(result, Exception)) else None
                    
                    if attempt > 0: 
                        print(f"  Retrying: {title} ({attempt})...", end="\r")
                        await asyncio.sleep(RETRY_DELAY)
                    
                    await process_and_export_club(cfg, pre_fetched_data=data)
                    print(f"  Success: {title}")
                    success = True
                    break
                except Exception as e:
                    # Clean error log, only show detail if needed or critical
                    result = None # Force re-fetch next loop
            
            if not success:
                print(f"  Failed: {title}")
                failed_clubs.append(title)
        
        print("") # Spacer between batches

    print("-" * 30)
    if failed_clubs:
        print(f"Completed with errors: {len(failed_clubs)} failed.")
    else:
        print("All operations complete.")
    print("-" * 30)
    input("Press Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())