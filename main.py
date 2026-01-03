import asyncio
import json
import os
import sys
import re
import pandas as pd
import zendriver as zd
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# Import globals
try:
    from globals import CLUBS, SHEET_ID
except ImportError:
    print("❌ Error: 'globals.py' not found!")
    sys.exit(1)

# ========== Google Sheets config ==========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_FILE = "credentials.json"
if not os.path.exists(CREDS_FILE):
    print(f"❌ Error: '{CREDS_FILE}' not found!")
    sys.exit(1)

CREDS = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
GC = gspread.authorize(CREDS)

# === DATA FETCHING METHOD (Optimized & English) ===
async def fetch_club_data_browser(club_cfg: dict):
    MAX_RETRIES = 3
    SEARCH_TERM = club_cfg["SEARCH_TERM"]
    CLUB_ID_STARTING = club_cfg["CLUB_ID_STARTING"]
    
    API_REGEX = re.compile(
        rf".*/api/club_profile\?circle_id={re.escape(CLUB_ID_STARTING)}.*", re.IGNORECASE
    )

    brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
    if not os.path.exists(brave_path):
        brave_path = "C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe"

    for attempt in range(MAX_RETRIES):
        browser = None
        captured_ids = []

        async def resp_handler(e: zd.cdp.network.ResponseReceived):
            if API_REGEX.match(e.response.url):
                captured_ids.append(e.request_id)

        try:
            browser = await zd.start(
                browser="chrome",
                browser_executable_path=brave_path,
                headless=False,
                arguments=["--mute-audio"]
            )

            page = await browser.get("https://chronogenesis.net/")
            
            # 1. Click Menu "Club Profile"
            menu_items = await page.select_all(".home-menu-item", timeout=15)
            if len(menu_items) > 1:
                await menu_items[1].click()
            else:
                raise Exception("Club Profile menu not found")
            
            await asyncio.sleep(1)

            # 2. Add Network Listener
            page.add_handler(zd.cdp.network.ResponseReceived, resp_handler)

            # 3. Input Search
            search_box = await page.select(".club-id-input", timeout=20)
            await search_box.send_keys(SEARCH_TERM)
            await search_box.send_keys(zd.SpecialKeys.ENTER)
            await asyncio.sleep(1.5)

            # 4. Click Result (Logic: Search Term check in string representation)
            try:
                results = await page.select_all(".club-results-row", timeout=5)
                
                target_clicked = False
                for row in results:
                    # Logic: convert row to string and check search term
                    if SEARCH_TERM in str(row):
                        await row.click()
                        target_clicked = True
                        break
                
                # Fallback: Click first result if specific match fails
                if not target_clicked and results:
                    await results[0].click()

            except Exception:
                pass

            # 5. Wait for Data
            await asyncio.sleep(4)

            if not captured_ids:
                raise Exception("Network Response not captured (API not called or wrong ID)")

            # 6. Get Largest Body
            largest_body = None
            max_size = 0
            for rid in captured_ids:
                try:
                    body, _ = await page.send(zd.cdp.network.get_response_body(request_id=rid))
                    content = body.decode('utf-8', errors='ignore') if isinstance(body, (bytes, bytearray)) else str(body)
                    if len(content) > max_size:
                        max_size = len(content)
                        largest_body = content
                except:
                    continue

            if not largest_body:
                raise Exception("Failed to retrieve JSON body from response")

            return json.loads(largest_body)

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise e
            print(f"⚠️ Retry {attempt+1}/{MAX_RETRIES} for '{club_cfg['title']}': {e}")
            await asyncio.sleep(2)
        finally:
            if browser:
                await browser.stop()

# === DataFrame Processing (From your code) ===
def build_dataframe(data: dict) -> pd.DataFrame:
    df = pd.json_normalize(data.get("club_friend_history") or [])
    if df.empty: return pd.DataFrame()

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

    # Keep only members present on the newest day
    nums = [n for n in map(_day_num, day_cols) if n is not None]
    if nums:
        latest_day = max(nums)
        latest_col = f"Day {latest_day}"
        if latest_col in df.columns:
            df = df[~df[latest_col].isna()].copy()

    # Sort columns
    day_cols = sorted(day_cols, key=lambda c: (_day_num(c) if _day_num(c) is not None else float("inf")))

    # Avg/d
    df["AVG/d"] = df[day_cols].mean(axis=1).round(0) if day_cols else 0
    df = df[["friend_viewer_id", "friend_name", "AVG/d"] + day_cols].rename(
        columns={"friend_viewer_id": "Member_ID", "friend_name": "Member_Name"}
    )
    
    df["Member_ID"] = df["Member_ID"].fillna("").astype(str)
    df["Member_Name"] = df["Member_Name"].fillna("").astype(str)
    
    # Numeric conversion
    for c in df.columns:
        if c not in ("Member_ID", "Member_Name"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.sort_values(["AVG/d", "Member_Name"], ascending=[False, True]).reset_index(drop=True)

# === Export to GSheets (EXACT LOGIC FROM YOUR CODE) ===
def export_to_gsheets(df: pd.DataFrame, spreadsheet_id: str, sheet_title: str, threshold: int):
    # ====== PREP DATA ======
    GAP_COL = " "
    dcols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
    df_to_write = df.copy()

    # Add Total and a blue gap column before it
    if dcols:
        df_to_write["Total"] = df_to_write[dcols].sum(axis=1, min_count=1)
        gidx = df_to_write.columns.get_loc("Total")
        df_to_write.insert(gidx, GAP_COL, "")
    else:
        gidx = None

    # Bottom "Total" row (sum)
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

    # Day AVG row — per-day means only (no AVG/d)
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

    # Values order: header, data..., Total, Day AVG
    values = [header] + data_rows + [totals_row, day_avg_row]

    # ====== OPEN SHEET ======
    ss = GC.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(sheet_title)
        ss.del_worksheet(ws)
    except gspread.WorksheetNotFound:
        pass
        
    ws = ss.add_worksheet(title=sheet_title, rows=max(len(values) + 50, 120), cols=max(len(header) + 10, 26))

    # Write values
    end_row = len(values)
    end_col = len(header)
    end_a1 = rowcol_to_a1(end_row, end_col)
    ws.update(values, f"A1:{end_a1}")

    # ====== FORMATTING ======
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

# === Batch Engine ===
async def process_batch(batch_configs):
    tasks = [fetch_club_data_browser(cfg) for cfg in batch_configs]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for i, data in enumerate(results):
        cfg = batch_configs[i]
        if isinstance(data, Exception):
            print(f"❌ Failed: {cfg['title']} | Error: {data}")
            continue
        try:
            df = build_dataframe(data)
            export_to_gsheets(df, SHEET_ID, cfg['title'], cfg['THRESHOLD'])
            print(f"✅ Success: {cfg['title']}")
        except Exception as e:
            print(f"❌ Processing Error: {cfg['title']} | {e}")

async def main():
    print("=== CHRONOGENESIS BATCH EXPORTER ===")
    print("0. Export ALL (Batch 5)")
    for k, v in CLUBS.items():
        print(f"{k}. {v['title']}")
    
    choice = input("\nSelect option: ").strip() or "0"
    
    if choice == "0":
        all_cfgs = list(CLUBS.values())
        for i in range(0, len(all_cfgs), 5):
            batch = all_cfgs[i : i + 5]
            print(f"\n🚀 Running Batch {i//5 + 1}...")
            await process_batch(batch)
    elif choice in CLUBS:
        await process_batch([CLUBS[choice]])
    
    print("\n🎉 COMPLETED!")

if __name__ == "__main__":
    asyncio.run(main())
    input("Press Enter to exit...")