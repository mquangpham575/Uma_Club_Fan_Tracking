import asyncio
import json
import os
import sys
from pathlib import Path
import pandas as pd
import zendriver as zd
import gspread
from google.oauth2.service_account import Credentials
import time
from globals import CLUBS, SHEET_ID


# ========== Google Sheets config ==========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
GC = gspread.authorize(CREDS)


# === Club selection ===
def pick_club() -> dict | str:
    print("=== Choose a club to export ===")
    for key, cfg in CLUBS.items():
        print(f"{key}. {cfg['title']}")
    print("0. Export ALL clubs (default)")
    choice = input("Enter Choice [default=0]: ").strip()
    if choice == "" or choice == "0":
        return "ALL"
    if choice not in CLUBS:
        print("Invalid choice, defaulting to 1.")
        choice = "1"
    return CLUBS[choice]


# === Paths ===
def resolve_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).parent


# === Data fetch (UPDATED WITH BRAVE BROWSER LOGIC) ===
async def fetch_json(URL: str):
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEOUT_SECONDS = 20  
    
    brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
    if not os.path.exists(brave_path):
        brave_path = "C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe"

    if not os.path.exists(brave_path):
        raise FileNotFoundError(f"Không tìm thấy Brave browser tại cả 2 đường dẫn mặc định.")

    for attempt in range(MAX_RETRIES):
        browser = None
        
        async def _run_browser_session():
            nonlocal browser 
            
            browser = await zd.start(
                browser="chrome",  
                browser_executable_path=brave_path,
                headless=False,     
                arguments=["--mute-audio"] 
            )
            
            page = await browser.get("https://google.com")
            
            async with page.expect_request(r".*\/api\/club_profile.*") as req:
                await page.get(URL) # Truy cập URL của Club
                
                await req.value
                body, _ = await req.response_body
            
            text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
            return json.loads(text)

        try:
            return await asyncio.wait_for(_run_browser_session(), timeout=TIMEOUT_SECONDS)
            
        except asyncio.TimeoutError:
            print(f"⚠️ Attempt {attempt + 1}: Timed out (> {TIMEOUT_SECONDS}s). Closing Brave and retrying...")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            else:
                raise Exception(f"Timeout exceeded ({TIMEOUT_SECONDS}s) on all {MAX_RETRIES} attempts.")

        except (zd.errors.RemoteDisconnectedError, zd.errors.ConnectionAbortedError) as e:
            print(f"⚠️ Attempt {attempt + 1}: Connection error ({e}). Retrying...")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            else:
                raise
                
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise e
            print(f"⚠️ Attempt {attempt + 1}: Unexpected error: {e}")
            await asyncio.sleep(RETRY_DELAY)
            
        finally:
            if browser:
                try:
                    await browser.stop()
                except Exception:
                    pass
    
    raise Exception(f"Thất bại sau {MAX_RETRIES} lần thử.")


# === DataFrame processing ===
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

    # --- Keep only members who have value on the newest day (max Day N) ---
    nums = [n for n in map(_day_num, day_cols) if n is not None]
    if nums:
        latest_day = max(nums)
        latest_col = f"Day {latest_day}"
        if latest_col in df.columns:
            df = df[~df[latest_col].isna()].copy()

    # Order Day columns numerically
    day_cols = sorted(day_cols, key=lambda c: (_day_num(c) if _day_num(c) is not None else float("inf")))

    # Compute AVG/d and finalize columns
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


# === Google Sheets export ===
def export_to_gsheets(df: pd.DataFrame, spreadsheet_id: str, sheet_title: str, threshold: int):
    from gspread.utils import rowcol_to_a1

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
    for ws in ss.worksheets():
        if ws.title == sheet_title:
            ss.del_worksheet(ws)
            break
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

# === Core Export Logic (Single Club) ===
async def process_and_export_club(cfg: dict, data_or_task_result=None):
    title = cfg['title']
    
    if isinstance(data_or_task_result, Exception) or data_or_task_result is None:
        data = await fetch_json(cfg["URL"]) 
    else:
        data = data_or_task_result

    df = build_dataframe(data)
    export_to_gsheets(df, spreadsheet_id=SHEET_ID, sheet_title=title, threshold=cfg["THRESHOLD"])
    return True

# === Logic for batch processing all clubs (UPDATED) ===
async def process_all_clubs_in_batches(all_clubs: dict):
    MAX_CLUB_RETRIES = 3 
    CLUB_RETRY_DELAY = 5
    BATCH_SIZE = 5
    
    club_keys = list(all_clubs.keys())
    batches = []

    # Dynamic batch creation: chunks of 5
    for i in range(0, len(club_keys), BATCH_SIZE):
        batch_keys = club_keys[i : i + BATCH_SIZE]
        start_idx = i + 1
        end_idx = i + len(batch_keys)
        batches.append({
            "name": f"(Club {start_idx}-{end_idx})",
            "keys": batch_keys
        })

    clubs_failed_total = []
    print("\n⚡ Exporting ALL clubs: Concurrent data fetching, Sequential processing/Export with retry in batches...\n")

    for i, batch in enumerate(batches):
        print(f"\n==================================================")
        print(f"⚡ BATCH {i+1}/{len(batches)} {batch['name']}")
        print(f"==================================================")
        
        batch_clubs = {k: all_clubs[k] for k in batch['keys']}

        if not batch_clubs:
            continue

        # 1. Concurrent data fetching for the current batch
        fetch_tasks = {
            key: asyncio.create_task(fetch_json(cfg["URL"])) 
            for key, cfg in batch_clubs.items()
        }
        
        results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
        results_map = {key: results[i] for i, key in enumerate(batch_clubs.keys())}
        
        # 2. Sequential Processing and Export with In-Place Retry
        clubs_failed_batch = []
        
        for key, cfg in batch_clubs.items():
            title = cfg["title"]
            initial_result = results_map[key]
            
            for attempt in range(MAX_CLUB_RETRIES):
                if attempt > 0:
                    print(f"\n⚡ Retrying club {title} (Attempt {attempt + 1}/{MAX_CLUB_RETRIES}) after waiting {CLUB_RETRY_DELAY}s...")
                    await asyncio.sleep(CLUB_RETRY_DELAY)
                
                try:
                    data_to_use = initial_result if attempt == 0 and not isinstance(initial_result, Exception) else None
                    
                    await process_and_export_club(cfg, data_or_task_result=data_to_use)
                    
                    if attempt == 0:
                          print(f"✅ {title} exported successfully.")
                    else:
                          print(f"✅ {title} exported successfully after {attempt} retry(ies).")
                    break
                        
                except Exception as e:
                    print(f"❌ {title} failed on attempt {attempt + 1}: {e}")
                    if attempt == MAX_CLUB_RETRIES - 1:
                        clubs_failed_batch.append(title)
        
        clubs_failed_total.extend(clubs_failed_batch)

    print("\n" + "="*50)
    if clubs_failed_total:
        print(f"⚠️ COMPLETED WITH ERRORS: {len(clubs_failed_total)} club(s) failed after {MAX_CLUB_RETRIES} attempts.")
        print("    List of failed clubs: " + ", ".join(clubs_failed_total))
    else:
        print("🎉 COMPLETED: All clubs were exported successfully in order across all batches!")
    print("="*50)
    return clubs_failed_total

# Updated main entry function for ALL/Single logic
async def main_updated():
    choice = pick_club()
    
    MAX_CLUB_RETRIES = 3
    CLUB_RETRY_DELAY = 5
    
    if choice == "ALL":
        await process_all_clubs_in_batches(CLUBS)

    else:
        cfg = choice
        print(f"\nSelected: {cfg['title']}\nURL: {cfg['URL']}\nSheet: {SHEET_ID}\nThreshold: {cfg['THRESHOLD']}\n")
        
        # Single club processing logic inline for simplicity
        title = cfg['title']
        for attempt in range(MAX_CLUB_RETRIES):
            if attempt > 0:
                print(f"\n⚡ Retrying full process for {title} (Attempt {attempt + 1}/{MAX_CLUB_RETRIES})...")
            
            try:
                await process_and_export_club(cfg, data_or_task_result=None)
                if attempt == 0:
                    print(f"✅ Exported single club '{title}' successfully.")
                else:
                    print(f"✅ Exported single club '{title}' successfully after {attempt} retry(ies).")
                break
                
            except Exception as e:
                print(f"❌ Club '{title}' failed on attempt {attempt + 1}: {e}")
                if attempt < MAX_CLUB_RETRIES - 1:
                    await asyncio.sleep(CLUB_RETRY_DELAY)
                else:
                    pass

if __name__ == "__main__":
    asyncio.run(main_updated())
    input("Press Enter to close terminal...")