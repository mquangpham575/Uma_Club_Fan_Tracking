import asyncio
import os
import sys
import random
import logging
import json
import calendar
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load local environment variables from .env if present
load_dotenv()

# Silence verbose browser logs
logging.getLogger("zendriver").setLevel(logging.WARNING)
logging.getLogger("uc").setLevel(logging.WARNING)

# Import Globals from OnlyRex
try:
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        # Script is inside OnlyRex/
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Add base_path to sys.path to ensure we can import src and OnlyRex
    if base_path not in sys.path:
        sys.path.append(base_path)
        
    from OnlyRex.globals import CLUBS, SHEET_ID, VERSION, CHRONO_API_KEY
except ImportError as e:
    print(f"Error: 'globals.py' not found (Base path: {base_path}). Details: {e}")
    sys.exit(1)

# Import Modules from src
from src.processing import build_dataframe
from src.sheets import export_to_gsheets, get_gspread_client, reorder_sheets
from src.utils import clear_screen, setup_windows_console, LogColor, colorize


# Global locks to prevent concurrent resource exhaustion
SHEETS_LOCK = asyncio.Lock()
API_SEMAPHORE = asyncio.Semaphore(1)

def parse_sheet_title(title: str):
    """
    Parses titles like "April 26 (A+)" or "June 26 (A+)".
    Title format should be: {Month} {YY} (A+)
    """
    if "(A+)" not in title:
        return None
        
    parts = title.strip().split()
    if len(parts) >= 2:
        month_name_part = parts[0].capitalize()
        year_short_part = parts[1]
        
        # Check if year_short_part is two digits (like "26")
        if not (year_short_part.isdigit() and len(year_short_part) == 2):
            return None
            
        try:
            month_num = list(calendar.month_name).index(month_name_part)
        except ValueError:
            return None
            
        try:
            year = 2000 + int(year_short_part)
        except ValueError:
            return None
            
        sdate = f"{year:04d}-{month_num:02d}-01"
        _, last_day = calendar.monthrange(year, month_num)
        return year, month_num, sdate, last_day
    return None

def pick_club() -> dict | str:
    clear_screen()
    print("Select Target Club (OnlyRex):")
    print("-" * 30)
    club_keys = list(CLUBS.keys())
    for key in club_keys:
        status = ""
        if CLUBS[key].get("complete"):
            status = " [Complete]"
        elif CLUBS[key].get("up_to_date_today"):
            status = " [Up to date]"
        print(f"[{key}] {CLUBS[key]['title']}{status}")
    print("-" * 30)
    print("[0] Process All (Default)")
    print("[E] Exit")
    
    print("\nSelection: ", end="", flush=True)

    if sys.platform == 'win32':
        import msvcrt
        buffer = []
        while True:
            char = msvcrt.getwch()
            if char.lower() == 'e' and not buffer:
                print(char)
                return "EXIT"
            if char == '\r' or char == '\n':
                print()
                choice = "".join(buffer).strip()
                break
            if char == '\b':
                if buffer:
                    buffer.pop()
                    sys.stdout.write('\b \b')
                    sys.stdout.flush()
                continue
            if char.isprintable():
                buffer.append(char)
                print(char, end="", flush=True)
    else:
        choice = input().strip().lower()
        if choice == "e":
            return "EXIT"
    
    if choice == "" or choice == "0":
        return "ALL"
    if choice in CLUBS:
        return CLUBS[choice]
    return CLUBS[list(CLUBS.keys())[0]]

async def process_club_workflow(
    key: str,
    cfg: dict,
    gc_client,
    engine,
    retry_delay: int,
    max_attempts: int,
    per_club_timeout_seconds: int,
) -> bool:
    title = cfg["title"]
    attempt = 0
    
    while attempt < max_attempts:
        try:
            from src.chrono_scraper import scrape_club_data
            cfg_with_key = {**cfg, "api_key": CHRONO_API_KEY}
            async with API_SEMAPHORE:
                raw_data, status_code = await asyncio.wait_for(
                    scrape_club_data(cfg_with_key),
                    timeout=per_club_timeout_seconds
                )
            
            if status_code == 429:
                prefix = colorize("[Rate Limit]", LogColor.RETRY)
                print(f"  {prefix} {title}: 429 hit. Cool-down 30s...", flush=True)
                await asyncio.sleep(30)
                raise Exception("Rate limited")
            
            if status_code != 200 or not raw_data:
                raise Exception(f"API fetch failed (Status {status_code})")

            data = json.loads(raw_data)
            if isinstance(data, dict) and data.get("detail") == "Error":
                 raise Exception("API returned data error")
            
            if isinstance(data, Exception): 
                raise data

            if not data.get("club_friend_history"):
                prefix = colorize("[No Data]", LogColor.RETRY)
                print(f"  {prefix} {title}: No history data available in API yet. Skipping sheet update.", flush=True)
                return True

            df = build_dataframe(data)
            
            async with SHEETS_LOCK:
                loop = asyncio.get_running_loop()
                try:
                    await loop.run_in_executor(
                        None, 
                        export_to_gsheets, 
                        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history")
                    )
                except Exception as e:
                    if "429" in str(e):
                        prefix = colorize("[Quota]", LogColor.RETRY)
                        print(f"  {prefix} {title}: Quota exceeded (429). Waiting 60s for reset...", flush=True)
                        await asyncio.sleep(60) 
                        await loop.run_in_executor(
                            None, 
                            export_to_gsheets, 
                            gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                            data.get("club_daily_history")
                        )
                    else:
                        raise e
            
            prefix = colorize("[Success]", LogColor.SUCCESS)
            print(f"  {prefix} {title}", flush=True)
            return True
            
        except Exception as e:
            attempt_no = attempt + 1
            prefix = colorize("[Error]", LogColor.ERROR)
            print(f"  {prefix} on {title} (Attempt {attempt_no}): {e}", flush=True)

            attempt += 1
            if attempt >= max_attempts:
                return False

            delay = retry_delay + random.uniform(1, 4)
            prefix = colorize("[Retry]", LogColor.RETRY)
            print(f"  {prefix} {title}: sleeping {delay:.1f}s before attempt {attempt + 1}...", flush=True)
            await asyncio.sleep(delay)

async def main():
    setup_windows_console(VERSION)
    is_cron = "--cron" in sys.argv
    force_run = "--force" in sys.argv
    
    if not is_cron:
        print(f"Starting OnlyRex Tracker v{VERSION}...", flush=True)

    # Initialize Google Sheets Client using OnlyRex credentials
    GC = get_gspread_client(base_path, creds_folder='OnlyRex')
    
    # Calculate current effective month
    now_utc = datetime.now(timezone.utc)
    reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
    effective_date = now_utc if now_utc >= reset_time else now_utc - timedelta(days=1)
    
    current_month_name = effective_date.strftime("%B")
    current_year_short = effective_date.strftime("%y")
    current_title = f"{current_month_name} {current_year_short} (A+)"
    
    # Target date calculation: Yesterday if after reset, else 2 days ago
    target_date = now_utc - timedelta(days=1 if now_utc >= reset_time else 2)
    target_col_name = f"Day {target_date.day}"
    
    # Retrieve worksheets to discover months dynamically
    if not is_cron:
        print("Fetching active worksheets...", flush=True)
    try:
        ss = GC.open_by_key(SHEET_ID)
        worksheets = ss.worksheets()
    except Exception as e:
        print(f"Error accessing Google Spreadsheet: {e}", flush=True)
        sys.exit(1)
        
    sheet_titles = [ws.title for ws in worksheets]
    if current_title not in sheet_titles:
        sheet_titles.append(current_title)
        
    # Map from sheet title to headers
    sheet_headers = {}
    for ws in worksheets:
        parsed = parse_sheet_title(ws.title)
        if parsed:
            try:
                headers = ws.row_values(1)
                sheet_headers[ws.title] = headers
            except Exception as e:
                print(f"Warning: Could not fetch headers for worksheet '{ws.title}': {e}", flush=True)
                sheet_headers[ws.title] = []
                
    discovered_clubs = []
    for title in sheet_titles:
        parsed = parse_sheet_title(title)
        if parsed:
            year, month_num, sdate, last_day = parsed
            headers = sheet_headers.get(title, [])
            last_day_col = f"Day {last_day}"
            
            is_complete = last_day_col in headers
            is_up_to_date_today = False
            
            if title == current_title:
                is_up_to_date_today = target_col_name in headers
                
            discovered_clubs.append({
                "title": title,
                "club_id": "150259101",
                "THRESHOLD": 1300000,
                "sdate": sdate,
                "year": year,
                "month": month_num,
                "last_day": last_day,
                "complete": is_complete,
                "up_to_date_today": is_up_to_date_today,
            })
            
    discovered_clubs.sort(key=lambda x: (x["year"], x["month"]))
    
    if discovered_clubs:
        global CLUBS
        CLUBS = {str(idx): c for idx, c in enumerate(discovered_clubs, 1)}
        
    engine_choice = "CHRONO"
    if is_cron:
        choice = "ALL"
    else:
        while True:
            choice = pick_club()
            clear_screen()
            if choice == "EXIT":
                sys.exit(0)
            break 

    RETRY_DELAY = int(os.getenv("CHRONO_RETRY_DELAY", "5"))
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}

    total_failures = 0
    print(f"\nProcessing {len(clubs_to_process)} clubs (OnlyRex)...\n", flush=True)

    tasks = []
    outcomes = []
    
    for key, cfg in clubs_to_process.items():
        is_complete = cfg.get("complete", False)
        is_up_to_date = cfg.get("up_to_date_today", False)
        
        if is_complete and not force_run:
            print(f"--- Skip: {cfg['title']} is complete (all days recorded) ---", flush=True)
            continue
            
        if is_up_to_date and not force_run:
            print(f"--- Skip: {cfg['title']} is already up to date with {target_col_name} ---", flush=True)
            continue
            
        # Staggered start
        await asyncio.sleep(random.uniform(0.5, 1.0))
        tasks.append(
            asyncio.create_task(
                process_club_workflow(
                    key,
                    cfg,
                    GC,
                    engine_choice,
                    RETRY_DELAY,
                    5,    # Increased max_attempts
                    90    # timeout
                )
            )
        )
            
    if tasks:
        results = await asyncio.gather(*tasks)
        outcomes.extend(results)
        
    total_failures = outcomes.count(False)

    # Reorder worksheets chronologically (positioned on the right)
    try:
        all_titles = [ws.title for ws in ss.worksheets()]
        monthly_titles = [c["title"] for c in discovered_clubs]
        other_titles = [t for t in all_titles if t not in monthly_titles]
        reorder_sheets(GC, SHEET_ID, other_titles + monthly_titles)
    except Exception as e:
        print(f"Warning: Failed to reorder worksheets: {e}", flush=True)

    print("-" * 30)
    if total_failures > 0:
        print(f"Completed with errors: {total_failures} failed.", flush=True)
    else:
        print("All operations complete.", flush=True)
    
    print("-" * 30)
    
    if not is_cron:
        input("Press Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
