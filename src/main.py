import asyncio
import os
import sys
import random
import logging
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load local environment variables from .env if present
load_dotenv()

# Silence verbose browser logs
logging.getLogger("zendriver").setLevel(logging.WARNING)
logging.getLogger("uc").setLevel(logging.WARNING)

# Import Globals
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
        
    from config.globals import CLUBS, SHEET_ID, VERSION, first_day_of_month
except ImportError as e:
    print(f"Error: 'globals.py' not found (Base path: {base_path}). Details: {e}")
    sys.exit(1)

# Zendriver compatibility patches removed (Chrono now uses direct API)

# Import Modules
from src.processing import build_dataframe
from src.sheets import export_to_gsheets, get_gspread_client, reorder_sheets, export_all_club_data_to_gsheets, get_green_members
from src.utils import clear_screen, setup_windows_console, LogColor, colorize


# Global locks to prevent concurrent resource exhaustion
SHEETS_LOCK = asyncio.Lock()
API_SEMAPHORE = asyncio.Semaphore(1)  # Strict sequential API access

# Throttling logic removed (Chrono now uses direct API)


def has_fresh_snapshot(circle_id: str, max_age_hours: int) -> bool:
    """Check whether a club already has a recent local snapshot (logic naturally returning False since JSONs are removed)."""
    return False

# JSON saving removed (Now syncs directly to Google Sheets)
# Helper Functions
def select_engine() -> str:
    # UMOE removed, defaulting to Chrono
    return "CHRONO"

def pick_club() -> dict | str:
    clear_screen()
    print("Select Target Club:")
    print("-" * 30)
    club_keys = list(CLUBS.keys())
    for key in club_keys:
        print(f"[{key}] {CLUBS[key]['title']}")
    print("-" * 30)
    print("[0] Process All (Default)")
    print("[E] Exit")
    
    print("\nSelection: ", end="", flush=True)

    # Hotkey implementation for Windows
    if sys.platform == 'win32':
        import msvcrt
        buffer = []
        while True:
            # Get a single character
            char = msvcrt.getwch()
            
            # Hotkeys for E (case insensitive)
            if char.lower() == 'e' and not buffer:
                print(char) # Echo the char
                return "EXIT"
            
            # Handle Enter
            if char == '\r' or char == '\n':
                print() # New line
                choice = "".join(buffer).strip()
                break
                
            # Handle Backspace
            if char == '\b':
                if buffer:
                    buffer.pop()
                    # visual backspace (move back, overwrite with space, move back)
                    sys.stdout.write('\b \b')
                    sys.stdout.flush()
                continue
                
            # Handle numeric input only
            if char.isprintable():
                buffer.append(char)
                print(char, end="", flush=True)
                
    else:
        # Fallback for non-Windows (or if msvcrt fails/not available)
        choice = input().strip().lower()
        if choice == "e":
            return "EXIT"
    
    if choice == "" or choice == "0":
        return "ALL"
    if choice in CLUBS:
        return CLUBS[choice]
    return CLUBS[list(CLUBS.keys())[0]]

# Main Execution
async def process_club_workflow(
    key: str,
    cfg: dict,
    gc_client,
    engine,
    retry_delay: int,
    max_attempts: int,
    per_club_timeout_seconds: int,
    green_members: set = None,
) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    attempt = 0
    
    while attempt < max_attempts:
        try:
            from src.chrono_scraper import scrape_club_data
            async with API_SEMAPHORE:
                raw_data, status_code = await asyncio.wait_for(
                    scrape_club_data(cfg),
                    timeout=per_club_timeout_seconds
                )
                await asyncio.sleep(2.5)
                
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
 
            # Phase 2: Export to Sheets with 429 Retry logic
            df = build_dataframe(data)
            async with SHEETS_LOCK:
                loop = asyncio.get_running_loop()
                try:
                    await loop.run_in_executor(
                        None, 
                        export_to_gsheets, 
                        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history"),
                        green_members
                    )
                except Exception as e:
                    if "429" in str(e) or "500" in str(e):
                        prefix = colorize("[Quota/Server]", LogColor.RETRY)
                        print(f"  {prefix} {title}: Error ({e}). Waiting 30s for reset...", flush=True)
                        await asyncio.sleep(30) 
                        await loop.run_in_executor(
                            None, 
                            export_to_gsheets, 
                            gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                            data.get("club_daily_history"),
                            green_members
                        )
                    else:
                        raise e
                # Cooldown to respect Google Sheets write quota limit
                await asyncio.sleep(3.0)
            
            prefix = colorize("[Success]", LogColor.SUCCESS)
            print(f"  {prefix} {title}", flush=True)
            
            # Extract data for summary sheet
            day_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
            member_data = []
            for _, row in df.iterrows():
                perf = row[day_cols].sum() if day_cols else 0.0
                member_data.append({
                    "member_name": row["Member_Name"],
                    "avg_day": row["AVG/d"],
                    "performance": perf
                })
                
            if "(" in title and ")" in title:
                short_name = title.split("(")[0].strip()
                grade = title.split("(")[1].split(")")[0].strip()
            else:
                short_name = title
                grade = ""
                
            rank = ""
            daily_history = data.get("club_daily_history") or []
            if daily_history:
                try:
                    latest_entry = max(daily_history, key=lambda x: int(x.get("actual_date", 0)))
                    rank_val = latest_entry.get("rank")
                    if rank_val is not None:
                        rank = f"#{rank_val}"
                except Exception:
                    rank_val = daily_history[-1].get("rank")
                    if rank_val is not None:
                        rank = f"#{rank_val}"
                        
            club_metadata = {
                "short_name": short_name,
                "grade": grade,
                "rank": rank,
                "members": member_data
            }
            return club_metadata
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            attempt_no = attempt + 1
            prefix = colorize("[Error]", LogColor.ERROR)
            print(f"  {prefix} on {title} (Attempt {attempt_no}): {e}", flush=True)

            attempt += 1
            if attempt >= max_attempts:
                return None

            delay = retry_delay + random.uniform(1, 4)
            prefix = colorize("[Retry]", LogColor.RETRY)
            print(f"  {prefix} {title}: sleeping {delay:.1f}s before attempt {attempt + 1}...", flush=True)
            await asyncio.sleep(delay)

# Phase 2 removal (Consolidated into process_club_workflow)

async def main():
    setup_windows_console(VERSION)
    is_cron = "--cron" in sys.argv
    scrape_only = "--scrape-only" in sys.argv
    sync_only = "--sync-only" in sys.argv
    
    # Startup
    if not is_cron:
        print(f"Starting Endless v{VERSION}...", flush=True)

    # Initialize Google Sheets Client
    GC = get_gspread_client(base_path)
    
    # Fetch existing green members (Carry Club/Hard Carry) to preserve them
    club_titles = [CLUBS[k]['title'] for k in CLUBS]
    print("Fetching existing Carry Club members from worksheets...", flush=True)
    green_members = get_green_members(GC, SHEET_ID, club_titles)
    print(f"Found {len(green_members)} Carry Club members.", flush=True)

    # Engine is now exclusively Chrono
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

    # Lazy-loading Dependencies
    zd = None

    RETRY_DELAY = int(os.getenv("CHRONO_RETRY_DELAY", "5"))
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}

    force_run = "--force" in sys.argv

    # Redundancy check: Skip if today's data is already updated
    if is_cron and choice == "ALL" and not force_run:
        try:
            # Chrono resets at 10:00 UTC. 
            # The data available at 10:00 UTC reflects results from 'Yesterday'.
            # e.g., On Day 15, after 10:00 UTC, we expect 'Day 14' to be present.
            now_utc = datetime.now(timezone.utc)
            reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
            
            # Target date calculation: Yesterday if after reset, else 2 days ago
            target_date = now_utc - timedelta(days=1 if now_utc >= reset_time else 2)
            target_col_name = f"Day {target_date.day}"
            expected_month_str = target_date.strftime("%B %Y").upper()
                
            # Check if the summary sheet month matches the target month to prevent skipping month transitions
            ss = GC.open_by_key(SHEET_ID)
            try:
                summary_ws = ss.worksheet("All Club Data")
                first_row = summary_ws.row_values(1)
                if not first_row or expected_month_str not in first_row[0]:
                    print(f"--- Month transition detected ({expected_month_str}). Proceeding with update... ---")
                else:
                    # Same month, verify if target day's column is already present in first club's sheet
                    first_club_title = list(CLUBS.values())[0]['title']
                    try:
                        ws = ss.worksheet(first_club_title)
                        headers = ws.row_values(1)
                        if target_col_name in headers:
                            print(f"--- Skip: Sheet is already up to date with {target_col_name} ---")
                            return
                    except Exception:
                        pass # Proceed if worksheet not found
            except Exception as e:
                print(f"Warning: Summary sheet month verification failed, proceeding: {e}")
        except Exception as e:
            print(f"Warning: Freshness check failed, proceeding anyway: {e}")

    total_failures = 0
    successful_clubs = []
    print(f"\nProcessing {len(clubs_to_process)} clubs (Engine: {engine_choice})...\n", flush=True)

    for key, cfg in clubs_to_process.items():
        outcome = await process_club_workflow(
            key,
            cfg,
            GC,
            engine_choice,
            RETRY_DELAY,
            5,    # Increased max_attempts
            90,   # timeout
            green_members
        )
        if outcome is not None:
            successful_clubs.append(outcome)
        else:
            total_failures += 1
        # Cooldown between clubs to prevent API rate limiting
        await asyncio.sleep(2.0)

    if choice == "ALL" and successful_clubs:
        print("Exporting All Club Data summary sheet...", flush=True)
        try:
            export_all_club_data_to_gsheets(GC, SHEET_ID, successful_clubs, sdate=first_day_of_month, green_members=green_members)
            print("All Club Data summary sheet updated.", flush=True)
        except Exception as e:
            print(f"Warning: Failed to update All Club Data summary sheet: {e}", flush=True)

    # Reordering is now always the final step after the parallel gather
    print("Reordering sheets...", flush=True)
    ordered_titles = ["All Club Data"] + [CLUBS[k]['title'] for k in CLUBS]
    try:
        reorder_sheets(GC, SHEET_ID, ordered_titles)
    except Exception as e:
        if "429" in str(e):
            print("  [Quota] Reordering hit limit. Waiting 60s...", flush=True)
            await asyncio.sleep(60)
            reorder_sheets(GC, SHEET_ID, ordered_titles)
        else:
            print(f"Warning: Failed to reorder sheets: {e}")
    print("Sheets reordered.", flush=True)

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