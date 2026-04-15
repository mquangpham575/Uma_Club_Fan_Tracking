import asyncio
import os
import sys
import random
import logging
import json
from datetime import datetime, timezone
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
        
    from config.globals import CLUBS, SHEET_ID, VERSION
except ImportError as e:
    print(f"Error: 'globals.py' not found (Base path: {base_path}). Details: {e}")
    sys.exit(1)

# Zendriver compatibility patches removed (Chrono now uses direct API)

# Import Modules
from src.processing import build_dataframe
from src.sheets import export_to_gsheets, get_gspread_client, reorder_sheets
from src.utils import clear_screen, setup_windows_console, LogColor, colorize


# Global lock to prevent concurrent Google Sheets structural modifications
SHEETS_LOCK = asyncio.Lock()

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
) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # Phase 1: Fetch
            from src.chrono_scraper import scrape_club_data
            raw_data = await asyncio.wait_for(
                scrape_club_data(cfg),
                timeout=per_club_timeout_seconds
            )
            if not raw_data:
                raise Exception("API fetch failed")
            data = json.loads(raw_data)
            
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

    # Redundancy check: Skip if today's data is already updated
    if is_cron and choice == "ALL":
        try:
            # Chrono resets at 10:00 UTC. 
            # The data available at 10:00 UTC reflects results from 'Yesterday'.
            # e.g., On Day 15, after 10:00 UTC, we expect 'Day 14' to be present.
            now_utc = datetime.now(timezone.utc)
            reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
            
            # Target day number calculation
            if now_utc >= reset_time:
                target_day_num = now_utc.day - 1
            else:
                target_day_num = now_utc.day - 2
                
            # Handle start of month edge cases
            if target_day_num > 0:
                target_col_name = f"Day {target_day_num}"
                
                # Check the first club's sheet as a status indicator
                first_club_title = list(CLUBS.values())[0]['title']
                ss = GC.open_by_key(SHEET_ID)
                try:
                    ws = ss.worksheet(first_club_title)
                    headers = ws.row_values(1)
                    if target_col_name in headers:
                        print(f"--- Skip: Sheet is already up to date with {target_col_name} ---")
                        return
                except Exception:
                    pass # Proceed if sheet not found
        except Exception as e:
            print(f"Warning: Freshness check failed, proceeding anyway: {e}")

    total_failures = 0
    print(f"\nProcessing {len(clubs_to_process)} clubs (Engine: {engine_choice})...\n", flush=True)

    # Launch all tasks simultaneously (Speed of API)
    tasks = []
    for key, cfg in clubs_to_process.items():
        tasks.append(
            asyncio.create_task(
                process_club_workflow(
                    key,
                    cfg,
                    GC,
                    engine_choice,
                    RETRY_DELAY,
                    3,    # max_attempts
                    90    # timeout
                )
            )
        )
            
    # Wait for all club updates to finish
    outcomes = await asyncio.gather(*tasks)
    total_failures = outcomes.count(False)

    # Reordering is now always the final step after the parallel gather
    print("Reordering sheets...", flush=True)
    ordered_titles = [CLUBS[k]['title'] for k in CLUBS]
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