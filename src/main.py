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
    zd_module,
    initial_result,
    retry_delay: int,
    chrono_start_interval: float,
    chrono_timeout_cooldown: int,
    max_attempts: int,
    per_club_timeout_seconds: int,
) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # use initial_result only on first attempt if it's valid
            data = initial_result if (attempt == 0 and not isinstance(initial_result, Exception)) else None

            # Phase 1: Fetch
            if data is None:
                from src.chrono_scraper import scrape_club_data
                raw_data = await asyncio.wait_for(
                    scrape_club_data(cfg),
                    timeout=per_club_timeout_seconds
                )
                if not raw_data:
                    raise Exception("Chrono API fetch failed")
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
            is_selector_timeout = "club-id-input" in str(e) or "search_box timeout" in str(e)

            attempt += 1
            if attempt >= max_attempts:
                return False

            delay = retry_delay
            if engine == "CHRONO" and is_selector_timeout:
                delay = max(retry_delay, chrono_timeout_cooldown)
            delay += random.uniform(1, 4)
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

    CHRONO_BATCH_SIZE = int(os.getenv("CHRONO_BATCH_SIZE", "3"))
    CHRONO_START_INTERVAL = float(os.getenv("CHRONO_START_INTERVAL", "8"))
    CHRONO_RETRY_DELAY = int(os.getenv("CHRONO_RETRY_DELAY", "15"))
    CHRONO_TIMEOUT_COOLDOWN = int(os.getenv("CHRONO_TIMEOUT_COOLDOWN", "12"))
    CHRONO_MAX_ATTEMPTS = int(os.getenv("CHRONO_MAX_ATTEMPTS", "3"))
    CHRONO_PER_CLUB_TIMEOUT = int(os.getenv("CHRONO_PER_CLUB_TIMEOUT", "90"))
    SKIP_FRESH_CHRONO = False
    FRESH_MAX_AGE_HOURS = int(os.getenv("FRESH_MAX_AGE_HOURS", "1"))

    BATCH_SIZE = CHRONO_BATCH_SIZE if engine_choice == "CHRONO" else 5
    RETRY_DELAY = CHRONO_RETRY_DELAY if engine_choice == "CHRONO" else 5
    
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}

    if engine_choice == "CHRONO" and choice == "ALL" and SKIP_FRESH_CHRONO:
        original_count = len(clubs_to_process)
        clubs_to_process = {
            k: v for k, v in clubs_to_process.items()
            if not has_fresh_snapshot(v.get("club_id", ""), FRESH_MAX_AGE_HOURS)
        }
        skipped = original_count - len(clubs_to_process)
        if skipped > 0:
            print(
                f"Skipping {skipped} clubs with fresh snapshots (<= {FRESH_MAX_AGE_HOURS}h old).",
                flush=True,
            )
    club_keys = list(clubs_to_process.keys())
    batches = [club_keys[i:i + BATCH_SIZE] for i in range(0, len(club_keys), BATCH_SIZE)]

    total_failures = 0
    if sync_only:
        # Skip Scrape Phase
        batches = []
        print("\nSkipping Scraping Phase (--sync-only)...\n", flush=True)
    else:
        print(f"\nProcessing {len(club_keys)} clubs (Engine: {engine_choice})...\n", flush=True)

    for batch_idx, batch_keys in enumerate(batches):
        batch_text = colorize(f"Batch {batch_idx + 1}/{len(batches)}", LogColor.BATCH)
        print(f"{batch_text}: Processing {len(batch_keys)} items...", flush=True)
        
        # Step 1: Parallel Fetch (Mapping Phase)
        results_map = {key: None for key in batch_keys}

        # Step 2: Parallel Export & Processing
        export_tasks = []
        for key in batch_keys:
            cfg = CLUBS[key]
            result = results_map.get(key)
            export_tasks.append(
                asyncio.create_task(
                    process_club_workflow(
                        key,
                        cfg,
                        GC,
                        engine_choice,
                        zd,
                        result,
                        RETRY_DELAY,
                        CHRONO_START_INTERVAL,
                        CHRONO_TIMEOUT_COOLDOWN,
                        CHRONO_MAX_ATTEMPTS,
                        CHRONO_PER_CLUB_TIMEOUT
                    )
                )
            )
            
        # Wait for all exports in this batch to finish
        batch_outcomes = await asyncio.gather(*export_tasks)
        failures = batch_outcomes.count(False)
        total_failures += failures
        print("") 

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