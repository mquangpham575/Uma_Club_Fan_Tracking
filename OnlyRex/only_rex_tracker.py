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


# Global lock to prevent concurrent Google Sheets structural modifications
SHEETS_LOCK = asyncio.Lock()

def pick_club() -> dict | str:
    clear_screen()
    print("Select Target Club (OnlyRex):")
    print("-" * 30)
    club_keys = list(CLUBS.keys())
    for key in club_keys:
        print(f"[{key}] {CLUBS[key]['title']}")
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
            raw_data = await asyncio.wait_for(
                scrape_club_data(cfg_with_key),
                timeout=per_club_timeout_seconds
            )
            if not raw_data:
                raise Exception("API fetch failed")
            data = json.loads(raw_data)
            
            if isinstance(data, Exception): 
                raise data

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
    
    if not is_cron:
        print(f"Starting OnlyRex Tracker v{VERSION}...", flush=True)

    # Initialize Google Sheets Client using OnlyRex credentials
    GC = get_gspread_client(base_path, creds_folder='OnlyRex')
    
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
            
    outcomes = await asyncio.gather(*tasks)
    total_failures = outcomes.count(False)

    # Reordering logic removed as requested for OnlyRex
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
