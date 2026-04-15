import asyncio
import os
import sys
import random
import logging
import json
from datetime import datetime, timezone

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

# --- PATCH FOR ZENDRIVER CDP COMPATIBILITY ---
try:
    import zendriver.cdp.network as network
    
    # Patch ClientSecurityState to handle missing 'privateNetworkRequestPolicy'
    _orig_css_from_json = network.ClientSecurityState.from_json
    def patched_css_from_json(json):
        if "privateNetworkRequestPolicy" not in json:
            json["privateNetworkRequestPolicy"] = "PreflightBlock" # Default value
        return _orig_css_from_json(json)
    network.ClientSecurityState.from_json = patched_css_from_json
    
    # Patch Cookie to handle missing 'sameParty'
    _orig_cookie_from_json = network.Cookie.from_json
    def patched_cookie_from_json(json):
        if "sameParty" not in json:
            json["sameParty"] = False
        return _orig_cookie_from_json(json)
    network.Cookie.from_json = patched_cookie_from_json
    
    print("Applied Zendriver compatibility patches for Chrome 146+")
except Exception as e:
    print(f"Note: Could not apply Zendriver patches: {e}")
# ---------------------------------------------

# Import Modules
from src.processing import build_dataframe
from src.sheets import export_to_gsheets, get_gspread_client, reorder_sheets
from src.utils import clear_screen, setup_windows_console, LogColor, colorize


# Global lock to prevent concurrent Google Sheets structural modifications
SHEETS_LOCK = asyncio.Lock()

# Global pacing state for Chrono scraping to avoid triggering anti-bot throttles.
CHRONO_START_LOCK = asyncio.Lock()
CHRONO_LAST_START_MONO = 0.0


async def throttle_chrono_start(min_interval_seconds: float):
    """Ensure a minimum interval between Chrono scrape starts across all tasks."""
    global CHRONO_LAST_START_MONO
    loop = asyncio.get_running_loop()
    async with CHRONO_START_LOCK:
        now = loop.time()
        wait_for = max(0.0, min_interval_seconds - (now - CHRONO_LAST_START_MONO))
        if wait_for > 0:
            prefix = colorize("[Cooldown]", LogColor.COOLDOWN)
            print(f"  {prefix} Waiting {wait_for:.1f}s before next Chrono request...", flush=True)
            await asyncio.sleep(wait_for)
        CHRONO_LAST_START_MONO = loop.time()


def has_fresh_snapshot(circle_id: str, max_age_hours: int) -> bool:
    """Check whether a club already has a recent local snapshot in api_data/."""
    path = os.path.join("api_data", f"{circle_id}.json")
    if not os.path.exists(path):
        return False
    max_age_seconds = max_age_hours * 3600
    age_seconds = datetime.now(timezone.utc).timestamp() - os.path.getmtime(path)
    return age_seconds <= max_age_seconds

def save_raw_json_to_file(circle_id: str, raw_data: any):
    """Save raw JSON to a local file for GitHub Pages hosting"""
    if not raw_data:
        return
    
    os.makedirs("api_data", exist_ok=True)
    file_path = f"api_data/{circle_id}.json"
    
    with open(file_path, "w", encoding="utf-8") as f:
        # Ensure it's a dict before saving
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except Exception:
                pass
        json.dump(raw_data, f, indent=2, ensure_ascii=False)
    prefix = colorize("[API]", LogColor.API)
    print(f"  {prefix} Saved raw JSON to {file_path}", flush=True)
# Helper Functions
def select_engine() -> str:
    clear_screen()
    print("Select Extraction Engine:")
    print("-" * 30)
    print("[1] UMOE (API - Faster, Default)")
    print("[2] Chrono (Browser - Reliable Backup)")
    print("-" * 30)
    print("\nSelection: ", end="", flush=True)
    
    if sys.platform == 'win32':
        import msvcrt
        char = msvcrt.getwch()
        if char == '2':
            print("Chrono")
            return "CHRONO"
    else:
        choice = input().strip()
        if choice == "2":
            return "CHRONO"
    
    print("UMOE")
    return "UMOE"

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
    skip_sheets: bool = False,
) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    
    attempt = 0
    # Staggered start to prevent thundering herd when starting browsers in parallel
    if engine == "CHRONO":
        await asyncio.sleep(random.uniform(0, 5))
    
    while attempt < max_attempts:
        try:
            # consistency: use initial_result only on first attempt if it's valid
            data = initial_result if (attempt == 0 and not isinstance(initial_result, Exception)) else None

            if engine == "CHRONO":
                await throttle_chrono_start(chrono_start_interval)
            
            # Phase 1: Fetch and Save Locally
            if data is None:
                if engine == "CHRONO":
                    from src.chrono_scraper import scrape_club_data
                    raw_data = await asyncio.wait_for(
                        scrape_club_data(cfg, zd_module),
                        timeout=per_club_timeout_seconds
                    )
                    if not raw_data:
                        raise Exception("Chrono scrape failed to capture data")
                    data = json.loads(raw_data)
                    if isinstance(data, dict) and data.get("detail") == "Error":
                        raise Exception("Chrono captured API error: detail: Error")
                else:
                    from src.umoe_scraper import fetch_club_data
                    data = await asyncio.wait_for(
                        fetch_club_data(cfg),
                        timeout=per_club_timeout_seconds
                    )
            
            if isinstance(data, Exception): raise data

            # STEP: Save raw JSON to file
            circle_id = cfg.get("club_id")
            if circle_id:
                raw_to_sync = data.get("raw_response") if engine == "UMOE" else data
                save_raw_json_to_file(circle_id, raw_to_sync)

            # Phase 2: Export to Sheets (Optionally skip during parallel run)
            if not skip_sheets:
                df = build_dataframe(data)
                async with SHEETS_LOCK:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, 
                        export_to_gsheets, 
                        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history")
                    )
            
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

def sync_local_json_to_sheets(clubs_to_sync: dict, gc_client):
    """Phase 2: Sequentially process saved JSON and export to Google Sheets."""
    print(f"\n--- Phase 2: Syncing Local Data to Google Sheets ---", flush=True)
    
    for key, cfg in clubs_to_sync.items():
        title = cfg["title"]
        circle_id = cfg.get("club_id")
        path = os.path.join("api_data", f"{circle_id}.json")
        
        if not os.path.exists(path):
            prefix = colorize("[Skip]", LogColor.RETRY)
            print(f"  {prefix} {title}: No local JSON found at {path}", flush=True)
            continue
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Process DataFrame
            df = build_dataframe(data)
            
            # Sequential Export with 429 Retry
            try:
                export_to_gsheets(
                    gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                    data.get("club_daily_history")
                )
            except Exception as e:
                if "429" in str(e):
                    prefix = colorize("[Quota]", LogColor.RETRY)
                    print(f"  {prefix} {title}: Quota exceeded (429). Waiting 60s for reset...", flush=True)
                    import time
                    time.sleep(60)
                    export_to_gsheets(
                        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history")
                    )
                else:
                    raise e

            prefix = colorize("[Sync]", LogColor.SUCCESS)
            print(f"  {prefix} {title}: Updated Sheets from local JSON", flush=True)
        except Exception as e:
            prefix = colorize("[Failed]", LogColor.ERROR)
            print(f"  {prefix} {title}: Sync failed: {e}", flush=True)

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
    
    # Engine Selection
    if is_cron:
        engine_choice = "UMOE"
        if "--engine" in sys.argv:
            idx = sys.argv.index("--engine")
            if idx + 1 < len(sys.argv):
                engine_choice = sys.argv[idx + 1].upper()
        choice = "ALL"
    else:
        # Check CLI args even if not cron
        if "--engine" in sys.argv:
            idx = sys.argv.index("--engine")
            if idx + 1 < len(sys.argv):
                engine_choice = sys.argv[idx + 1].upper()
            else:
                engine_choice = select_engine()
        else:
            engine_choice = select_engine()

        while True:
            choice = pick_club()
            clear_screen()
            if choice == "EXIT":
                sys.exit(0)
            break 

    # Lazy-load dependencies for selected engine
    zd = None
    if engine_choice == "CHRONO":
        import zendriver as zd_module
        zd = zd_module
    
    # Re-import UMOE scraper for the parallel fetch step
    if engine_choice == "UMOE":
        from src.umoe_scraper import fetch_club_data

    CHRONO_BATCH_SIZE = int(os.getenv("CHRONO_BATCH_SIZE", "3"))
    CHRONO_START_INTERVAL = float(os.getenv("CHRONO_START_INTERVAL", "15"))
    CHRONO_RETRY_DELAY = int(os.getenv("CHRONO_RETRY_DELAY", "15"))
    CHRONO_TIMEOUT_COOLDOWN = int(os.getenv("CHRONO_TIMEOUT_COOLDOWN", "15"))
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
        
        # Step 1: Parallel Fetch
        results_map = {}
        if engine_choice == "UMOE":
            fetch_tasks = {key: asyncio.create_task(fetch_club_data(CLUBS[key])) for key in batch_keys}
            fetch_results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
            results_map = dict(zip(batch_keys, fetch_results))
        else:
            # Chrono is best run one by one in some environments, but let's try parallel if resources allow
            # However, for integrated app, let's just use raw data mapping later
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
                        CHRONO_PER_CLUB_TIMEOUT,
                        skip_sheets=True # Only scrape during Phase 1
                    )
                )
            )
            
        # Wait for all exports in this batch to finish
        batch_outcomes = await asyncio.gather(*export_tasks)
        failures = batch_outcomes.count(False)
        total_failures += failures
        print("") 

    # Phase 2: Sequential Sync
    if not scrape_only:
        sync_local_json_to_sheets(clubs_to_process, GC)

    print("-" * 30)
    if total_failures > 0:
        print(f"Completed with errors: {total_failures} failed.", flush=True)
    else:
        print("All operations complete.", flush=True)
        
    print("Reordering sheets...", flush=True)
    ordered_titles = [CLUBS[k]['title'] for k in CLUBS]
    try:
        reorder_sheets(GC, SHEET_ID, ordered_titles)
    except Exception as e:
        if "429" in str(e):
            print("  [Quota] Reordering hit limit. Waiting 60s...", flush=True)
            import time
            time.sleep(60)
            reorder_sheets(GC, SHEET_ID, ordered_titles)
        else:
            print(f"Warning: Failed to reorder sheets: {e}")
    print("Sheets reordered.", flush=True)
    print("-" * 30)
    
    if not is_cron:
        input("Press Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())