import asyncio
import os
import sys
import random

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
from src.utils import clear_screen, setup_windows_console
from src.sync import sync_raw_json_to_db # New database sync logic


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
async def process_and_export_club(cfg: dict, gc_client, engine="UMOE", zd_module=None, pre_fetched_data=None):
    # Fetch data if not provided (now engine-aware)
    if pre_fetched_data is not None:
        data = pre_fetched_data
    else:
        if engine == "CHRONO":
            from src.chrono_scraper import scrape_club_data
            import json
            raw_data = await scrape_club_data(cfg, zd_module)
            if not raw_data:
                raise Exception("Chrono scrape failed to capture data")
            data = json.loads(raw_data)
        else:
            from src.umoe_scraper import fetch_club_data
            data = await fetch_club_data(cfg)

    if isinstance(data, Exception): 
        raise data
    
    # Process DataFrame (CPU-bound, fast)
    df = build_dataframe(data)
    
    # STEP: Database Sync (Push raw JSON to UmaCore DB)
    circle_id = cfg.get("club_id")
    if circle_id:
        print(f"  Syncing raw JSON for {cfg['title']} ({circle_id}) to DB...", flush=True)
        # For Chrono, it's a string. For UMOE, it's in raw_response.
        raw_to_sync = pre_fetched_data if pre_fetched_data else (data.get("raw_response") if engine == "UMOE" else data)
        await sync_raw_json_to_db(circle_id, raw_to_sync)

    # Export to Google Sheets (Blocking I/O - Run in Thread)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, 
        export_to_gsheets, 
        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
        data.get("club_daily_history")
    )
    return True

async def process_club_workflow(key: str, cfg: dict, gc_client, engine, zd_module, initial_result, retry_delay: int) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    
    attempt = 0
    # Staggered start to prevent thundering herd when starting browsers in parallel
    if engine == "CHRONO":
        await asyncio.sleep(random.uniform(0, 5))
    
    while True:
        try:
            # consistency: use initial_result only on first attempt if it's valid
            data = initial_result if (attempt == 0 and not isinstance(initial_result, Exception)) else None
            
            if attempt > 0:
                await asyncio.sleep(retry_delay)
            
            await process_and_export_club(cfg, gc_client, engine=engine, zd_module=zd_module, pre_fetched_data=data)
            print(f"  Success: {title}", flush=True)
            return True
            
        except Exception as e:
            print(f"  Error on {title} (Attempt {attempt + 1}): {e}", flush=True)
            attempt += 1
            if attempt >= 2: # Keep retries reasonable for integrated app
                return False

async def main():
    setup_windows_console(VERSION)
    is_cron = "--cron" in sys.argv
    
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

    BATCH_SIZE = 3 if engine_choice == "CHRONO" else 5
    RETRY_DELAY = 5
    
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}
    club_keys = list(clubs_to_process.keys())
    batches = [club_keys[i:i + BATCH_SIZE] for i in range(0, len(club_keys), BATCH_SIZE)]

    print(f"\nProcessing {len(club_keys)} clubs (Engine: {engine_choice})...\n", flush=True)
    
    total_failures = 0
    for batch_idx, batch_keys in enumerate(batches):
        print(f"Batch {batch_idx + 1}/{len(batches)}: Processing {len(batch_keys)} items...", flush=True)
        
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
                    process_club_workflow(key, cfg, GC, engine_choice, zd, result, RETRY_DELAY)
                )
            )
            
        # Wait for all exports in this batch to finish
        batch_outcomes = await asyncio.gather(*export_tasks)
        failures = batch_outcomes.count(False)
        total_failures += failures
        print("") 

    print("-" * 30)
    if total_failures > 0:
        print(f"Completed with errors: {total_failures} failed.", flush=True)
    else:
        print("All operations complete.", flush=True)
        
    print("Reordering sheets...", flush=True)
    ordered_titles = [CLUBS[k]['title'] for k in CLUBS]
    reorder_sheets(GC, SHEET_ID, ordered_titles)
    print("Sheets reordered.", flush=True)
    print("-" * 30)
    
    if not is_cron:
        input("Press Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())