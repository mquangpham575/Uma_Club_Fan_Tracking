import asyncio
import os
import sys

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

# Import Modules
from src import updater
from src.processing import build_dataframe
from src.scraper import fetch_club_data
from src.sheets import export_to_gsheets, get_gspread_client, reorder_sheets
from src.utils import clear_screen, setup_windows_console


# Helper Functions
def pick_club() -> dict | str:
    clear_screen()
    print("Select Target Club:")
    print("-" * 30)
    club_keys = list(CLUBS.keys())
    for key in club_keys:
        print(f"[{key}] {CLUBS[key]['title']}")
    print("-" * 30)
    print("[0] Process All (Default)")
    print("[U] Check for Updates")
    print("[E] Exit")
    
    print("\nSelection: ", end="", flush=True)

    # Hotkey implementation for Windows
    if sys.platform == 'win32':
        import msvcrt
        buffer = []
        while True:
            # Get a single character
            char = msvcrt.getwch()
            
            # Hotkeys for U and E (case insensitive)
            if char.lower() == 'u' and not buffer:
                print(char) # Echo the char
                return "UPDATE"
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
                
            # Handle numeric input only (since club keys are digits or 0)
            # Actually, club keys are strings "1", "2", etc.
            # We should allow any printable char really in case keys change, but digits are safer for now
            # Let's just allow printable chars to fill buffer
            if char.isprintable():
                buffer.append(char)
                print(char, end="", flush=True)
                
    else:
        # Fallback for non-Windows (or if msvcrt fails/not available)
        choice = input().strip().lower()
        if choice == "u":
            return "UPDATE"
        if choice == "e":
            return "EXIT"
    
    if choice == "" or choice == "0":
        return "ALL"
    if choice in CLUBS:
        return CLUBS[choice]
    return CLUBS[list(CLUBS.keys())[0]]

# Main Execution
async def process_and_export_club(cfg: dict, gc_client, pre_fetched_data=None):
    # Fetch data if not provided
    data = await fetch_club_data(cfg) if pre_fetched_data is None else pre_fetched_data
    if isinstance(data, Exception): 
        raise data
    
    # Process DataFrame (CPU-bound, fast)
    df = build_dataframe(data)
    
    # Export to Google Sheets (Blocking I/O - Run in Thread)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, 
        export_to_gsheets, 
        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
        data.get("club_daily_history")
    )
    return True

async def process_club_workflow(key: str, cfg: dict, gc_client, initial_result, retry_delay: int) -> bool:
    # Handles the retry loop and processing for a single club
    title = cfg["title"]
    
    attempt = 0
    while True:
        try:
            # consistency: use initial_result only on first attempt if it's valid
            data = initial_result if (attempt == 0 and not isinstance(initial_result, Exception)) else None
            
            if attempt > 0:
                await asyncio.sleep(retry_delay)
            
            await process_and_export_club(cfg, gc_client, pre_fetched_data=data)
            print(f"  Success: {title}", flush=True)
            return True
            
        except Exception:
            # On failure, data becomes None for next loop -> triggers re-fetch
            attempt += 1



async def main():
    setup_windows_console(VERSION)
    
    # Startup Update Check (Non-blocking)
    print(f"Starting Endless v{VERSION}...", flush=True)
    try:
        print("Checking for updates...", end="", flush=True)
        update_info = updater.check_for_update()
        if update_info:
            tag, url = update_info
            print(f"\n[!] New version available: {tag}")
            if input("    Do you want to install it now? (y/n): ").strip().lower().startswith('y'):
                 updater.update_application(url)
                 return 
        else:
            print(" (Up to date)")
    except Exception:
        print(" (Check failed - continuing)")

    # Initialize Google Sheets Client
    GC = get_gspread_client(base_path)
    
    while True:
        choice = pick_club()
        clear_screen()
        
        if choice == "EXIT":
            sys.exit(0)

        if choice == "UPDATE":
            try:
                print("Checking for updates...", flush=True)
                update_info = updater.check_for_update()
                if update_info:
                    tag, url = update_info
                    print(f"\nNew version available: {tag}")
                    if input("Do you want to update? (y/n): ").strip().lower().startswith('y'):
                        updater.update_application(url)
                        return # Should not be reached if update restarts, but good safety
                else:
                    print("No updates found.")
                    input("\nPress Enter to return to menu...")
            except Exception as e:
                print(f"Update check failed: {e}")
                input("\nPress Enter to return to menu...")
            continue
            
        break # Valid club selection made
    
    BATCH_SIZE = 5
    # MAX_RETRIES = 3 # Removed for infinite retry
    RETRY_DELAY = 5
    
    clubs_to_process = CLUBS if choice == "ALL" else {k: v for k, v in CLUBS.items() if v == choice}
    club_keys = list(clubs_to_process.keys())

        
    batches = [club_keys[i:i + BATCH_SIZE] for i in range(0, len(club_keys), BATCH_SIZE)]

    print(f"\nProcessing {len(club_keys)} clubs...\n", flush=True)
    
    total_failures = 0
    
    for batch_idx, batch_keys in enumerate(batches):
        print(f"Batch {batch_idx + 1}/{len(batches)}: Processing {len(batch_keys)} items...", flush=True)
        
        # Step 1: Parallel Fetch
        fetch_tasks = {key: asyncio.create_task(fetch_club_data(CLUBS[key])) for key in batch_keys}
        fetch_results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
        results_map = dict(zip(batch_keys, fetch_results))

        # Step 2: Parallel Export & Processing
        export_tasks = []
        for key in batch_keys:
            cfg = CLUBS[key]
            result = results_map[key]
            export_tasks.append(
                asyncio.create_task(
                    process_club_workflow(key, cfg, GC, result, RETRY_DELAY)
                )
            )
            
        # Wait for all exports in this batch to finish
        batch_outcomes = await asyncio.gather(*export_tasks)
        
        # Count failures
        failures = batch_outcomes.count(False)
        total_failures += failures
        
        print("") # Spacer between batches

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
    input("Press Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())