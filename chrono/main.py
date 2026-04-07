import asyncio
import os
import sys
import json

def get_hotkey() -> str:
    # Captures a single keypress on Windows, or falls back to input()
    if sys.platform == 'win32':
        import msvcrt
        while True:
            char = msvcrt.getwch()
            if char == '\r' or char == '\n':
                return "ENTER"
            if char == '\b':
                return "BACKSPACE"
            if char.isprintable():
                print(char) # Echo the choice
                return char.lower()
    return input().strip().lower() or "ENTER"

async def main():
    print("Starting Chrono Scraper...", flush=True)
    try:
        # Import Globals and Modules inside try to catch bundling issues
        if getattr(sys, 'frozen', False):
            # When frozen (EXE), prioritize finding globals_*.py in the SAME folder as the EXE.
            exe_dir = os.path.dirname(os.path.abspath(sys.executable))
            try:
                external_globals = [f for f in os.listdir(exe_dir) if f.startswith("globals_") and f.endswith(".py")]
            except Exception:
                external_globals = []

            if external_globals:
                base_path = exe_dir
                current_dir = exe_dir
            else:
                base_path = sys._MEIPASS
                current_dir = os.path.join(base_path, 'chrono')
        else:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            current_dir = os.path.dirname(os.path.abspath(__file__))
        
        if base_path not in sys.path:
            sys.path.insert(0, base_path)
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
            
        import zendriver as zd
        import importlib.util
        from chrono_scraper import scrape_club_data
        from src.processing import build_dataframe
        from src.sheets import export_to_gsheets, get_gspread_client
        from src.utils import clear_screen, setup_windows_console

        while True:
            # Re-detect available globals files for dynamism
            globals_files = [f[:-3] for f in os.listdir(current_dir) if f.startswith("globals_") and f.endswith(".py")]
            
            if not globals_files:
                print("FATAL ERROR: No globals_*.py files found in the directory.")
                return

            # Ensure Uma Vault is Option 1 if it exists
            globals_files.sort(key=lambda x: 0 if "uma_vault" in x.lower() else 1)

            clear_screen()
            print("Select Configuration:")
            print("-" * 30)
            for i, f in enumerate(globals_files, 1):
                name = f.replace('globals_', '').replace('_', ' ').title()
                print(f"[{i}] {name}")
            print("-" * 30)
            print("[Q] Exit Application")
            
            print("\nSelection: ", end="", flush=True)
            g_choice = get_hotkey()
            if g_choice == 'q':
                break

            if g_choice == "ENTER" or not g_choice.isdigit():
                selected_globals_module = globals_files[0]
            elif 1 <= int(g_choice) <= len(globals_files):
                selected_globals_module = globals_files[int(g_choice) - 1]
            else:
                selected_globals_module = globals_files[0]

            # ENSURE we load from the ACTUAL file in current_dir (crucial for EXE portability)
            filepath = os.path.join(current_dir, f"{selected_globals_module}.py")
            spec = importlib.util.spec_from_file_location(selected_globals_module, filepath)
            g_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(g_mod)
            
            CLUBS = getattr(g_mod, "CLUBS")
            SHEET_ID = getattr(g_mod, "SHEET_ID")
            setup_windows_console("ChronoScraper")
            
            # Handle Optional Command Line Arg (ONLY on first run or if provided)
            choice = None
            if len(sys.argv) > 1:
                if sys.argv[1].isdigit() and sys.argv[1] in CLUBS:
                    choice = sys.argv[1]
                elif sys.argv[1] == "--club" and len(sys.argv) > 2 and sys.argv[2] in CLUBS:
                    choice = sys.argv[2]
                sys.argv = [sys.argv[0]]

            # Auto-pick "1" if only one club and no choice
            if not choice and len(CLUBS) == 1:
                choice = "1"

            if not choice:
                clear_screen()
                config_name = selected_globals_module.replace('globals_', '').replace('_', ' ').title()
                print(f"Configuration: {config_name}")
                print("-" * 30)
                for k, v in CLUBS.items():
                    print(f"[{k}] {v['title']}")
                print("-" * 30)
                print("[Enter] Run All")
                print("[B] Back to Selection")
                
                print("\nSelection: ", end="", flush=True)
                choice = get_hotkey()
                if choice == "b" or choice == "BACKSPACE":
                    continue
                if choice == "ENTER":
                    choice = "ALL"

            clubs_to_run = []
            if choice == "ALL":
                clubs_to_run = list(CLUBS.values())
            elif choice in CLUBS:
                clubs_to_run = [CLUBS[choice]]
            else:
                print("\nInvalid selection. Returning to menu...")
                await asyncio.sleep(1)
                continue

            # Match main application's batch processing logic
            BATCH_SIZE = 5
            # Ensure clubs are sorted by their selection key
            sorted_keys = sorted([k for k in CLUBS.keys() if (choice == "ALL" or k == choice)], key=lambda x: int(x) if x.isdigit() else x)
            batches = [sorted_keys[i:i + BATCH_SIZE] for i in range(0, len(sorted_keys), BATCH_SIZE)]

            async def _run_and_export(key):
                cfg = CLUBS[key]
                print(f"\n>>> Processing: {cfg['title']}...", flush=True)
                try:
                    raw_data = await scrape_club_data(cfg, zd)
                    if not raw_data:
                        print(f"  FAILED to scrape {cfg['title']}", flush=True)
                        return False

                    # Save raw JSON locally
                    json_dir = os.path.join(current_dir, "json_data")
                    os.makedirs(json_dir, exist_ok=True)
                    json_path = os.path.join(json_dir, f"{cfg['title'].replace('/', '_').replace('\\', '_')}.json")
                    with open(json_path, "w", encoding="utf-8") as f:
                        f.write(raw_data)
                    print(f"  Saved JSON: {os.path.basename(json_path)}", flush=True)

                    data = json.loads(raw_data)
                    df = build_dataframe(data)
                    
                    # Export to Sheets (blocking I/O)
                    # Resolve credentials location (same folder if standalone EXE, 'chrono' folder if root)
                    creds_folder = 'chrono' if base_path != current_dir else '.'
                    GC = get_gspread_client(base_path, creds_folder)
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, 
                        export_to_gsheets, 
                        GC, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history")
                    )
                    print(f"  Success: {cfg['title']}", flush=True)
                    return True
                except Exception as e:
                    print(f"  ERROR processing {cfg['title']}: {e}", flush=True)
                    return False

            if sorted_keys:
                total_success = 0
                for batch_idx, batch_keys in enumerate(batches):
                    if len(batches) > 1:
                        print(f"\n--- Batch {batch_idx + 1}/{len(batches)} ---", flush=True)
                    
                    # Run all items in this batch in parallel (max 5)
                    results = await asyncio.gather(*[_run_and_export(k) for k in batch_keys])
                    total_success += results.count(True)
                
                print(f"\n✅ All tasks done! ({total_success}/{len(sorted_keys)} succeeded)", flush=True)
            print("\nPress Q to quit or any other key for menu: ", end="", flush=True)
            res = get_hotkey()
            if res == 'q':
                break

    except Exception as e:
        print(f"\nFATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())