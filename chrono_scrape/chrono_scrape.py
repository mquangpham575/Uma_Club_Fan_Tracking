import asyncio
import os
import sys
import json

async def scrape_club_data(cfg: dict, zd):
    # Determine the best ID or term to use
    search_id = cfg.get('CLUB_ID_STARTING') or cfg.get('club_id')

    browser = await zd.start(
        browser="edge",
        browser_executable_path="C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe",
    )
    
    captured_requests = []

    async def resp_handler(*args, **kwargs):
        if args and hasattr(args[0], 'response'):
             url = args[0].response.url
             if "api.chronogenesis.net/club_profile" in url:
                 captured_requests.append(args[0].request_id)

    page = await browser.get("https://chronogenesis.net/")
    await page.send(zd.cdp.network.enable())
    page.add_handler(zd.cdp.network.ResponseReceived, resp_handler)

    club_profile = await page.select_all(".home-menu-item")
    await club_profile[1].click()
    await asyncio.sleep(2)

    search_box = await page.select(".club-id-input", timeout=20)
    await search_box.send_keys(search_id) # Use exact ID for backup accuracy
    await search_box.send_keys(zd.SpecialKeys.ENTER)
    await asyncio.sleep(3)

    try:
        results = await page.select_all(".club-results-row", timeout=10)
        for result in results:
            content = result.text_all.lower()
            if search_id in content: # Match by ID
                await result.click()
                break
    except Exception:
        pass

    await asyncio.sleep(8) 

    largest_response = None
    largest_size = 0
    for req_id in captured_requests:
        try:
             response_body, _ = await page.send(
                zd.cdp.network.get_response_body(request_id=req_id)
            )
             if len(response_body) > largest_size:
                 largest_size = len(response_body)
                 largest_response = response_body
        except Exception:
            pass

    await browser.stop()
    return largest_response

async def main():
    print("Starting Chrono Scraper...", flush=True)
    try:
        # Import Globals and Modules inside try to catch bundling issues
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        if base_path not in sys.path:
            sys.path.append(base_path)
            
        import zendriver as zd
        from config.globals_chrono import CLUBS, SHEET_ID, VERSION
        from src.processing import build_dataframe
        from src.sheets import export_to_gsheets, get_gspread_client
        from src.utils import clear_screen, setup_windows_console

        setup_windows_console(VERSION)
        
        # Handle Optional Command Line Arg
        choice = None
        if len(sys.argv) > 1:
            if sys.argv[1].isdigit() and sys.argv[1] in CLUBS:
                choice = sys.argv[1]
            elif sys.argv[1] == "--club" and len(sys.argv) > 2 and sys.argv[2] in CLUBS:
                choice = sys.argv[2]

        # Auto-pick "1" if only one club and no choice
        if not choice and len(CLUBS) == 1:
            choice = "1"

        if not choice:
            clear_screen()
            choice = input("Select Club: ").strip()

        if choice not in CLUBS:
            return

        cfg = CLUBS[choice]
        raw_data = await scrape_club_data(cfg, zd)

        if not raw_data:
            return

        data = json.loads(raw_data)
        df = build_dataframe(data)
        
        # Export to Sheets
        GC = get_gspread_client(base_path)
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, 
            export_to_gsheets, 
            GC, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
            data.get("club_daily_history")
        )
        
        print("✅ Done!", flush=True)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        # Keep window open on Windows
        if sys.platform == 'win32':
             input("\nPress Enter to close...")

if __name__ == "__main__":
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())