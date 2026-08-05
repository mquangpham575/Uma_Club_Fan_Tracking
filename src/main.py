import asyncio
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

# Load local environment variables from .env if present
load_dotenv()


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
        
    from config.globals import (
        CLUBS,
        SHEET_ID,
        # TEMP_SHEET_ID,  # temp sheet retired
        VERSION,
        effective_date,
        first_day_of_month,
    )
except ImportError as e:
    print(f"Error: 'globals.py' not found (Base path: {base_path}). Details: {e}")
    sys.exit(1)

# Zendriver compatibility patches removed (Chrono now uses direct API)

# Import Modules
from src.processing import build_dataframe  # noqa: E402
from src.sheets import (  # noqa: E402
    export_all_club_data_to_gsheets,
    export_to_gsheets,
    get_gspread_client,
    reorder_sheets,
)
from src.utils import (  # noqa: E402
    LogColor,
    clear_screen,
    colorize,
    setup_windows_console,
)

# Global locks to prevent concurrent resource exhaustion
SHEETS_LOCK = asyncio.Lock()
# Limits concurrent Chrono API requests (per-club fetch phase). Configurable via env.
API_SEMAPHORE = asyncio.Semaphore(int(os.getenv("API_CONCURRENCY", "3")))

# Throttling logic removed (Chrono now uses direct API)

# Sentinel returned when the API responded but has no history data yet (not an error).
NO_DATA = ("NO_DATA",)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        print(f"Warning: Invalid numeric value for {name}. Using default {default}.", flush=True)
        return default

# JSON saving removed (Now syncs directly to Google Sheets)
# Helper Functions

def pick_club() -> dict | str:
    if not CLUBS:
        print("Error: No active clubs loaded. Check the database configuration.", flush=True)
        sys.exit(1)

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
    print(f"\nInvalid selection: '{choice}'. Defaulting to ALL.", flush=True)
    return "ALL"

# Main Execution
async def process_club_workflow(
    cfg: dict,
    gc_client,
    retry_delay: int,
    max_attempts: int,
    per_club_timeout_seconds: int,
) -> tuple | None:
    # Handles the retry loop and processing for a single club.
    title = cfg["title"]
    attempt = 0
    sdate = cfg.get("sdate") or first_day_of_month
    fallback_attempted = False
    
    while attempt < max_attempts:
        try:
            from src.chrono_scraper import scrape_club_data
            
            cfg_to_use = cfg.copy()
            cfg_to_use["sdate"] = sdate
            
            async with API_SEMAPHORE:
                raw_data, status_code = await asyncio.wait_for(
                    scrape_club_data(cfg_to_use),
                    timeout=per_club_timeout_seconds
                )
            await asyncio.sleep(2.5)
            
            if status_code == 429:
                prefix = colorize("[Rate Limit]", LogColor.RETRY)
                print(f"  {prefix} {title}: 429 hit. Cool-down 30s...", flush=True)
                await asyncio.sleep(30)
                raise Exception("Rate limited")
            
            # Early-month fallback: only when the API responded 200 but the
            # current month's history is not populated yet. Any non-200 is a
            # retryable failure, NOT a signal to reuse last month's data.
            is_early_month = effective_date.day <= 3
            
            data = None
            if status_code == 200 and raw_data:
                try:
                    data = json.loads(raw_data)
                except (json.JSONDecodeError, ValueError) as je:
                    print(f"  [Parse Error] {title}: Invalid JSON from API (Status 200): {je}", flush=True)
            
            has_no_data = isinstance(data, dict) and not data.get("club_friend_history")
            if not fallback_attempted and is_early_month and status_code == 200 and has_no_data:
                try:
                    curr_dt = datetime.strptime(sdate, "%Y-%m-%d")
                    prev_month_date = curr_dt.replace(day=1) - timedelta(days=1)
                    prev_month_first_day = prev_month_date.replace(day=1).strftime("%Y-%m-%d")
                    
                    print(f"  [Fallback] {title}: Early month detected ({effective_date.strftime('%Y-%m-%d')}) and current month has no data yet. Falling back to previous month ({prev_month_first_day})...", flush=True)
                    sdate = prev_month_first_day
                    fallback_attempted = True
                    attempt = 0
                    continue
                except Exception as fe:
                    print(f"  [Fallback Error] Failed to calculate fallback date: {fe}", flush=True)

            if status_code != 200 or not raw_data:
                raise Exception(f"API fetch failed (Status {status_code})")
            if data is None:
                raise Exception("API returned invalid/unparseable JSON")
            if not isinstance(data, dict):
                raise Exception(f"API returned unexpected payload type: {type(data).__name__}")
            if data.get("detail") == "Error":
                raise Exception("API returned data error")

            if not data.get("club_friend_history"):
                prefix = colorize("[No Data]", LogColor.RETRY)
                print(f"  {prefix} {title}: No history data available in API yet. Skipping sheet update.", flush=True)
                return NO_DATA

            # Phase 2: Export to Sheets with 429 Retry logic
            # Join-map is best-effort: a fetch failure only disables pre-join graying
            # for this club, it must not force a retry of the main data.
            join_map = {}
            try:
                from src.chrono_scraper import scrape_club_join_map
                async with API_SEMAPHORE:
                    join_map = await asyncio.wait_for(
                        scrape_club_join_map(cfg_to_use),
                        timeout=per_club_timeout_seconds
                    )
            except Exception as e:
                print(f"  [Join Map] {title}: fetch failed ({e}). Continuing without pre-join graying.", flush=True)
            df = build_dataframe(data, join_map, sdate)

            # --- Temp sheet retired (was days 22-31 filter + separate export) ---
            # # Filter data for temp sheet (days 22 to 31)
            # import copy
            # temp_data = copy.deepcopy(data)
            # if "club_friend_history" in temp_data:
            #     temp_data["club_friend_history"] = [
            #         x for x in temp_data["club_friend_history"]
            #         if x.get("actual_date") is not None and 22 <= int(x.get("actual_date")) <= 31
            #     ]
            # if "club_daily_history" in temp_data:
            #     temp_data["club_daily_history"] = [
            #         x for x in temp_data["club_daily_history"]
            #         if x.get("actual_date") is not None and 22 <= int(x.get("actual_date")) <= 31
            #     ]
            # temp_df = build_dataframe(temp_data)

            async with SHEETS_LOCK:
                loop = asyncio.get_running_loop()
                
                # 1. Update normal sheet
                try:
                    await loop.run_in_executor(
                        None, 
                        export_to_gsheets, 
                        gc_client, df, SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                        data.get("club_daily_history"), cfg.get("club_id")
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
                            data.get("club_daily_history"), cfg.get("club_id")
                        )
                    else:
                        raise e
                await asyncio.sleep(3.0)

                # 2. Update temp sheet (retired)
                # try:
                #     await loop.run_in_executor(
                #         None, 
                #         export_to_gsheets, 
                #         gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                #         temp_data.get("club_daily_history"), cfg.get("club_id")
                #     )
                # except Exception as e:
                #     if "429" in str(e) or "500" in str(e):
                #         prefix = colorize("[Quota/Server]", LogColor.RETRY)
                #         print(f"  {prefix} {title} (Temp): Error ({e}). Waiting 30s for reset...", flush=True)
                #         await asyncio.sleep(30) 
                #         await loop.run_in_executor(
                #             None, 
                #             export_to_gsheets, 
                #             gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                #             temp_data.get("club_daily_history"), cfg.get("club_id")
                #         )
                #     else:
                #         raise e
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

            # Extract data for temp summary sheet (retired)
            # temp_day_cols = [c for c in temp_df.columns if isinstance(c, str) and c.startswith("Day ")]
            # temp_member_data = []
            # for _, row in temp_df.iterrows():
            #     temp_perf = row[temp_day_cols].sum() if temp_day_cols else 0.0
            #     temp_member_data.append({
            #         "member_name": row["Member_Name"],
            #         "avg_day": row["AVG/d"],
            #         "performance": temp_perf
            #     })
                
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

            # temp_rank = ""  # temp sheet retired
            # temp_daily_history = temp_data.get("club_daily_history") or []
            # if temp_daily_history:
            #     try:
            #         latest_entry = max(temp_daily_history, key=lambda x: int(x.get("actual_date", 0)))
            #         rank_val = latest_entry.get("rank")
            #         if rank_val is not None:
            #             temp_rank = f"#{rank_val}"
            #     except Exception:
            #         rank_val = temp_daily_history[-1].get("rank")
            #         if rank_val is not None:
            #             temp_rank = f"#{rank_val}"
                        
            club_metadata = {
                "short_name": short_name,
                "grade": grade,
                "rank": rank,
                "members": member_data
            }

            # temp_club_metadata = {
            #     "short_name": short_name,
            #     "grade": grade,
            #     "rank": temp_rank,
            #     "members": temp_member_data
            # }
            return club_metadata, sdate
            
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

async def fetch_db_active_clubs(database_url: str, check_date, guild_id: str = None) -> list:
    """
    Fetch active clubs and their daily quotas from the database for the given date.
    
    Intent:
        Retrieve circle ID, name, and current daily quota for all active clubs in the database.
    """
    import asyncpg
    conn = None
    try:
        # SSH-tunneled databases can refuse the very first connection attempt;
        # retry transient network errors (ConnectionRefusedError subclasses OSError).
        last_err = None
        for attempt in range(3):
            try:
                conn = await asyncpg.connect(database_url)
                break
            except OSError as e:
                last_err = e
                print(f"  [Retry] DB connect refused (attempt {attempt + 1}/3): {e}", flush=True)
                await asyncio.sleep(1 + attempt * 2)
        if conn is None:
            raise last_err
        if guild_id:
            query = """
                SELECT c.circle_id, c.club_name, c.quota_period,
                       COALESCE(
                           (SELECT daily_quota FROM quota_requirements qr 
                            WHERE qr.club_id = c.club_id AND qr.effective_date <= $1 
                            ORDER BY qr.effective_date DESC LIMIT 1),
                           c.daily_quota
                       ) as quota
                FROM clubs c
                WHERE c.circle_id IS NOT NULL 
                  AND c.is_active = TRUE 
                  AND c.guild_id = $2
            """
            rows = await conn.fetch(query, check_date, int(guild_id))
        else:
            query = """
                SELECT c.circle_id, c.club_name, c.quota_period,
                       COALESCE(
                           (SELECT daily_quota FROM quota_requirements qr 
                            WHERE qr.club_id = c.club_id AND qr.effective_date <= $1 
                            ORDER BY qr.effective_date DESC LIMIT 1),
                           c.daily_quota
                       ) as quota
                FROM clubs c
                WHERE c.circle_id IS NOT NULL AND c.is_active = TRUE
            """
            rows = await conn.fetch(query, check_date)
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Error: Failed to fetch active clubs from database: {e}.", flush=True)
        raise e
    finally:
        if conn:
            await conn.close()


async def export_summary_with_retry(gc_client, spreadsheet_id: str, all_clubs_data: list, sdate: str, label: str, max_attempts: int = 3) -> bool:
    """Exports the summary sheet, retrying with a 30s cool-down on 429/500."""
    loop = asyncio.get_running_loop()
    for attempt in range(max_attempts):
        try:
            await loop.run_in_executor(None, export_all_club_data_to_gsheets, gc_client, spreadsheet_id, all_clubs_data, sdate)
            return True
        except Exception as e:
            if "429" in str(e) or "500" in str(e):
                print(f"  [Quota/Server] {label} summary: Error ({e}). Waiting 30s for reset...", flush=True)
                await asyncio.sleep(30)
            else:
                print(f"Warning: Failed to update {label} summary sheet: {e}", flush=True)
                return False
    print(f"Warning: Failed to update {label} summary sheet after retries.", flush=True)
    return False


async def reorder_sheets_with_retry(gc_client, spreadsheet_id: str, ordered_titles: list, label: str, max_attempts: int = 3) -> bool:
    """Reorders sheets, retrying with escalating cool-downs on 429/500."""
    loop = asyncio.get_running_loop()
    for attempt in range(max_attempts):
        try:
            await loop.run_in_executor(None, reorder_sheets, gc_client, spreadsheet_id, ordered_titles)
            return True
        except Exception as e:
            if "429" in str(e) or "500" in str(e):
                wait = 30 * (attempt + 1)
                print(f"  [Quota] {label} Reordering hit limit ({e}). Waiting {wait}s...", flush=True)
                await asyncio.sleep(wait)
            else:
                print(f"Warning: Failed to reorder {label} sheets: {e}", flush=True)
                return False
    print(f"Warning: Failed to reorder {label} sheets after retries.", flush=True)
    return False


async def main():
    setup_windows_console(VERSION)
    is_cron = "--cron" in sys.argv
    
    # Startup
    if not is_cron:
        print(f"Starting Endless v{VERSION}...", flush=True)

    # Initialize Google Sheets Client
    GC = get_gspread_client(base_path)

    if not SHEET_ID:
        print("Error: SHEET_ID must be configured (via .env or config/globals.py).", flush=True)
        sys.exit(1)
    
    # Load dynamic quotas and active clubs from UmaCore PostgreSQL database
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        umacore_env_path = os.path.abspath(os.path.join(base_path, "..", "UmaCore", ".env"))
        if os.path.exists(umacore_env_path):
            try:
                from dotenv import dotenv_values
                env_vals = dotenv_values(umacore_env_path)
                database_url = env_vals.get("DATABASE_URL")
            except Exception:
                pass

    if not database_url:
        print("Error: DATABASE_URL must be configured. Database connectivity is required.", flush=True)
        sys.exit(1)

    try:
        from config.globals import SERVER_ID
        db_clubs = await fetch_db_active_clubs(database_url, effective_date.date(), SERVER_ID)
    except Exception as e:
        print(f"Fatal error: Database connection or query failed: {e}. Exiting.", flush=True)
        sys.exit(1)

    # Match circle_id and build new sorted dict
    new_clubs = {}
    
    # Compute daily equivalent threshold for each club based on quota_period
    # This aligns with column C (AVG/d) comparison in the spreadsheet
    parsed_db_clubs = []
    for club in db_clubs:
        cid = str(club['circle_id'])
        quota = int(club['quota'])
        cname = club['club_name']
        period = club.get('quota_period', 'daily')
        
        if period == 'daily':
            threshold = quota
        elif period == 'weekly':
            threshold = int(quota / 7.0)
        elif period == 'biweekly':
            threshold = int(quota / 14.0)
        else:
            threshold = int(quota / 30.0)  # Monthly normalization
            
        parsed_db_clubs.append({
            "circle_id": cid,
            "club_name": cname,
            "quota": quota,
            "threshold": threshold,
        })
        
    # Sort by computed threshold descending, then by name
    parsed_db_clubs.sort(key=lambda x: (-x['threshold'], x['club_name']))
    
    idx = 1
    for club in parsed_db_clubs:
        cid = club['circle_id']
        threshold = club['threshold']
        cname = club['club_name']
        
        title = cname
        
        new_clubs[str(idx)] = {
            "title": title,
            "club_id": cid,
            "THRESHOLD": threshold,
            "sdate": first_day_of_month
        }
        idx += 1
        
    print(f"Loaded {len(new_clubs)} active clubs from database in quota-sorted order.", flush=True)
    CLUBS.clear()
    CLUBS.update(new_clubs)
    
    # Rename sheets based on stored circle_id (CID) if name changed, then delete stale worksheets
    cid_to_active_cfg = {cfg['club_id']: cfg for cfg in CLUBS.values()}
    
    try:
        ss = GC.open_by_key(SHEET_ID)
        all_worksheets = ss.worksheets()
        ws_by_id = {ws.id: ws for ws in all_worksheets}
        
        # Read CID for each sheet to discover renames
        # Read CID for each sheet to discover renames via a single batchGet call
        sheet_to_cid = {}
        print("Scanning worksheet IDs to check for name changes...", flush=True)
        scan_sheets = [ws for ws in all_worksheets if ws.title != "All Club Data"]
        if scan_sheets:
            ranges = [f"'{ws.title}'!A1:A" for ws in scan_sheets]
            try:
                batch_resp = ss.values_batch_get(ranges)
                title_to_ws = {ws.title: ws for ws in scan_sheets}
                for vr in batch_resp.get("valueRanges", []):
                    sheet_name = vr.get("range", "").split("!")[0].strip("'")
                    ws = title_to_ws.get(sheet_name)
                    if ws is None:
                        continue
                    col_a = [row[0] for row in vr.get("values", []) if row]
                    for val in col_a:
                        if val and str(val).startswith("CID:"):
                            cid = str(val).split("CID:")[1].strip()
                            sheet_to_cid[ws.id] = cid
                            break
            except Exception as ex:
                print(f"Warning: Failed to batch-read worksheet CIDs: {ex}", flush=True)
            
        # Process renames
        for ws_id, cid in sheet_to_cid.items():
            ws = ws_by_id.get(ws_id)
            if ws is None or cid not in cid_to_active_cfg:
                continue
            target_title = cid_to_active_cfg[cid]['title']
            if ws.title != target_title:
                print(f"Renaming worksheet '{ws.title}' to '{target_title}' (Circle ID: {cid})...", flush=True)
                try:
                    ws.update_title(target_title)
                    print(f"Successfully renamed worksheet to '{target_title}'.", flush=True)
                    ws.title = target_title
                except Exception as ex:
                    print(f"Warning: Failed to rename worksheet '{ws.title}' to '{target_title}': {ex}", flush=True)
        
        # Refresh the worksheets list after renaming and match by stable id
        for ws in ss.worksheets():
            title = ws.title
            if title == "All Club Data":
                continue
            
            # If this sheet is in our CID list, it's a club sheet.
            # Check if its CID is in the currently active database config.
            sheet_cid = sheet_to_cid.get(ws.id)
            if sheet_cid is not None and sheet_cid not in cid_to_active_cfg:
                print(f"Detected deactivated club sheet '{title}' (Circle ID: {sheet_cid}). Deleting worksheet...", flush=True)
                try:
                    ss.del_worksheet(ws)
                    print(f"Deleted worksheet '{title}'.", flush=True)
                except Exception as ex:
                    print(f"Warning: Failed to delete worksheet '{title}': {ex}", flush=True)
    except Exception as e:
        print(f"Warning: Failed to perform stale sheet cleanup & renames: {e}", flush=True)
    
    # Engine is now exclusively Chrono
    engine_choice = "CHRONO"
    if is_cron:
        choice = "ALL"
    else:
        choice = pick_club()
        clear_screen()
        if choice == "EXIT":
            sys.exit(0)


    RETRY_DELAY = _env_int("CHRONO_RETRY_DELAY", 5)
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
    successful_results = []
    concurrency = max(1, _env_int("CLUB_CONCURRENCY", 4))
    print(f"\nProcessing {len(clubs_to_process)} clubs (Engine: {engine_choice}, concurrency: {concurrency})...\n", flush=True)

    items = list(clubs_to_process.items())
    if not items:
        print("No clubs to process.", flush=True)
    else:
        sem = asyncio.Semaphore(concurrency)

        async def _process_one(cfg):
            async with sem:
                return await process_club_workflow(cfg, GC, RETRY_DELAY, 5, 90)

        outcomes = await asyncio.gather(*(_process_one(cfg) for _, cfg in items))

        for outcome in outcomes:
            if outcome == NO_DATA:
                continue
            if outcome is not None and isinstance(outcome, tuple) and len(outcome) == 2:
                normal_outcome, resolved_sdate = outcome
                if normal_outcome:
                    successful_results.append((resolved_sdate, normal_outcome))
            else:
                total_failures += 1

    if choice == "ALL" and successful_results:
        # Exclude clubs that resolved to a different month than the majority so the
        # dashboard never mixes months (e.g. early-month fallback to the previous month).
        sdate_counts = {}
        for sdate, _ in successful_results:
            sdate_counts[sdate] = sdate_counts.get(sdate, 0) + 1
        summary_sdate = max(sdate_counts, key=lambda s: (sdate_counts[s], s))
        conforming = [r for r in successful_results if r[0] == summary_sdate]
        excluded = len(successful_results) - len(conforming)
        if excluded:
            print(f"  Excluded {excluded} club(s) from summary (resolved to a different month than {summary_sdate}).", flush=True)
        successful_clubs = [r[1] for r in conforming]

        if successful_clubs:
            print("Exporting All Club Data summary sheet...", flush=True)
            if await export_summary_with_retry(GC, SHEET_ID, successful_clubs, summary_sdate, "All Club Data"):
                print("All Club Data summary sheet updated.", flush=True)

        # Temp All Club Data summary sheet retired:
        # temp_successful_clubs = [r[2] for r in conforming]
        # if temp_successful_clubs:
        #     print("Exporting Temp All Club Data summary sheet...", flush=True)
        #     dt_summary = datetime.strptime(summary_sdate, "%Y-%m-%d")
        #     temp_sdate = dt_summary.replace(day=22).strftime("%Y-%m-%d")
        #     if await export_summary_with_retry(GC, TEMP_SHEET_ID, temp_successful_clubs, temp_sdate, "Temp All Club Data"):
        #         print("Temp All Club Data summary sheet updated.", flush=True)

    # Reordering is now always the final step after the parallel gather
    print("Reordering sheets...", flush=True)
    ordered_titles = ["All Club Data"] + [CLUBS[k]['title'] for k in CLUBS]
    await reorder_sheets_with_retry(GC, SHEET_ID, ordered_titles, "")
    # await reorder_sheets_with_retry(GC, TEMP_SHEET_ID, ordered_titles, "Temp")
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