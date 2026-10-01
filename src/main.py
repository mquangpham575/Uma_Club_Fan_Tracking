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
        TEMP_SHEET_ID,
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
    temp_only: bool = False,
) -> tuple | None:
    # Handles the retry loop and processing for a single club.
    title = cfg["title"]
    attempt = 0
    sdate = cfg.get("sdate") or first_day_of_month
    fallback_attempted = False

    if temp_only:
        if not TEMP_SHEET_ID:
            print("Error: TEMP_SHEET_ID not configured.", flush=True)
            return None

        while attempt < max_attempts:
            try:
                from src.chrono_scraper import scrape_club_data, scrape_club_join_map

                curr_dt = datetime.strptime(sdate, "%Y-%m-%d")
                prev_sdate = (curr_dt.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
                cfg_prev = cfg.copy()
                cfg_prev["sdate"] = prev_sdate

                async with API_SEMAPHORE:
                    raw_prev_data, prev_status = await asyncio.wait_for(
                        scrape_club_data(cfg_prev),
                        timeout=per_club_timeout_seconds
                    )
                await asyncio.sleep(2.5)

                if prev_status == 429:
                    prefix = colorize("[Rate Limit]", LogColor.RETRY)
                    print(f"  {prefix} {title} (Temp): 429 hit. Cool-down 30s...", flush=True)
                    await asyncio.sleep(30)
                    raise Exception("Rate limited")

                if prev_status != 200 or not raw_prev_data:
                    raise Exception(f"API fetch failed (Status {prev_status})")

                prev_data = json.loads(raw_prev_data)
                if not isinstance(prev_data, dict) or not prev_data.get("club_friend_history"):
                    prefix = colorize("[No Data]", LogColor.RETRY)
                    print(f"  {prefix} {title} (Temp): No history data available for {prev_sdate}. Skipping.", flush=True)
                    return NO_DATA

                join_map = {}
                try:
                    async with API_SEMAPHORE:
                        join_map = await asyncio.wait_for(
                            scrape_club_join_map(cfg_prev),
                            timeout=per_club_timeout_seconds
                        )
                except Exception as e:
                    print(f"  [Join Map] {title}: fetch failed ({e}). Continuing without pre-join graying.", flush=True)

                temp_df = build_dataframe(prev_data, join_map, prev_sdate)

                async with SHEETS_LOCK:
                    loop = asyncio.get_running_loop()
                    try:
                        await loop.run_in_executor(
                            None,
                            export_to_gsheets,
                            gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                            prev_data.get("club_daily_history"), cfg.get("club_id")
                        )
                    except Exception as e:
                        if "429" in str(e) or "500" in str(e):
                            prefix = colorize("[Quota/Server]", LogColor.RETRY)
                            print(f"  {prefix} {title} (Temp): Error ({e}). Waiting 30s for reset...", flush=True)
                            await asyncio.sleep(30)
                            await loop.run_in_executor(
                                None,
                                export_to_gsheets,
                                gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                                prev_data.get("club_daily_history"), cfg.get("club_id")
                            )
                        else:
                            raise e
                    await asyncio.sleep(3.0)

                prefix = colorize("[Success]", LogColor.SUCCESS)
                print(f"  {prefix} {title} (Temp)", flush=True)

                if "(" in title and ")" in title:
                    short_name = title.split("(")[0].strip()
                    grade = title.split("(")[1].split(")")[0].strip()
                else:
                    short_name = title
                    grade = ""

                temp_day_cols = [c for c in temp_df.columns if isinstance(c, str) and c.startswith("Day ")]
                temp_member_data = []
                for _, row in temp_df.iterrows():
                    temp_perf = row[temp_day_cols].sum() if temp_day_cols else 0.0
                    temp_member_data.append({
                        "member_name": row["Member_Name"],
                        "avg_day": row["AVG/d"],
                        "performance": temp_perf
                    })

                temp_rank = ""
                temp_daily_history = prev_data.get("club_daily_history") or []
                if temp_daily_history:
                    try:
                        latest_entry = max(temp_daily_history, key=lambda x: int(x.get("actual_date", 0)))
                        rank_val = latest_entry.get("rank")
                        if rank_val is not None:
                            temp_rank = f"#{rank_val}"
                    except Exception:
                        rank_val = temp_daily_history[-1].get("rank")
                        if rank_val is not None:
                            temp_rank = f"#{rank_val}"

                temp_club_metadata = {
                    "short_name": short_name,
                    "grade": grade,
                    "rank": temp_rank,
                    "members": temp_member_data
                }
                return None, None, temp_club_metadata, prev_sdate

            except Exception as e:
                import traceback
                traceback.print_exc()
                attempt_no = attempt + 1
                prefix = colorize("[Error]", LogColor.ERROR)
                print(f"  {prefix} on {title} (Temp Attempt {attempt_no}): {e}", flush=True)

                attempt += 1
                if attempt >= max_attempts:
                    return None

                delay = retry_delay + random.uniform(1, 4)
                prefix = colorize("[Retry]", LogColor.RETRY)
                print(f"  {prefix} {title} (Temp): sleeping {delay:.1f}s before attempt {attempt + 1}...", flush=True)
                await asyncio.sleep(delay)
    
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

            # Fetch previous month data for temp sheet if TEMP_SHEET_ID configured
            temp_df = None
            temp_data = None
            prev_sdate = None
            if TEMP_SHEET_ID:
                try:
                    curr_dt = datetime.strptime(sdate, "%Y-%m-%d")
                    prev_sdate = (curr_dt.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
                    cfg_prev = cfg.copy()
                    cfg_prev["sdate"] = prev_sdate

                    async with API_SEMAPHORE:
                        raw_prev_data, prev_status = await asyncio.wait_for(
                            scrape_club_data(cfg_prev),
                            timeout=per_club_timeout_seconds
                        )
                    await asyncio.sleep(2.5)

                    if prev_status == 200 and raw_prev_data:
                        try:
                            prev_data = json.loads(raw_prev_data)
                            if isinstance(prev_data, dict) and prev_data.get("club_friend_history"):
                                temp_data = prev_data
                                temp_df = build_dataframe(prev_data, join_map, prev_sdate)
                        except (json.JSONDecodeError, ValueError) as pje:
                            print(f"  [Parse Error] {title} (Temp): Invalid JSON from API: {pje}", flush=True)
                except Exception as pfe:
                    print(f"  [Temp Fetch Error] {title}: Failed to fetch previous month data ({prev_sdate}): {pfe}", flush=True)

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

                # 2. Update temp sheet with full previous month data
                if temp_df is not None and TEMP_SHEET_ID:
                    try:
                        await loop.run_in_executor(
                            None, 
                            export_to_gsheets, 
                            gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                            temp_data.get("club_daily_history"), cfg.get("club_id")
                        )
                    except Exception as e:
                        if "429" in str(e) or "500" in str(e):
                            prefix = colorize("[Quota/Server]", LogColor.RETRY)
                            print(f"  {prefix} {title} (Temp): Error ({e}). Waiting 30s for reset...", flush=True)
                            await asyncio.sleep(30) 
                            await loop.run_in_executor(
                                None, 
                                export_to_gsheets, 
                                gc_client, temp_df, TEMP_SHEET_ID, cfg['title'], cfg["THRESHOLD"],
                                temp_data.get("club_daily_history"), cfg.get("club_id")
                            )
                        else:
                            print(f"  [Temp Export Error] {title}: Failed to export to temp sheet: {e}", flush=True)
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

            temp_club_metadata = None
            if temp_df is not None and temp_data is not None:
                temp_day_cols = [c for c in temp_df.columns if isinstance(c, str) and c.startswith("Day ")]
                temp_member_data = []
                for _, row in temp_df.iterrows():
                    temp_perf = row[temp_day_cols].sum() if temp_day_cols else 0.0
                    temp_member_data.append({
                        "member_name": row["Member_Name"],
                        "avg_day": row["AVG/d"],
                        "performance": temp_perf
                    })

                temp_rank = ""
                temp_daily_history = temp_data.get("club_daily_history") or []
                if temp_daily_history:
                    try:
                        latest_entry = max(temp_daily_history, key=lambda x: int(x.get("actual_date", 0)))
                        rank_val = latest_entry.get("rank")
                        if rank_val is not None:
                            temp_rank = f"#{rank_val}"
                    except Exception:
                        rank_val = temp_daily_history[-1].get("rank")
                        if rank_val is not None:
                            temp_rank = f"#{rank_val}"

                temp_club_metadata = {
                    "short_name": short_name,
                    "grade": grade,
                    "rank": temp_rank,
                    "members": temp_member_data
                }

            return club_metadata, sdate, temp_club_metadata, prev_sdate
            
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
    Fetch active clubs and their daily quotas from the database for the given date and guild.
    
    Intent:
        Retrieve circle ID, name, and current daily quota for all active clubs in the target guild.
    """
    ssh_key = os.path.abspath(os.path.join(base_path, "..", "UmaCore", ".ssh", "umacore_key"))
    if os.path.exists(ssh_key):
        import json
        import subprocess
        
        guild_clause = f"AND c.guild_id = {int(guild_id)}" if guild_id else ""
        sql = (
            "SELECT json_agg(t) FROM ("
            "SELECT c.circle_id, c.club_name, c.quota_period, COALESCE(qr.daily_quota, c.daily_quota) AS quota "
            "FROM clubs c "
            "LEFT JOIN quota_requirements qr ON qr.club_id = c.club_id "
            f"WHERE c.is_active = true AND c.circle_id IS NOT NULL AND c.circle_id != '' {guild_clause} "
            "ORDER BY c.circle_id"
            ") t;"
        )
        cmd = [
            "ssh",
            "-i", ssh_key,
            "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "umacore@20.212.105.13",
            f'docker exec umacore-postgres psql -U umacore -t -A -c "{sql}"'
        ]
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15, creationflags=flags)
            if res.returncode == 0 and res.stdout.strip():
                data = json.loads(res.stdout.strip())
                if isinstance(data, list):
                    active_clubs = {}
                    for r in data:
                        cid = str(r['circle_id'])
                        if cid not in active_clubs or r.get('quota') is not None:
                            active_clubs[cid] = {
                                'circle_id': cid,
                                'club_name': r['club_name'],
                                'quota': r.get('quota') or 0,
                                'quota_period': r.get('quota_period') or 'daily'
                            }
                    return list(active_clubs.values())
        except Exception as se:
            print(f"  [SSH DB Query] Notice: SSH direct query failed ({se}), trying TCP fallback...", flush=True)

    if not database_url:
        raise Exception("Database URL not configured")

    import asyncpg
    conn = None
    try:
        conn = await asyncpg.connect(database_url)
        where_clauses = ["c.is_active = TRUE", "c.circle_id IS NOT NULL", "c.circle_id != ''"]
        params = []
        if guild_id:
            where_clauses.append(f"c.guild_id = ${len(params) + 1}")
            params.append(int(guild_id))
            
        where_sql = " AND ".join(where_clauses)
        query = f"""
            SELECT c.circle_id, c.club_name, c.quota_period,
                   COALESCE(qr.daily_quota, c.daily_quota) as quota
            FROM clubs c
            LEFT JOIN quota_requirements qr ON qr.club_id = c.club_id
            WHERE {where_sql}
            ORDER BY c.circle_id
        """
        rows = await conn.fetch(query, *params)
        active_clubs = {}
        for r in rows:
            cid = str(r['circle_id'])
            if cid not in active_clubs or r['quota'] is not None:
                active_clubs[cid] = {
                    'circle_id': cid,
                    'club_name': r['club_name'],
                    'quota': r['quota'] or 0,
                    'quota_period': r['quota_period'] or 'daily'
                }
        return list(active_clubs.values())
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
    is_temp_only = "--temp-only" in sys.argv or "--temp" in sys.argv
    
    # Startup
    if not is_cron:
        mode_str = " (Temp Sheet Only)" if is_temp_only else ""
        print(f"Starting Endless v{VERSION}{mode_str}...", flush=True)

    # Initialize Google Sheets Client
    GC = get_gspread_client(base_path)

    target_sheet = TEMP_SHEET_ID if is_temp_only else SHEET_ID
    if not target_sheet:
        sheet_var_name = "TEMP_SHEET_ID" if is_temp_only else "SHEET_ID"
        print(f"Error: {sheet_var_name} must be configured (via .env or config/globals.py).", flush=True)
        sys.exit(1)
    
    # Load dynamic quotas and active clubs from UmaCore PostgreSQL database if available
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

    db_clubs = None
    try:
        from config.globals import SERVER_ID
        db_clubs = await fetch_db_active_clubs(database_url, effective_date.date(), SERVER_ID)
    except Exception as e:
        print(f"Error: Database query failed ({e}).", flush=True)
        sys.exit(1)

    if db_clubs:
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
                threshold = int(quota / 30.0)
                
            parsed_db_clubs.append({
                "circle_id": cid,
                "club_name": cname,
                "quota": quota,
                "threshold": threshold,
            })
            
        parsed_db_clubs.sort(key=lambda x: (-x['threshold'], x['club_name']))
        
        new_clubs = {}
        idx = 1
        for club in parsed_db_clubs:
            cid = club['circle_id']
            threshold = club['threshold']
            cname = club['club_name']
            
            new_clubs[str(idx)] = {
                "title": cname,
                "club_id": cid,
                "THRESHOLD": threshold,
                "sdate": first_day_of_month
            }
            idx += 1
            
        print(f"Loaded {len(new_clubs)} active clubs from database in quota-sorted order.", flush=True)
        CLUBS.clear()
        CLUBS.update(new_clubs)
    elif CLUBS:
        # Sort existing clubs by THRESHOLD descending
        sorted_clubs = sorted(
            CLUBS.values(),
            key=lambda x: x.get("THRESHOLD", 0),
            reverse=True
        )
        new_clubs = {}
        for idx, cfg in enumerate(sorted_clubs, 1):
            cfg_copy = cfg.copy()
            cfg_copy["sdate"] = first_day_of_month
            new_clubs[str(idx)] = cfg_copy
        CLUBS.clear()
        CLUBS.update(new_clubs)
        print(f"Loaded {len(CLUBS)} clubs from config in quota-sorted order.", flush=True)
    else:
        print("Error: No clubs configured in database or globals.py. Exiting.", flush=True)
        sys.exit(1)
    
    # Rename sheets based on stored circle_id (CID) if name changed, then delete stale worksheets
    cid_to_active_cfg = {cfg['club_id']: cfg for cfg in CLUBS.values()}
    
    sheets_to_sync = [(TEMP_SHEET_ID, "Temp")] if is_temp_only else [(SHEET_ID, "Main"), (TEMP_SHEET_ID, "Temp")]
    for s_id, s_label in sheets_to_sync:
        if not s_id:
            continue
        try:
            ss = GC.open_by_key(s_id)
            all_worksheets = ss.worksheets()
            ws_by_id = {ws.id: ws for ws in all_worksheets}
            
            sheet_to_cid = {}
            print(f"[{s_label}] Scanning worksheet IDs to check for name changes...", flush=True)
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
                    print(f"Warning: [{s_label}] Failed to batch-read worksheet CIDs: {ex}", flush=True)
                
            existing_titles = {w.title for w in all_worksheets}
            for ws_id, cid in sheet_to_cid.items():
                ws = ws_by_id.get(ws_id)
                if ws is None or cid not in cid_to_active_cfg:
                    continue
                target_title = cid_to_active_cfg[cid]['title']
                if ws.title != target_title:
                    if target_title in existing_titles:
                        continue
                    print(f"[{s_label}] Renaming worksheet '{ws.title}' to '{target_title}' (Circle ID: {cid})...", flush=True)
                    try:
                        ws.update_title(target_title)
                        print(f"[{s_label}] Successfully renamed worksheet to '{target_title}'.", flush=True)
                        existing_titles.discard(ws.title)
                        existing_titles.add(target_title)
                    except Exception as ex:
                        print(f"Warning: [{s_label}] Failed to rename worksheet '{ws.title}' to '{target_title}': {ex}", flush=True)
            
            active_titles = {cfg['title'] for cfg in CLUBS.values()}
            for ws in ss.worksheets():
                title = ws.title
                if title == "All Club Data":
                    continue
                sheet_cid = sheet_to_cid.get(ws.id)
                is_unintended = (sheet_cid is not None and sheet_cid not in cid_to_active_cfg) or (title not in active_titles)
                if is_unintended:
                    reason = f"Circle ID: {sheet_cid}" if sheet_cid else "Unintended / stray worksheet"
                    print(f"[{s_label}] Detected unintended worksheet '{title}' ({reason}). Deleting...", flush=True)
                    try:
                        if len(ss.worksheets()) > 1:
                            ss.del_worksheet(ws)
                            print(f"[{s_label}] Deleted worksheet '{title}'.", flush=True)
                    except Exception as ex:
                        print(f"Warning: [{s_label}] Failed to delete worksheet '{title}': {ex}", flush=True)
        except Exception as e:
            print(f"Warning: [{s_label}] Failed to perform stale sheet cleanup & renames: {e}", flush=True)
    
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

    # Redundancy check: Skip if today's data is already updated (only for main sheet)
    if not is_temp_only and is_cron and choice == "ALL" and not force_run:
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
    temp_successful_results = []
    concurrency = max(1, _env_int("CLUB_CONCURRENCY", 4))
    print(f"\nProcessing {len(clubs_to_process)} clubs (Engine: {engine_choice}, concurrency: {concurrency})...\n", flush=True)

    items = list(clubs_to_process.items())
    if not items:
        print("No clubs to process.", flush=True)
    else:
        sem = asyncio.Semaphore(concurrency)

        async def _process_one(cfg):
            async with sem:
                return await process_club_workflow(cfg, GC, RETRY_DELAY, 5, 90, temp_only=is_temp_only)

        outcomes = await asyncio.gather(*(_process_one(cfg) for _, cfg in items))

        for outcome in outcomes:
            if outcome == NO_DATA:
                continue
            if outcome is not None and isinstance(outcome, tuple) and len(outcome) == 4:
                normal_outcome, resolved_sdate, temp_outcome, temp_resolved_sdate = outcome
                if normal_outcome:
                    successful_results.append((resolved_sdate, normal_outcome))
                if temp_outcome and temp_resolved_sdate:
                    temp_successful_results.append((temp_resolved_sdate, temp_outcome))
            elif outcome is not None and isinstance(outcome, tuple) and len(outcome) == 2:
                normal_outcome, resolved_sdate = outcome
                if normal_outcome:
                    successful_results.append((resolved_sdate, normal_outcome))
            else:
                total_failures += 1

    if not is_temp_only and choice == "ALL" and successful_results:
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

    if choice == "ALL" and temp_successful_results and TEMP_SHEET_ID:
        temp_sdate_counts = {}
        for sdate, _ in temp_successful_results:
            temp_sdate_counts[sdate] = temp_sdate_counts.get(sdate, 0) + 1
        temp_summary_sdate = max(temp_sdate_counts, key=lambda s: (temp_sdate_counts[s], s))
        temp_conforming = [r for r in temp_successful_results if r[0] == temp_summary_sdate]
        temp_excluded = len(temp_successful_results) - len(temp_conforming)
        if temp_excluded:
            print(f"  Excluded {temp_excluded} club(s) from temp summary (resolved to a different month than {temp_summary_sdate}).", flush=True)
        temp_successful_clubs = [r[1] for r in temp_conforming]

        if temp_successful_clubs:
            print("Exporting Temp All Club Data summary sheet...", flush=True)
            if await export_summary_with_retry(GC, TEMP_SHEET_ID, temp_successful_clubs, temp_summary_sdate, "Temp All Club Data"):
                print("Temp All Club Data summary sheet updated.", flush=True)

    # Reordering is now always the final step after the parallel gather
    print("Reordering sheets...", flush=True)
    club_titles = [CLUBS[k]['title'] for k in CLUBS]
    rank_by_name = {}
    for _, _club in successful_results:
        _r = str(_club.get("rank", "")).lstrip("#")
        if _r.isdigit():
            rank_by_name[_club["short_name"]] = int(_r)

    def _tab_rank(idx_title):
        idx, t = idx_title
        short = t.split("(")[0].strip() if "(" in t else t
        # Ranked clubs first (best rank first); unranked keep their existing order at the end.
        return (rank_by_name.get(short, float("inf")), idx)

    club_titles = [t for _, t in sorted(enumerate(club_titles), key=_tab_rank)]
    ordered_titles = ["All Club Data"] + club_titles
    if not is_temp_only:
        await reorder_sheets_with_retry(GC, SHEET_ID, ordered_titles, "")
    if TEMP_SHEET_ID:
        await reorder_sheets_with_retry(GC, TEMP_SHEET_ID, ordered_titles, "Temp")
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