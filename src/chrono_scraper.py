import calendar
import json
from datetime import datetime

from curl_cffi.requests import AsyncSession

from config.globals import CHRONO_API_KEY, UMAMOE_API_KEY, first_day_of_month
from src.utils import LogColor, colorize


def _umamoe_to_chrono(data: dict, year: int, month: int) -> dict | None:
    """Convert a uma.moe /api/v4/circles payload into the Chrono shape build_dataframe expects.

    uma.moe ``daily_fans`` is lifetime fans per slot (slot d == Chrono day d, slot 0 == baseline).
    Chrono rows carry the fans gained per day, so subtract the join baseline and diff the slots.
    Join day is inferred from the sign pattern (uma.moe has no join_time); pre-join days get no
    row, which the sheet renders as blank/gray. Same inference as UmaCore's ClubScraper.
    """
    members = data.get("members") or []
    days_in_month = calendar.monthrange(year, month)[1]
    latest = 0
    for m in members:
        fans = m.get("daily_fans") or []
        for i in range(min(len(fans), days_in_month + 1) - 1, latest, -1):
            if fans[i]:
                latest = i
                break
    if latest < 1:
        return None

    history = []
    for m in members:
        fans = m.get("daily_fans") or []
        vid, name = m.get("viewer_id"), m.get("trainer_name")
        if not vid or not name or len(fans) <= latest or not fans[latest] or fans[latest] <= 0:
            continue  # missing data, or not in the club on the latest day
        first_pos = next((i for i in range(latest + 1) if fans[i] and fans[i] > 0), None)
        if first_pos is None:
            continue
        if first_pos <= 1:
            join_day = 1
            baseline = fans[0] if first_pos == 0 else (abs(fans[0]) if fans[0] and fans[0] < 0 else fans[1])
        else:
            join_day = min(first_pos + 2, latest)
            baseline = fans[max(first_pos, join_day - 1)]
        prev = 0
        for d in range(join_day, latest + 1):
            v = fans[d]
            if not v or v <= 0:
                continue
            cum = max(v - baseline, 0)
            history.append({
                "friend_viewer_id": vid,
                "friend_name": name,
                "actual_date": d,
                "adjusted_interpolated_fan_gain": cum - prev,
                "adjusted_fan_gain_cumulative": cum,
            })
            prev = cum

    circle = data.get("circle") or {}
    rank = circle.get("monthly_rank") or circle.get("live_rank")
    # ponytail: uma.moe has no per-day rank history, only the latest day gets one.
    # Also: when slot 0 is empty the day-1 gain is absorbed into the baseline (shows 0), same as the bot's fallback.
    daily = [{"actual_date": latest, "rank": rank}] if rank else []
    return {"club_friend_history": history, "club_daily_history": daily, "source": "umamoe"}


async def _scrape_umamoe(cfg: dict):
    """uma.moe fallback for clubs Chrono returns 403 for. Returns (json_text, 200) or (None, 404)."""
    sdate = cfg.get("sdate") or first_day_of_month
    try:
        dt = datetime.strptime(sdate, "%Y-%m-%d")
        headers = {"accept": "application/json", "User-Agent": "Mozilla/5.0"}
        if UMAMOE_API_KEY:
            headers["X-API-Key"] = UMAMOE_API_KEY
        async with AsyncSession() as session:
            r = await session.get(
                "https://uma.moe/api/v4/circles",
                params={"circle_id": cfg.get("club_id"), "year": dt.year, "month": dt.month},
                headers=headers, impersonate="chrome", timeout=30,
            )
        if r.status_code != 200:
            return None, r.status_code
        converted = _umamoe_to_chrono(r.json(), dt.year, dt.month)
        if not converted or not converted["club_friend_history"]:
            return None, 404
        return json.dumps(converted), 200
    except Exception as e:
        print(f"  {colorize('[uma.moe]', LogColor.SCRAPER)} fallback error: {e}", flush=True)
        return None, 500


async def scrape_club_data(cfg: dict, zd=None):
    """
    Fetches club data from ChronoGenesis API directly using the Authorization key.
    This replaces the old zendriver/browser-based scraping logic.

    Uses curl_cffi with browser TLS impersonation: the API rejects the plain
    requests/curl TLS fingerprint with 403, but accepts browser fingerprints.
    """
    club_id = cfg.get('club_id')
    sdate = cfg.get('sdate')
    endpoint = "club_data_by_month" if sdate else "club_profile"
    url = f"https://api.chronogenesis.net/{endpoint}?circle_id={club_id}"
    if sdate:
        url += f"&sdate={sdate}"

    headers = {
        "Authorization": cfg.get('api_key') or CHRONO_API_KEY,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    prefix = colorize("[Chrono API]", LogColor.SCRAPER)

    try:
        async with AsyncSession() as session:
            response = await session.get(url, headers=headers, impersonate="chrome", timeout=15)
            if response.status_code != 403:
                return response.text, response.status_code

        # 403 = our key has no access to this club: use uma.moe data instead.
        text, status = await _scrape_umamoe(cfg)
        if status == 200:
            print(f"  {colorize('[uma.moe]', LogColor.SCRAPER)} Chrono 403 for {club_id}, using uma.moe data.", flush=True)
            return text, 200
        return response.text, 403

    except Exception as e:
        print(f"  {prefix} Connection error: {e}", flush=True)
        return None, 500


async def scrape_club_join_map(cfg: dict) -> dict:
    """Fetch each friend's join_time (JST) from the club profile.

    Returns a dict keyed by string viewer id -> join_time ISO string.
    An empty dict is returned on any failure so pre-join graying can be
    skipped without failing the main data flow.
    """
    club_id = cfg.get('club_id')
    url = f"https://api.chronogenesis.net/club_profile?circle_id={club_id}"

    headers = {
        "Authorization": cfg.get('api_key') or CHRONO_API_KEY,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        async with AsyncSession() as session:
            response = await session.get(url, headers=headers, impersonate="chrome", timeout=15)
            if response.status_code != 200:
                return {}
            data = response.json()
    except Exception:
        return {}

    join_map = {}
    for p in data.get("club_friend_profile") or []:
        vid = p.get("friend_viewer_id")
        if vid is not None and p.get("join_time"):
            join_map[str(vid)] = p["join_time"]
    return join_map
