from curl_cffi.requests import AsyncSession

from config.globals import CHRONO_API_KEY
from src.utils import LogColor, colorize


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
            return response.text, response.status_code

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
