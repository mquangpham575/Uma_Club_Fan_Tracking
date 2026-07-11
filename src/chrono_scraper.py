import asyncio

import requests

from config.globals import CHRONO_API_KEY
from src.utils import LogColor, colorize


async def scrape_club_data(cfg: dict, zd=None):
    """
    Fetches club data from ChronoGenesis API directly using the Authorization key.
    This replaces the old zendriver/browser-based scraping logic.
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
        # Using asyncio.to_thread to run the blocking requests.get in a separate thread
        response = await asyncio.to_thread(
            requests.get, 
            url, 
            headers=headers, 
            timeout=15
        )
        
        return response.text, response.status_code

    except Exception as e:
        print(f"  {prefix} Connection error: {e}", flush=True)
        return None, 500
