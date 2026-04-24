import asyncio
import requests
import json
from src.utils import LogColor, colorize
from config.globals import CHRONO_API_KEY

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
        
        if response.status_code == 200:
            # Check if the response actually contains club data or an error message
            res_json = response.json()
            if isinstance(res_json, dict) and res_json.get("detail") == "Error":
                 print(f"  {prefix} API returned error for ID {club_id}", flush=True)
                 return None
            
            return response.text
        
        elif response.status_code == 403:
            print(f"  {prefix} Forbidden (403). Your API key might be invalid or restricted.", flush=True)
        elif response.status_code == 429:
            print(f"  {prefix} Rate limited (429).", flush=True)
        else:
            print(f"  {prefix} Failed with status {response.status_code}", flush=True)
            
        return None

    except Exception as e:
        print(f"  {prefix} Connection error: {e}", flush=True)
        return None
