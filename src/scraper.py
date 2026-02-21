import asyncio
from datetime import datetime

import requests


async def fetch_club_data(club_cfg: dict):
    # Fetch club member data directly from the uma.moe API dynamically using the current month and year.
    circle_id = club_cfg.get("club_id", "")
    
    now = datetime.now()
    url = f"https://uma.moe/api/v4/circles?circle_id={circle_id}&year={now.year}&month={now.month}"
    
    loop = asyncio.get_event_loop()
    try:
        # Adjustment: timeout=20 prevents infinite hangs on bad requests
        response = await loop.run_in_executor(None, lambda: requests.get(url, timeout=20))
        if response.status_code != 200:
            raise Exception(f"API Error {response.status_code}")
            
        data = response.json()
    except Exception as e:
        raise Exception(f"Failed to fetch {url}: {e}")
        
    club_friend_history = []
    
    members = data.get("members", [])
    for member in members:
        friend_viewer_id = str(member.get("viewer_id", ""))
        friend_name = member.get("trainer_name", "")
        daily_fans = member.get("daily_fans", [])
        
        # We start from Day 2 (index 1) to calculate difference from Day 1 (index 0)
        for i in range(1, len(daily_fans)):
            prev = daily_fans[i-1]
            curr = daily_fans[i]
            
            if curr > 0 and prev > 0 and curr >= prev:
                gain = curr - prev
                day_num = i + 1
                
                club_friend_history.append({
                    "friend_viewer_id": friend_viewer_id,
                    "friend_name": friend_name,
                    "actual_date": str(day_num),
                    "adjusted_interpolated_fan_gain": gain
                })
                
    return {
        "club_friend_history": club_friend_history,
        "club_daily_history": []
    }
