import asyncio
from datetime import datetime

import requests


async def fetch_club_data(club_cfg: dict):
    # Fetch club member data directly from the uma.moe API dynamically using the current month and year.
    circle_id = club_cfg.get("club_id", "")
    
    now = datetime.now()
    # If today is the 1st or 2nd day of the month, query the previous month to avoid incomplete data.
    if now.day <= 2:
        if now.month == 1:
            target_year = now.year - 1
            target_month = 12
        else:
            target_year = now.year
            target_month = now.month - 1
    else:
        target_year = now.year
        target_month = now.month

    url = f"https://uma.moe/api/v4/circles?circle_id={circle_id}&year={target_year}&month={target_month}"
    
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
        
        # The API always returns an array of length 31 (for March), filled with 0s for future days.
        # To ignore the actively accumulating day (the latest day with data), we must find its index first.
        last_active_idx = -1
        for i in range(len(daily_fans) - 1, -1, -1):
            if daily_fans[i] > 0:
                last_active_idx = i
                break

        # Edge case: only Day 1 exists (month just started), use raw value as gain since there's no prior day.
        if last_active_idx == 0 and daily_fans[0] > 0:
            club_friend_history.append({
                "friend_viewer_id": friend_viewer_id,
                "friend_name": friend_name,
                "actual_date": "1",
                "adjusted_interpolated_fan_gain": daily_fans[0]
            })
            continue

        # Calculate daily gain by comparing current and previous day values
        # We stop processing strictly BEFORE the last_active_idx (still accumulating)
        for i in range(1, last_active_idx):
            prev = daily_fans[i-1]
            curr = daily_fans[i]
            
            if curr > 0 and prev > 0 and curr >= prev:
                gain = curr - prev
                day_num = i
                
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
