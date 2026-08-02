import os
from datetime import datetime, timedelta, timezone

# globals.py

VERSION = "1.1"
GITHUB_REPO_URL = "https://github.com/mquangpham575/Uma_Club_Fan_Tracking"
GITHUB_API_URL = "https://api.github.com/repos/mquangpham575/Uma_Club_Fan_Tracking/releases/latest"

SHEET_ID = os.getenv("SHEET_ID", "1O09PM-hYo-H05kWWqMg71GelEpfaGrePQWzdDCKOqyU")
CHRONO_API_KEY = os.getenv("CHRONO_API_KEY", "YOUR_LOCAL_KEY_HERE")
SERVER_ID = os.getenv("SERVER_ID") or os.getenv("GUILD_ID", "1108441000873033869")

# Calculate effective month (Chrono resets at 10:00 UTC)
now_utc = datetime.now(timezone.utc)
reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
effective_date = now_utc if now_utc >= reset_time else now_utc - timedelta(days=1)
first_day_of_month = effective_date.replace(day=1).strftime("%Y-%m-%d")
TEMP_SHEET_ID = os.getenv("TEMP_SHEET_ID", "19BIFTfXUckFhWsQO9e6TiFPFp_p46fN3BVefvdZZi0I")

CLUBS = {}
