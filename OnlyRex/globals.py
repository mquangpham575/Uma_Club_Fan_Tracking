import os
from datetime import datetime, timezone, timedelta
# OnlyRex/globals.py

VERSION = "1.1-REX"
GITHUB_REPO_URL = "https://github.com/mquangpham575/Uma_Club_Fan_Tracking"
GITHUB_API_URL = "https://api.github.com/repos/mquangpham575/Uma_Club_Fan_Tracking/releases/latest"

SHEET_ID = "1Bwana_HwxkjKWEAYuFvttpRivAf6rIOAAlZ9Zf2ke14"
CHRONO_API_KEY = os.getenv("CHRONO_API_KEY", "YOUR_LOCAL_KEY_HERE")

# Calculate effective month (Chrono resets at 10:00 UTC)
now_utc = datetime.now(timezone.utc)
reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
effective_date = now_utc if now_utc >= reset_time else now_utc - timedelta(days=1)

month_name = effective_date.strftime("%B")
year_short = effective_date.strftime("%y") # "26" for 2026
first_day_of_month = effective_date.replace(day=1).strftime("%Y-%m-%d")

CLUBS = {
    "1": {
        "title": f"{month_name} {year_short} (A+)",
        "club_id": "150259101",
        "THRESHOLD": 1300000,
        "sdate": first_day_of_month,
    },
}
