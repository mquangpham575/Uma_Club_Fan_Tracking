import os
# OnlyRex/globals.py

VERSION = "1.1-REX"
GITHUB_REPO_URL = "https://github.com/mquangpham575/Uma_Club_Fan_Tracking"
GITHUB_API_URL = "https://api.github.com/repos/mquangpham575/Uma_Club_Fan_Tracking/releases/latest"

SHEET_ID = "1Bwana_HwxkjKWEAYuFvttpRivAf6rIOAAlZ9Zf2ke14"
CHRONO_API_KEY = os.getenv("CHRONO_API_KEY", "YOUR_LOCAL_KEY_HERE")

CLUBS = {
    "1": {
        "title": "April 26 (A+)",
        "club_id": "150259101",
        "THRESHOLD": 1300000,
        "sdate": "2026-04-01",
    },
}
