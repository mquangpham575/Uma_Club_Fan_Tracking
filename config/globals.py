import os
from datetime import datetime, timezone, timedelta
# globals.py

VERSION = "1.1"
GITHUB_REPO_URL = "https://github.com/mquangpham575/Uma_Club_Fan_Tracking"
GITHUB_API_URL = "https://api.github.com/repos/mquangpham575/Uma_Club_Fan_Tracking/releases/latest"

SHEET_ID = "1O09PM-hYo-H05kWWqMg71GelEpfaGrePQWzdDCKOqyU"
CHRONO_API_KEY = os.getenv("CHRONO_API_KEY", "YOUR_LOCAL_KEY_HERE")

# Calculate effective month (Chrono resets at 10:00 UTC)
now_utc = datetime.now(timezone.utc)
reset_time = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
effective_date = now_utc if now_utc >= reset_time else now_utc - timedelta(days=1)
first_day_of_month = effective_date.replace(day=1).strftime("%Y-%m-%d")

CLUBS = {
    "1": {
        "title": "ENDER (SS)",
        "club_id": "971029133",
        "THRESHOLD": 4500000,
    },
    "2": {
        "title": "ENDCORE (SS)",
        "club_id": "168472480",
        "THRESHOLD": 4500000,
    },
    "3": {
        "title": "ENDORSE (SS)",
        "club_id": "151723709",
        "THRESHOLD": 4000000,
    },
    "4": {
        "title": "ENDGOON (S)",
        "club_id": "125289696",
        "THRESHOLD": 2500000,
    },
    "5": {
        "title": "ENDGAME (S)",
        "club_id": "558806657",
        "THRESHOLD": 2500000,
    },
    "6": {
        "title": "ENDURANCE (S)",
        "club_id": "237354394",
        "THRESHOLD": 2500000,
    },
    "7": {
        "title": "ENDCRYPTED (A+)",
        "club_id": "313985233",
        "THRESHOLD": 1300000,
    },
    "8": {
        "title": "ENIGMA (A+)",
        "club_id": "166373193",
        "THRESHOLD": 1500000,
    },
    "9": {
        "title": "ENDYMION (A+)",
        "club_id": "636004975",
        "THRESHOLD": 1300000,
    },
    "10": {
        "title": "ENDDOOKIE (A+)",
        "club_id": "532271884",
        "THRESHOLD": 1300000,
    },
    "11": {
        "title": "ENDWAVES (A+)",
        "club_id": "815680661",
        "THRESHOLD": 700000,
    },
    "12": {
        "title": "ENDFIELD (A)",
        "club_id": "653291031",
        "THRESHOLD": 700000,
    },
    "13": {
        "title": "ENDMOON (A)",
        "club_id": "327708043",
        "THRESHOLD": 700000,
    },
    "14": {
        "title": "ENDGRAVE (Casual)",
        "club_id": "647067019",
        "THRESHOLD": 0,
    },
    "15": {
        "title": "ENDVILE (Casual)",
        "club_id": "114502546",
        "THRESHOLD": 0,
    },
    "16": {
        "title": "UMA Vault (A+)",
        "club_id": "150259101",
        "THRESHOLD": 1200000,
    },
    "17": {
        "title": "Dirt Idols (A+)",
        "club_id": "524890130",
        "THRESHOLD": 1000000,
    },
    "18": {
        "title": "Artisia (A+)",
        "club_id": "531543637",
        "THRESHOLD": 1000000,
    },
    "19": {
        "title": "MamboFanbo (A)",
        "club_id": "394737639",
        "THRESHOLD": 700000,
    },
    "20": {
        "title": "yurimusume (Casual)",
        "club_id": "415579817",
        "THRESHOLD": 0,
    },
}

# Inject dynamic sdate into all club configs
for club_cfg in CLUBS.values():
    club_cfg["sdate"] = first_day_of_month
