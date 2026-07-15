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

CLUBS = {
    "1": {
        "title": "ENDER (SS)",
        "club_id": "971029133",
    },
    "2": {
        "title": "ENDCORE (SS)",
        "club_id": "168472480",
    },
    "3": {
        "title": "ENDORSE (SS)",
        "club_id": "151723709",
    },
    "4": {
        "title": "ENDGOON (S)",
        "club_id": "125289696",
    },
    "5": {
        "title": "ENDGAME (S)",
        "club_id": "558806657",
    },
    "6": {
        "title": "ENDURANCE (S)",
        "club_id": "237354394",
    },
    "7": {
        "title": "ENDCRYPTED (A+)",
        "club_id": "313985233",
    },
    "8": {
        "title": "ENIGMA (A+)",
        "club_id": "166373193",
    },
    "9": {
        "title": "ENDYMION (A+)",
        "club_id": "636004975",
    },
    "10": {
        "title": "ENDDOOKIE (A+)",
        "club_id": "532271884",
    },
    "11": {
        "title": "ENDWAVES (A+)",
        "club_id": "815680661",
    },
    "12": {
        "title": "ENDFIELD (A)",
        "club_id": "653291031",
    },
    "13": {
        "title": "ENDMOON (A)",
        "club_id": "327708043",
    },
    "14": {
        "title": "ENDGRAVE (Casual)",
        "club_id": "647067019",
    },
    "15": {
        "title": "ENDVILE (Casual)",
        "club_id": "114502546",
    },
    "16": {
        "title": "UMA Vault (A+)",
        "club_id": "150259101",
    },
    "17": {
        "title": "Dirt Idols (A+)",
        "club_id": "524890130",
    },
    "18": {
        "title": "Artisia (A+)",
        "club_id": "531543637",
    },
    "19": {
        "title": "MamboFanbo (A)",
        "club_id": "394737639",
    },
    "20": {
        "title": "yurimusume (Casual)",
        "club_id": "415579817",
    },
    "21": {
        "title": "End Nuts (B)",
        "club_id": "567130959",
    },
    "22": {
        "title": "aMAIzxyng (A)",
        "club_id": "412167431",
    },
}
