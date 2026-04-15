import requests
import json
import base64


def fetch_via_github_api(owner, repo, path, token=None):
    """Method 2: GitHub Contents API (Handles metadata, private repos, and bypasses cache)"""
    print(f"\n--- Method 2: GitHub API ---")
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    print(f"API Endpoint: {api_url}")
    
    headers = {
        "Accept": "application/vnd.github.v3+json"
    }
    # If the repo is private, you MUST provide a token
    if token:
        headers["Authorization"] = f"token {token}"
    
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    
    result = response.json()
    
    # GitHub API returns content Base64 encoded inside a JSON object
    content_encoded = result.get("content", "")
    content_decoded = base64.b64decode(content_encoded).decode("utf-8")
    
    print(f"Metadata - SHA: {result.get('sha')}")
    print(f"Metadata - Size: {result.get('size')} bytes")
    return json.loads(content_decoded)

def run_demonstration():
    owner = "mquangpham575"
    repo = "Uma_Club_Fan_Tracking"
    file_path = "api_data/125289696.json"
    
    # Construct URLs
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/{file_path}"
    
    try:
        # 1. Fetch via GitHub API
        data = fetch_via_github_api(owner, repo, file_path)
        
        # Extract meaningful data
        club = data.get("club", [{}])[0]
        members = data.get("club_friend_profile", [])
        history = data.get("club_daily_history", [])

        print(f"\n==========================================")
        print(f" CLUB: {club.get('name')} (ID: {club.get('circle_id')})")
        print(f"==========================================")
        print(f" Current Rank : #{club.get('rank')}")
        print(f" Total Fans   : {club.get('fan_count', 0):,}")
        print(f" Members      : {len(members)}/30")
        print(f" Updated At   : {club.get('updated_at')}")
        
        print("\n--- TOP MEMBERS (Sample) ---")
        # Print top 5 members by fan count
        sorted_members = sorted(members, key=lambda x: x.get('fan_count', 0), reverse=True)
        for i, m in enumerate(sorted_members[:5]):
            print(f" {i+1}. {m.get('name'):<15} | Fans: {m.get('fan_count', 0):,}")

        print("\n--- RECENT HISTORY ---")
        for h in history[:3]:
            print(f"  Day {h.get('actual_date'):<2} | Rank: #{h.get('rank'):<3} | Gain: +{h.get('interpolated_fan_gain', 0):,}")
        print(f"==========================================\n")

    except Exception as e:
        print(f"Error during fetch: {e}")

if __name__ == "__main__":
    run_demonstration()
