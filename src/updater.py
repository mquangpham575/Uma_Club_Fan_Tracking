import os
import subprocess
import sys
import time

import requests
from packaging import version

from config.globals import GITHUB_API_URL, VERSION


# Helper to determine if we are frozen (running as exe)
def is_frozen():
    return getattr(sys, 'frozen', False)

# Check for updates
def check_for_update():
    """
    Checks GitHub Releases for a newer version.
    Returns (version_string, download_url) if update available, otherwise None.
    """
    try:
        response = requests.get(GITHUB_API_URL, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        latest_tag = data.get("tag_name", "").strip()
        # Remove 'v' prefix if present
        latest_version_str = latest_tag.lstrip("v")
        
        current_ver = version.parse(VERSION)
        latest_ver = version.parse(latest_version_str)
        
        if latest_ver > current_ver:
            # Find the exe asset
            assets = data.get("assets", [])
            download_url = None
            for asset in assets:
                if asset["name"].endswith(".exe"):
                    download_url = asset["browser_download_url"]
                    break
            
            if download_url:
                return latest_tag, download_url
                
    except Exception as e:
        print(f"Update check failed: {e}")
        
    return None

def update_application(download_url):
    """
    Downloads the new executable and sets up a batch script to swap it.
    """
    if not is_frozen():
        print("Running from source, cannot self-update executable.")
        return

    current_exe = sys.executable
    current_dir = os.path.dirname(current_exe)
    new_exe_name = "new_update.exe.tmp"
    new_exe_path = os.path.join(current_dir, new_exe_name)
    
    print(f"Downloading update from {download_url}...")
    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        with open(new_exe_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        print(f"Failed to download update: {e}")
        if os.path.exists(new_exe_path):
            os.remove(new_exe_path)
        return

    print("Download complete. preparing to restart...")
    
    # Batch script to wait, move files, and restart
    # We rename current -> .old
    # Rename new -> current
    # Start current
    # Delete script
    
    bat_script = f"""
@echo off
timeout /t 2 /nobreak > NUL
:RETRY
del "{current_exe}.old" 2>NUL
move /y "{current_exe}" "{current_exe}.old"
if exist "{current_exe}" goto RETRY
move /y "{new_exe_path}" "{current_exe}"
start "" "{current_exe}"
del "%~f0"
    """
    
    bat_path = os.path.join(current_dir, "update_installer.bat")
    with open(bat_path, "w") as f:
        f.write(bat_script)
        
    # Launch batch file
    subprocess.Popen([bat_path], shell=True)
    
    # Exit current app
    sys.exit(0)



def cleanup_old_versions():
    """
    Removes any .old executable files left over from previous updates.
    Retries for a few seconds to allow file locks to release.
    """
    if not is_frozen():
        return
        
    current_exe = sys.executable
    old_exe = current_exe + ".old"
    
    # Try to delete for up to 3 seconds
    for _ in range(10): 
        if not os.path.exists(old_exe):
            return
            
        try:
            os.remove(old_exe)
            print(f"Removed old version: {old_exe}")
            return
        except Exception:
            time.sleep(0.3)
            
    # Final attempt or log failure
    if os.path.exists(old_exe):
        print(f"Could not remove old version: {old_exe} (File likely locked)")
