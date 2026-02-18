import sys
import os
import asyncio
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath("."))

from src import main

async def test_menu_flow():
    print("Testing menu flow...")
    
    # Mock dependencies
    main.setup_windows_console = MagicMock()
    main.get_gspread_client = MagicMock()
    main.clear_screen = MagicMock()
    main.updater.check_for_update = MagicMock(return_value=None) # No update found
    
    # Mock pick_club to return UPDATE first, then ALL
    # We also need to mock input() for the "Press Enter to return to menu..."
    with patch('src.main.pick_club', side_effect=["UPDATE", "ALL"]), \
         patch('builtins.input', return_value=""), \
         patch('src.main.fetch_club_data_browser') as mock_fetch, \
         patch('src.main.process_club_workflow'), \
         patch('src.main.reorder_sheets'):
             
             # We want to break after the big loop to avoid running the whole processing
             # But main() runs the whole thing. 
             # Let's just mock the processing part to do nothing or raise an exception to stop
             
             mock_fetch.side_effect = Exception("Stop here") 
             
             try:
                await main.main()
             except Exception as e:
                 if str(e) == "Stop here":
                     print("Processing started (expected)")
                 else:
                     raise e
            
             # Check if updater was called
             assert main.updater.check_for_update.called
             print("Update check called: OK")
             
             # Check if pick_club was called twice
             assert main.pick_club.call_count == 2
             print("Menu looped: OK")

if __name__ == "__main__":
    asyncio.run(test_menu_flow())
