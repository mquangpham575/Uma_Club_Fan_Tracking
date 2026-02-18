import os
import sys
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath("."))

from src import updater


def test_check_update():
    print("Testing check_for_update...")
    
    # Mock requests.get
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "tag_name": "v99.0.0",
        "assets": [
            {
                "name": "app.exe",
                "browser_download_url": "http://example.com/app.exe"
            }
        ]
    }
    
    with patch('requests.get', return_value=mock_response):
        tag, url = updater.check_for_update()
        print(f"Result: Tag={tag}, URL={url}")
        assert tag == "v99.0.0"
        assert url == "http://example.com/app.exe"
        print("check_for_update PASSED")

def test_update_app():
    print("\nTesting update_application...")
    
    # Mock sys.frozen and sys.executable
    with patch('src.updater.is_frozen', return_value=True), \
         patch('sys.executable', new=os.path.abspath("dummy_app.exe")), \
         patch('requests.get') as mock_get, \
         patch('subprocess.Popen') as mock_popen, \
         patch('sys.exit') as mock_exit:
             
        # Mock download response
        mock_get.return_value.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_get.return_value.status_code = 200
        
        updater.update_application("http://example.com/app.exe")
        
        # Check if subprocess was called (batch file execution)
        assert mock_popen.called
        args = mock_popen.call_args[0][0]
        print(f"Batch file executed: {args[0]}")
        assert "update_installer.bat" in args[0]
        
        # Check if exit was called
        assert mock_exit.called
        print("update_application PASSED")

if __name__ == "__main__":
    test_check_update()
    test_update_app()
