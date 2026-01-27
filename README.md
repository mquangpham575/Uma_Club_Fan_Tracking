# Uma Club Tracking

**Disclaimer: This is a fan-made automatic program.**

This software automates the process of fetching club friend history data from [ChronoGenesis](https://chronogenesis.net/) and exporting it into a formatted Google Spreadsheet. The output includes automated styling, borders, totals, averages, and conditional formatting for enhanced readability.

![preview](assets/preview.png)

## Overview

The application streamlines data tracking for Uma Musume clubs by:
- Retrieving latest club data.
- Formatting data into a clear, professional Google Sheet.
- Handling distinct club profiles concurrently or individually.

For users preferring a pre-compiled solution, please visit the [Releases](https://github.com/mquangpham575/Uma_Club_Fan_Tracking/releases/tag/v1.0) page. The following setup instructions are intended for developers or users running the source code directly.

## Features

- **Automated Formatting:** Headers and totals are styled for high visibility.
- **Visual Aids:** Alternating row colors and automatic borders.
- **Conditional Formatting:**
  - Red highlights for values below defined thresholds.
  - Grey background for empty cells.
- **Auto-Summarization:** Automatic calculation of totals and averages.
- **Scalability:** Supports parallel exporting for multiple clubs.

## Setup

### Prerequisites
- Python 3.x installed.
- A Google Cloud Project with the Google Sheets API enabled.

### Installation

1. **Clone or Download:**
   - Click **Code** -> **Download ZIP** and extract, or clone the repository via Git.

2. **Google API Credentials:**
   - A `credentials.json` file is required to authenticate with Google Sheets.
   - Refer to this [video tutorial](https://youtu.be/zCEJurLGFRk) (01:59 - 06:50) for instructions on creating a Service Account Key.
   - Rename the downloaded key file to `credentials.json` and place it in the `config/` directory.
   - **Important:** Share the target Google Sheet with the Service Account's `client_email` (Editor access).

3. **Configuration:**
   - Open `config/globals.py` to configure the `SHEET_ID` and club details:
     ```python
     SHEET_ID = "YOUR_SPREADSHEET_ID"

     CLUBS = {
         "1": {"title": "EndGame", "URL": "...", "THRESHOLD": 1800000},
         # Add other clubs as needed
     }
     ```

## Usage

To execute the program, run the provided batch script:
`Script_run.bat`

Alternatively, execute via command line:
```bash
python main.py
```

### Operation
Upon running, select an operation mode:
```text
=== Choose a club to export ===

1. EndGame
...
0. Export ALL clubs (default)
```
- **Enter 0 or Press Enter:** Exports all configured clubs in parallel.
- **Enter Number:** Exports only the selected club.

Each club will be generated as a separate worksheet within the specified Google Spreadsheet. Note that existing sheets with the same name will be deleted and recreated.

## Build Instructions (Windows)

To bundle the application into a standalone executable:

1. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```

2. Build the executable:
   ```bash
   python -m PyInstaller --onefile --noconfirm --clean --icon=assets/app_icon.ico --name [name-file] --paths . src/main.py --add-data "config;config"
   ```

3. Locate the output:
   The compiled file will be available at `dist/main.exe`.

## Notes

- To change the destination Google Sheet, update the `SHEET_ID` variable in `globals.py`.
- The application automatically handles the deletion and recreation of sheets during export.
- For systems with limited resources, concurrency settings can be adjusted in the `main()` function in `main.py`.

![evernight](assets/evernight.gif)
