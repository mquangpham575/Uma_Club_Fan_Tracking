import os
import sys

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


def get_gspread_client(base_path: str, creds_folder: str = 'config'):
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        # Construct path to credentials.json in specified folder
        creds_path = os.path.join(base_path, creds_folder, 'credentials.json')
        CREDS = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        GC = gspread.authorize(CREDS)
        return GC
    except Exception as e:
        print(f"Config Error: {e}")
        sys.exit(1)


def get_conditional_format_rules_count(gc_client, spreadsheet_id: str, sheet_title: str) -> int:
    # Retrieves the count of conditional formatting rules active on the specified sheet.
    try:
        import urllib.parse
        q_range = urllib.parse.quote(f"'{sheet_title}'")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}?ranges={q_range}&includeGridData=false"
        res_obj = gc_client.http_client.session.get(url)
        if res_obj.status_code == 200:
            res = res_obj.json()
            sheets = res.get('sheets', [])
            if sheets:
                return len(sheets[0].get('conditionalFormats', []))
    except Exception as e:
        print(f"Warning: Failed to get conditional formatting rules count: {e}")
    return 0


def get_banded_range_ids(gc_client, spreadsheet_id: str, sheet_title: str) -> list[str]:
    # Retrieves all bandedRangeIds active on the specified sheet.
    try:
        import urllib.parse
        q_range = urllib.parse.quote(f"'{sheet_title}'")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}?ranges={q_range}&includeGridData=false"
        res_obj = gc_client.http_client.session.get(url)
        if res_obj.status_code == 200:
            res = res_obj.json()
            sheets = res.get('sheets', [])
            if sheets:
                banded_ranges = sheets[0].get('bandedRanges', [])
                return [b.get('bandedRangeId') for b in banded_ranges if b.get('bandedRangeId')]
    except Exception as e:
        print(f"Warning: Failed to get banded range IDs: {e}")
    return []


def reorder_sheets(gc_client, spreadsheet_id: str, ordered_titles: list[str]):
    # Reorders the worksheets in the spreadsheet to match the order of ordered_titles
    try:
        ss = gc_client.open_by_key(spreadsheet_id)
        worksheets = ss.worksheets()
        ws_map = {ws.title: ws for ws in worksheets}
        
        ordered_ws = []
        # Add sheets in the order specified
        for title in ordered_titles:
            if title in ws_map:
                ordered_ws.append(ws_map[title])
        
        # Add any remaining sheets that were not in the ordered list
        for ws in worksheets:
             if ws.title not in ordered_titles:
                ordered_ws.append(ws)

        if ordered_ws:
            ss.reorder_worksheets(ordered_ws)
    except Exception as e:
        print(f"Warning: Failed to reorder sheets: {e}")

def export_to_gsheets(gc_client, df: pd.DataFrame, spreadsheet_id: str, sheet_title: str, threshold: int, club_daily_history: list = None, circle_id: str = None):
    # Exports individual club data and daily history to Google Sheets.
    GAP_COL = " "
    dcols = [c for c in df.columns if isinstance(c, str) and c.startswith("Day ")]
    df_to_write = df.copy()

    if dcols:
        df_to_write["Total"] = df_to_write[dcols].sum(axis=1, min_count=1)
        gidx = df_to_write.columns.get_loc("Total")
        df_to_write.insert(gidx, GAP_COL, "")
    else:
        gidx = None

    bottom_totals = {}
    for c in df_to_write.columns:
        if c == "Member_Name":
            bottom_totals[c] = "Total"
        elif c in ("Member_ID", GAP_COL):
            bottom_totals[c] = ""
        else:
            total = pd.to_numeric(df_to_write[c], errors="coerce").sum(min_count=1)
            if pd.isna(total):
                bottom_totals[c] = ""
            elif isinstance(total, float):
                bottom_totals[c] = total  
            elif hasattr(total, 'item'):
                bottom_totals[c] = total.item()
            else:
                bottom_totals[c] = total 

    if circle_id:
        bottom_totals["Member_ID"] = f"CID:{circle_id}"

    day_avgs = pd.Series("", index=df_to_write.columns, dtype=object)
    if dcols:
        means = df_to_write[dcols].mean(axis=0, skipna=True).round(0)
        for c in dcols:
            day_avgs[c] = means.get(c, "")
    day_avgs["Member_Name"] = "Day AVG"

    header = list(map(str, df_to_write.columns))
    data_rows = df_to_write.where(pd.notna(df_to_write), "").values.tolist()
    totals_row = [("" if pd.isna(v) else v) for v in (bottom_totals.get(c, "") for c in df_to_write.columns)]
    day_avg_row = [day_avgs.get(c, "") for c in df_to_write.columns]

    club_row = []
    if club_daily_history:
        # map actual_date string -> rank
        history_map = {}
        for entry in club_daily_history:
            d_str = str(entry.get("actual_date", ""))
            if d_str:
                history_map[d_str] = entry.get("rank", "")
        
        club_row_vals = pd.Series("", index=df_to_write.columns, dtype=object)
        
        for col in dcols:
            # col is like "Day <date>"
            # extraction logic matching processing.py: "Day " + actual_date
            if col.startswith("Day "):
                date_part = col[4:] # strip "Day "
                if date_part in history_map:
                     club_row_vals[col] = history_map[date_part]
        
        club_row_vals["Member_Name"] = "Club Daily Rank"
        club_row = [club_row_vals.get(c, "") for c in df_to_write.columns]

    values = [header] + data_rows + [totals_row, day_avg_row]
    if club_row:
        values.append(club_row)

    ss = gc_client.open_by_key(spreadsheet_id)
    is_new_sheet = False
    try:
        ws = ss.worksheet(sheet_title)
        ws.clear()
        ws.resize(rows=max(len(values) + 50, 120), cols=max(len(header) + 10, 26))
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_title, rows=max(len(values) + 50, 120), cols=max(len(header) + 10, 26))
        is_new_sheet = True

    num_cf_rules = 0 if is_new_sheet else get_conditional_format_rules_count(gc_client, spreadsheet_id, sheet_title)
    existing_banded_ids = [] if is_new_sheet else get_banded_range_ids(gc_client, spreadsheet_id, sheet_title)

    end_row = len(values)
    end_col = len(header)
    end_a1 = rowcol_to_a1(end_row, end_col)
    values = [[("" if pd.isna(cell) else cell) for cell in row] for row in values]
    ws.update(values, f"A1:{end_a1}")

    sheet_id = int(ws.id)  # Adjustment: cast to int for API
    last_data_row_1based = 1 + len(data_rows) 

    header_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": end_col}
    totals_range = {"sheetId": sheet_id, "startRowIndex": last_data_row_1based, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}
    header_plus_data_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": end_col}

    full_table_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}

    band_left = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": (gidx if gidx is not None else end_col)}
    band_right = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based, "startColumnIndex": (gidx + 1 if gidx is not None else end_col), "endColumnIndex": end_col}

    def col_1_based(col_name: str) -> int | None:
        try:
            return header.index(col_name) + 1
        except ValueError:
            return None

    skip_for_number = {"Member_ID", "Member_Name", GAP_COL}
    numeric_cols_1 = [i + 1 for i, c in enumerate(header) if c not in skip_for_number]

    day_cols_1 = [col_1_based(c) for c in dcols]
    day_cols_1 = [c1 for c1 in day_cols_1 if c1 is not None]

    def col_range_rows(start_row_1, end_row_1, col_1):
        return {"sheetId": sheet_id, "startRowIndex": start_row_1 - 1, "endRowIndex": end_row_1,
                "startColumnIndex": col_1 - 1, "endColumnIndex": col_1}

    numeric_ranges_all = [col_range_rows(2, end_row, c1) for c1 in numeric_cols_1]
    numeric_ranges_data_days = [col_range_rows(2, last_data_row_1based, c1) for c1 in day_cols_1]

    avgd_col_1 = col_1_based("AVG/d")
    numeric_ranges_data = list(numeric_ranges_data_days)
    if avgd_col_1 is not None:
        numeric_ranges_data.append(col_range_rows(2, last_data_row_1based, avgd_col_1))

    blue_fill  = {"red": 0.31, "green": 0.51, "blue": 0.74}
    white_font = {"red": 1, "green": 1, "blue": 1}
    red_fill   = {"red": 1.00, "green": 0.78, "blue": 0.81}
    grey_fill  = {"red": 0.75, "green": 0.75, "blue": 0.75}
    band_light = {"red": 0.86, "green": 0.92, "blue": 0.97}
    band_very  = {"red": 0.95, "green": 0.97, "blue": 0.98}
    number_format = {"type": "NUMBER", "pattern": "#,##0"}

    cf_ranges = []
    if last_data_row_1based > 1:
        if gidx is not None:
            cf_ranges.append({
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": last_data_row_1based,
                "startColumnIndex": 2,
                "endColumnIndex": gidx
            })
            cf_ranges.append({
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": last_data_row_1based,
                "startColumnIndex": gidx + 1,
                "endColumnIndex": end_col
            })
        else:
            cf_ranges.append({
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": last_data_row_1based,
                "startColumnIndex": 2,
                "endColumnIndex": end_col
            })

    requests = []
    if not is_new_sheet:
        requests.extend([
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(end_row + 100, 200),
                        "startColumnIndex": 0,
                        "endColumnIndex": max(end_col + 20, 50)
                    }
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(end_row + 100, 200),
                        "startColumnIndex": 0,
                        "endColumnIndex": max(end_col + 20, 50)
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat"
                }
            }
        ])
        for _ in range(num_cf_rules):
            requests.append({
                "deleteConditionalFormatRule": {
                    "index": 0,
                    "sheetId": sheet_id
                }
            })
        for banded_id in existing_banded_ids:
            requests.append({
                "deleteBanding": {
                    "bandedRangeId": banded_id
                }
            })

    requests.extend([
        {"setBasicFilter": {"filter": {"range": header_plus_data_range}}},
        {
            "repeatCell": {
                "range": header_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": blue_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        {
            "repeatCell": {
                "range": totals_range,
                "cell": {"userEnteredFormat": {"backgroundColor": blue_fill, "textFormat": {"bold": True, "foregroundColor": white_font}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        },
        *([
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": gidx, "endColumnIndex": gidx + 1},
                    "cell": {"userEnteredFormat": {"backgroundColor": blue_fill}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": gidx, "endIndex": gidx + 1},
                    "properties": {"pixelSize": 40},
                    "fields": "pixelSize"
                }
            }
        ] if gidx is not None else []),

        *([
            {"addBanding": {"bandedRange": {"range": band_left,  "rowProperties": {"firstBandColor": band_light, "secondBandColor": band_very}}}}
        ] if (gidx is None or gidx > 0) and last_data_row_1based > 1 else []),
        *([
            {"addBanding": {"bandedRange": {"range": band_right, "rowProperties": {"firstBandColor": band_light, "secondBandColor": band_very}}}}
        ] if gidx is not None and gidx + 1 < end_col and last_data_row_1based > 1 else []),

        *[
            {"repeatCell": {"range": r, "cell": {"userEnteredFormat": {"numberFormat": number_format}}, "fields": "userEnteredFormat.numberFormat"}}
            for r in numeric_ranges_all
        ],
        *([{
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": cf_ranges,
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": f"=AND(ISNUMBER($C2), $C2 < {threshold})"}],
                        },
                        "format": {"backgroundColor": red_fill}
                    }
                },
                "index": 0
            }
        }] if last_data_row_1based > 1 else []),
        *([{
            "addConditionalFormatRule": {
                "rule": {"ranges": numeric_ranges_data_days,
                         "booleanRule": {"condition": {"type": "BLANK"},
                                     "format": {"backgroundColor": grey_fill}}},
                "index": 0
            }
        }] if numeric_ranges_data_days else []),
        {
            "updateBorders": {
                "range": full_table_range,
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        }
    ])

    if "Member_Name" in header:
        name_col_index = header.index("Member_Name")
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": name_col_index, "endIndex": name_col_index + 1},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }
        })

    if dcols:
        first_day_idx = header.index(dcols[0])
        last_day_idx = header.index(dcols[-1])
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": first_day_idx, "endIndex": last_day_idx + 1},
                "properties": {"pixelSize": 90},
                "fields": "pixelSize"
            }
        })

    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    green_members = set()
    if "Member_Name" in df_to_write.columns:
        green_members = set(df_to_write["Member_Name"].head(3).tolist())

    if green_members and "Member_Name" in header:
        name_col_index = header.index("Member_Name")
        green_fill = {"red": 0.576, "green": 0.769, "blue": 0.490}
        for i, row_data_vals in enumerate(data_rows):
            member_name = row_data_vals[name_col_index]
            if member_name in green_members:
                row_idx = 1 + i
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": name_col_index,
                            "endColumnIndex": name_col_index + 1
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": green_fill}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

    ws.spreadsheet.batch_update({"requests": requests})


def export_all_club_data_to_gsheets(gc_client, spreadsheet_id: str, all_clubs_data: list, sdate: str = None):
    """Exports combined member and club statistics across all tracked clubs to a formatted side-by-side dashboard in Google Sheets."""
    # Define green members dynamically as the top 3 members of each club
    green_members = set()
    for club in all_clubs_data:
        # Get first 3 members of this club (they are sorted by AVG/d descending)
        for m in club["members"][:3]:
            green_members.add(m["member_name"])

    # 1. Compile Member Rows
    left_rows = []
    for club in all_clubs_data:
        short_name = club["short_name"]
        for member in club["members"]:
            left_rows.append({
                "Club Spec": short_name,
                "Members": member["member_name"],
                "Average Day": member["avg_day"],
                "Perfomance": member["performance"]
            })
    # Sort members by Average Day descending
    left_rows.sort(key=lambda x: x["Average Day"] if x["Average Day"] is not None else 0, reverse=True)

    # 2. Compile Club Rows
    right_rows = []
    for club in all_clubs_data:
        short_name = club["short_name"]
        grade = club["grade"]
        rank = club["rank"]
        members = club["members"]
        
        total_avg_day = sum(m["avg_day"] for m in members if m["avg_day"] is not None)
        total_perf = sum(m["performance"] for m in members if m["performance"] is not None)
        avg_per_player = round(total_avg_day / len(members)) if members else 0
        
        right_rows.append({
            "GRADE": grade,
            "RANK": rank,
            "CLUB NAME": short_name,
            "Average Day": total_avg_day,
            "Average/player": avg_per_player,
            "Perfomance": total_perf
        })
    # Sort clubs by Average Day descending
    right_rows.sort(key=lambda x: x["Average Day"] if x["Average Day"] is not None else 0, reverse=True)

    # 3. Build side-by-side grid
    from datetime import datetime
    try:
        dt = datetime.strptime(sdate, "%Y-%m-%d")
        month_year_str = dt.strftime("%B %Y").upper()
    except Exception:
        month_year_str = datetime.now().strftime("%B %Y").upper()

    row1 = [f"{month_year_str} PLAYER", "", "", "", "", "", f"{month_year_str} CLUB", "", "", "", "", ""]
    row2 = [
        "NO", "Club Spec", "Members", "Average Day", "Perfomance",
        " ",
        "GRADE", "RANK", "CLUB NAME", "Average Day", "Average/player", "Perfomance"
    ]
    
    values = [row1, row2]
    max_rows = max(len(left_rows), len(right_rows))
    
    for i in range(max_rows):
        row = []
        # Left Table
        if i < len(left_rows):
            m = left_rows[i]
            row.extend([i + 1, m["Club Spec"], m["Members"], m["Average Day"], m["Perfomance"]])
        else:
            row.extend(["", "", "", "", ""])
            
        # Gap Column
        row.append("")
        
        # Right Table
        if i < len(right_rows):
            c = right_rows[i]
            row.extend([c["GRADE"], c["RANK"], c["CLUB NAME"], c["Average Day"], c["Average/player"], c["Perfomance"]])
        else:
            row.extend(["", "", "", "", "", ""])
            
        values.append(row)

    # Pad with empty rows to at least 32 rows to allow writing the legend at row 24-31
    while len(values) < 32:
        values.append([""] * 12)

    # Populate Legend into values array
    legend_data = [
        ("SS", "SS"),
        ("S+", "S+"),
        ("S", "S"),
        ("A+", "A+"),
        ("A", "A"),
        ("Casual", "Casual"),
        (None, None),
        ("Carry Club", "Carry Club")
    ]
    for idx, (lbl, val) in enumerate(legend_data):
        row_idx = 23 + idx
        values[row_idx][6] = ""
        values[row_idx][7] = val if val is not None else ""

    # 4. Write to Google Sheets
    ss = gc_client.open_by_key(spreadsheet_id)
    sheet_title = "All Club Data"
    is_new_sheet = False
    try:
        ws = ss.worksheet(sheet_title)
        ws.clear()
        ws.resize(rows=max(len(values) + 50, 100), cols=15)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_title, rows=max(len(values) + 50, 100), cols=15)
        is_new_sheet = True
    
    end_row = len(values)
    end_a1 = rowcol_to_a1(end_row, 12)
    values = [[("" if pd.isna(cell) else cell) for cell in row] for row in values]
    ws.update(values, f"A1:{end_a1}")
    
    # 5. Format the sheet
    sheet_id = int(ws.id)
    
    dark_green_fill = {"red": 0.118, "green": 0.271, "blue": 0.129}
    white_font      = {"red": 1, "green": 1, "blue": 1}
    number_format   = {"type": "NUMBER", "pattern": "#,##0"}

    # Base ranges
    row1_left_range  = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 5}
    row1_right_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 12}
    row2_left_range  = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 5}
    row2_right_range = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 6, "endColumnIndex": 12}
    
    requests = []
    if not is_new_sheet:
        requests.extend([
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(len(values) + 100, 200),
                        "startColumnIndex": 0,
                        "endColumnIndex": 20
                    }
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(len(values) + 100, 200),
                        "startColumnIndex": 0,
                        "endColumnIndex": 20
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat"
                }
            },
            {
                "clearBasicFilter": {
                    "sheetId": sheet_id
                }
            }
        ])

    requests.extend([
        # Frozen Rows (2 rows frozen to keep table titles and headers visible)
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                "fields": "gridProperties.frozenRowCount"
            }
        },
        # Merge Table Titles
        {
            "mergeCells": {
                "range": row1_left_range,
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": row1_right_range,
                "mergeType": "MERGE_ALL"
            }
        },
        # Row 1 header styling
        {
            "repeatCell": {
                "range": row1_left_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_green_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font, "fontSize": 11, "fontFamily": "Merriweather"},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        {
            "repeatCell": {
                "range": row1_right_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_green_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font, "fontSize": 11, "fontFamily": "Merriweather"},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        # Row 2 headers styling
        {
            "repeatCell": {
                "range": row2_left_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_green_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font, "fontSize": 10, "fontFamily": "Merriweather"},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        {
            "repeatCell": {
                "range": row2_right_range,
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_green_fill,
                    "textFormat": {"bold": True, "foregroundColor": white_font, "fontSize": 10, "fontFamily": "Merriweather"},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE"
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        },
        # Gap Column width (80 pixels)
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 5, "endIndex": 6},
                "properties": {"pixelSize": 80},
                "fields": "pixelSize"
            }
        },
        # Numeric Formats for Left Table: Average Day (Col 3), Performance (Col 4)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 3, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"numberFormat": number_format}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        # Numeric Formats for Right Table: Average Day (Col 9), Average/player (Col 10), Performance (Col 11)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 9, "endColumnIndex": 12},
                "cell": {"userEnteredFormat": {"numberFormat": number_format}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        # Alignments
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 6, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        },
        # Column widths for names
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 8, "endIndex": 9},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }
        },
        # Left Table Columns B to E (index 1 to 5)
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": 1, "endColumnIndex": 5},
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        },
        # Column A Header rows (Row 1 and 2, indices 0 to 2)
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 1},
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        },
        # Column A Data rows (Row 3 to end, indices 2 to end_row) - No inner horizontal borders
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": 1},
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
                "innerHorizontal": {"style": "NONE"},
            }
        },
        # Right Table (Columns G-L, index 6 to 12) - Only down to right_table_end
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": len(right_rows) + 2, "startColumnIndex": 6, "endColumnIndex": 12},
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
                "innerHorizontal": {"style": "SOLID"},
                "innerVertical": {"style": "SOLID"},
            }
        },
        # Remove inner horizontal borders for GRADE and RANK data rows (indices 6 and 7)
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": len(right_rows) + 2, "startColumnIndex": 6, "endColumnIndex": 8},
                "innerHorizontal": {"style": "NONE"}
            }
        },
        # Align legend labels left (rows 24 to 31, Col H index 7)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 23, "endRowIndex": 31, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }
        },
        # Font families for number data (Col A, D, E, H, J, K, L)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Roboto"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 3, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Roboto"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Roboto"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 9, "endColumnIndex": 12},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Roboto"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        # Font families for club name / spec / Members (Col B, C, I)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 1, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        },
        # Legend labels (rows 24 to 31, Col H index 7) font Arial
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 23, "endRowIndex": 31, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial"}}},
                "fields": "userEnteredFormat.textFormat.fontFamily"
            }
        }
    ])

    # Color Left Table B (Club Spec) based on grade
    for i, row in enumerate(left_rows):
        grade = ""
        for c in all_clubs_data:
            if c["short_name"] == row["Club Spec"]:
                grade = c["grade"]
                break
        color = GRADE_COLORS.get(grade)
        if color:
            row_idx = 2 + i
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 1, "endColumnIndex": 2},
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    # Color Left Table C (Members) green if carry member
    for i, row in enumerate(left_rows):
        member_name = row["Members"]
        if green_members and member_name in green_members:
            row_idx = 2 + i
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 2, "endColumnIndex": 3},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.576, "green": 0.769, "blue": 0.490}}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    # Color and bold top 10 performance cells yellow in left table
    yellow_color = {"red": 1.0, "green": 0.851, "blue": 0.400}
    top_n = min(10, len(left_rows))
    if top_n > 0:
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 2 + top_n, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": yellow_color,
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"
            }
        })

    # Bold columns: A (no), B (club spec), G (grade), H (rank), I (club name)
    requests.extend([
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": end_row, "startColumnIndex": 1, "endColumnIndex": 2},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": len(right_rows) + 2, "startColumnIndex": 6, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        }
    ])

    # Color Right Table I (CLUB NAME) based on grade
    for i, row in enumerate(right_rows):
        grade = row["GRADE"]
        color = GRADE_COLORS.get(grade)
        if color:
            row_idx = 2 + i
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": 8, "endColumnIndex": 9},
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    # Format Legend color blocks
    legend_colors = {
        23: GRADE_COLORS["SS"],
        24: GRADE_COLORS["S+"],
        25: GRADE_COLORS["S"],
        26: GRADE_COLORS["A+"],
        27: GRADE_COLORS["A"],
        28: GRADE_COLORS["Casual"],
        30: GRADE_COLORS["A"] # Carry Club green
    }
    for r_idx, color in legend_colors.items():
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": r_idx, "endRowIndex": r_idx + 1, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
        requests.append({
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": r_idx, "endRowIndex": r_idx + 1, "startColumnIndex": 6, "endColumnIndex": 7},
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            }
        })

    ws.spreadsheet.batch_update({"requests": requests})


GRADE_COLORS = {
    "SS": {"red": 0.557, "green": 0.486, "blue": 0.765},
    "S+": {"red": 0.965, "green": 0.698, "blue": 0.420},
    "S": {"red": 0.878, "green": 0.400, "blue": 0.400},
    "A+": {"red": 1.000, "green": 0.851, "blue": 0.400},
    "A": {"red": 0.576, "green": 0.769, "blue": 0.490},
    "B+": {"red": 0.310, "green": 0.510, "blue": 0.737},  # same as steel blue
    "C": {"red": 0.600, "green": 0.600, "blue": 0.600},
    "Casual": {"red": 0.600, "green": 0.600, "blue": 0.600},
}



