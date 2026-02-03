import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

def get_gspread_client(base_path: str):
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        # Construct path to credentials.json in config folder
        creds_path = os.path.join(base_path, 'config', 'credentials.json')
        CREDS = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        GC = gspread.authorize(CREDS)
        return GC
    except Exception as e:
        print(f"Config Error: {e}")
        sys.exit(1)

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

def export_to_gsheets(gc_client, df: pd.DataFrame, spreadsheet_id: str, sheet_title: str, threshold: int):
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

    values = [header] + data_rows + [totals_row, day_avg_row]

    ss = gc_client.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(sheet_title)
        ss.del_worksheet(ws)
    except gspread.WorksheetNotFound:
        pass
        
    ws = ss.add_worksheet(title=sheet_title, rows=max(len(values) + 50, 120), cols=max(len(header) + 10, 26))

    end_row = len(values)
    end_col = len(header)
    end_a1 = rowcol_to_a1(end_row, end_col)
    ws.update(values, f"A1:{end_a1}")

    sheet_id = ws._properties["sheetId"]
    last_data_row_1based = 1 + len(data_rows) 

    header_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": end_col}
    totals_range = {"sheetId": sheet_id, "startRowIndex": end_row - 2, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}
    header_plus_data_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": end_col}
    band_left = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based, "startColumnIndex": 0, "endColumnIndex": (gidx if gidx is not None else end_col)}
    band_right = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": last_data_row_1based,
                  "startColumnIndex": (gidx + 1 if gidx is not None else end_col), "endColumnIndex": end_col}
    full_table_range = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": end_col}

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

    requests = [
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
        ] if gidx is None or gidx > 0 else []),
        *([
            {"addBanding": {"bandedRange": {"range": band_right, "rowProperties": {"firstBandColor": band_light, "secondBandColor": band_very}}}}
        ] if gidx is not None and gidx + 1 < end_col else []),
        *[
            {"repeatCell": {"range": r, "cell": {"userEnteredFormat": {"numberFormat": number_format}}, "fields": "userEnteredFormat.numberFormat"}}
            for r in numeric_ranges_all
        ],
        *([{
            "addConditionalFormatRule": {
                "rule": {"ranges": numeric_ranges_data,
                         "booleanRule": {"condition": {"type": "NUMBER_LESS",
                                                       "values": [{"userEnteredValue": str(threshold)}]},
                                     "format": {"backgroundColor": red_fill}}},
                "index": 0
            }
        }] if numeric_ranges_data else []),
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
        },
    ]

    if "Member_Name" in header:
        name_col_index = header.index("Member_Name")
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": name_col_index, "endIndex": name_col_index + 1},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }
        })

    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    ws.spreadsheet.batch_update({"requests": requests})
