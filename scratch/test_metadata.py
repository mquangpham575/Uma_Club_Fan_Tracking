import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

import urllib.parse
from config.globals import SHEET_ID
from src.sheets import get_gspread_client

def test():
    GC = get_gspread_client(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    sheet_title = "ENDER (SS)"
    
    q_range_sheet = urllib.parse.quote(f"'{sheet_title}'")
    url_sheet = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}?ranges={q_range_sheet}&includeGridData=false"
    res_sheet = GC.http_client.session.get(url_sheet).json()
    sheet_s = res_sheet.get('sheets', [])[0]
    
    cf_rules = sheet_s.get('conditionalFormats', [])
    print(f"Total Conditional Format Rules on {sheet_title}: {len(cf_rules)}")
    for idx, rule in enumerate(cf_rules):
        rule_info = rule.get('rule', {})
        boolean_rule = rule_info.get('booleanRule', {})
        condition = boolean_rule.get('condition', {})
        ranges = rule.get('ranges', [])
        
        # Simplify range info for display
        range_details = [
            f"Col [{r.get('startColumnIndex', 0)} to {r.get('endColumnIndex', 0)}], Row [{r.get('startRowIndex', 0)} to {r.get('endRowIndex', 0)}]"
            for r in ranges
        ]
        
        print(f"Rule {idx + 1}:")
        print(f"  Type: {condition.get('type')}")
        print(f"  Values: {condition.get('values')}")
        print(f"  Ranges: {range_details}")

if __name__ == '__main__':
    test()
