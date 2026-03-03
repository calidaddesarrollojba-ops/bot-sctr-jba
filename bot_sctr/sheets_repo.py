import json
from typing import Dict, List, Optional
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

class SheetsRepo:
    def __init__(self, creds_json_text: str, sheet_id: str):
        creds_info = json.loads(creds_json_text)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(sheet_id)

    def _ws(self, tab: str):
        return self.sh.worksheet(tab)

    def get_all_records(self, tab: str) -> List[Dict]:
        ws = self._ws(tab)
        return ws.get_all_records()

    def append_row_by_headers(self, tab: str, row_dict: Dict[str, object]):
        ws = self._ws(tab)
        headers = ws.row_values(1)
        row = [row_dict.get(h, "") for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")