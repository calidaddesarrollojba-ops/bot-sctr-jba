from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


class SheetsRepo:
    def __init__(self, google_creds_json_text: str, sheet_id: str):
        creds_dict = json.loads(google_creds_json_text)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_key(sheet_id)

    def ws(self, tab_name: str):
        return self.sheet.worksheet(tab_name)

    def get_all_records(self, tab_name: str) -> List[Dict[str, Any]]:
        ws = self.ws(tab_name)
        # gspread get_all_records as dict by headers, default empties
        return ws.get_all_records(default_blank="")

    def get_headers(self, tab_name: str) -> List[str]:
        ws = self.ws(tab_name)
        headers = ws.row_values(1)
        return [h.strip() for h in headers]

    def append_row_by_headers(self, tab_name: str, row: Dict[str, Any]) -> None:
        ws = self.ws(tab_name)
        headers = self.get_headers(tab_name)
        values = [row.get(h, "") for h in headers]
        ws.append_row(values, value_input_option="USER_ENTERED")

    def upsert_by_key(self, tab_name: str, key_col: str, row: Dict[str, Any]) -> str:
        """
        Upsert por columna llave (ej: user_id). Si existe, actualiza la fila.
        Si no existe, agrega nueva fila.
        Retorna "updated" o "inserted".
        """
        ws = self.ws(tab_name)
        headers = self.get_headers(tab_name)

        if key_col not in headers:
            raise ValueError(f"key_col '{key_col}' no existe en headers de {tab_name}")

        key_value = str(row.get(key_col, "")).strip()
        if not key_value:
            raise ValueError(f"row[{key_col}] vacío")

        key_idx = headers.index(key_col) + 1  # 1-based
        # Leer solo la columna key (desde fila 2)
        col_values = ws.col_values(key_idx)
        # col_values incluye header en posición 0
        target_row_index: Optional[int] = None
        for i, v in enumerate(col_values[1:], start=2):
            if str(v).strip() == key_value:
                target_row_index = i
                break

        values = [row.get(h, "") for h in headers]

        if target_row_index is None:
            ws.append_row(values, value_input_option="USER_ENTERED")
            return "inserted"

        # Actualiza rango completo de la fila
        start_cell = gspread.utils.rowcol_to_a1(target_row_index, 1)
        end_cell = gspread.utils.rowcol_to_a1(target_row_index, len(headers))
        ws.update(f"{start_cell}:{end_cell}", [values], value_input_option="USER_ENTERED")
        return "updated"

    def delete_by_key(self, tab_name: str, key_col: str, key_value: str) -> bool:
        """
        Elimina la fila que coincida con key_value en key_col.
        Retorna True si borró, False si no encontró.
        """
        ws = self.ws(tab_name)
        headers = self.get_headers(tab_name)

        if key_col not in headers:
            raise ValueError(f"key_col '{key_col}' no existe en headers de {tab_name}")

        key_idx = headers.index(key_col) + 1
        col_values = ws.col_values(key_idx)

        for i, v in enumerate(col_values[1:], start=2):
            if str(v).strip() == str(key_value).strip():
                ws.delete_rows(i)
                return True
        return False
