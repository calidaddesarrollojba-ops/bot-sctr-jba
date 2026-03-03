from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
from sheets_repo import SheetsRepo

class LoggingRepo:
    def __init__(self, sheets: SheetsRepo, tab_log: str, tz_name: str):
        self.sheets = sheets
        self.tab = tab_log
        self.tz = ZoneInfo(tz_name)

    def log(
        self,
        chat_id: int,
        user_id: int,
        username: str,
        rol_detectado: str,
        accion: str,
        detalle: str,
        resultado: str,
        archivo_origen: str = "",
        file_id_drive: str = "",
        latencia_ms: str = "",
    ):
        ts = datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")
        self.sheets.append_row_by_headers(self.tab, {
            "timestamp": ts,
            "chat_id": str(chat_id),
            "user_id": str(user_id),
            "username": username or "",
            "rol_detectado": rol_detectado,
            "accion": accion,
            "detalle": detalle,
            "resultado": resultado,
            "archivo_origen": archivo_origen,
            "file_id_drive": file_id_drive,
            "latencia_ms": latencia_ms,
        })