import io
import json
from typing import Tuple
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]

class DriveRepo:
    def __init__(self, creds_json_text: str):
        creds_info = json.loads(creds_json_text)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        self.svc = build("drive", "v3", credentials=creds, cache_discovery=False)

    def download_file(self, file_id: str) -> Tuple[bytes, str]:
        meta = self.svc.files().get(fileId=file_id, fields="name,mimeType").execute()
        name = meta.get("name", "documento.pdf")

        request = self.svc.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        return fh.getvalue(), name