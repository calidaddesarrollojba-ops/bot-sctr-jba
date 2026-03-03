import os

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v

BOT_TOKEN = env("BOT_TOKEN")
SHEET_ID = env("SHEET_ID")

TAB_ASEGURADOS = os.getenv("TAB_ASEGURADOS", "Asegurados")
TAB_USUARIOS = os.getenv("TAB_USUARIOS", "USUARIOS_AUTORIZADOS")
TAB_LOG = os.getenv("TAB_LOG", "LOG_INTERACCIONES")

GOOGLE_CREDS_JSON_TEXT = env("GOOGLE_CREDS_JSON_TEXT")

TZ_NAME = os.getenv("TZ", "America/Lima")
SESSION_TTL_MIN = int(os.getenv("SESSION_TTL_MIN", "5"))
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "10"))