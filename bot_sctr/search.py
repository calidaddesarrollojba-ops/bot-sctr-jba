import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date
from zoneinfo import ZoneInfo


# -----------------------------
# Helpers básicos
# -----------------------------
def clean_digits(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\D", "", str(text))


def normalize_doc(doc: str) -> str:
    """
    Normaliza DNI/CE para evitar problemas con ceros iniciales.
    Reglas:
    - Si tiene 7 u 8 dígitos -> DNI (8)
    - Si tiene 9 dígitos -> CE (9)
    """
    digits = clean_digits(doc)
    if len(digits) <= 8:
        return digits.zfill(8)
    return digits.zfill(9)


def mask_doc(doc: str) -> str:
    d = clean_digits(doc)
    if len(d) <= 4:
        return d
    return "*" * (len(d) - 4) + d[-4:]


def norm_text(s: str) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


# -----------------------------
# Fechas y estado
# -----------------------------
def parse_sheet_date(v) -> Optional[date]:
    """
    Convierte un valor de Sheets a date, soportando:
    - 'YYYY-MM-DD'
    - 'DD/MM/YYYY'
    - 'YYYY/MM/DD'
    """
    if v in (None, ""):
        return None

    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None


def format_date_ddmmyyyy(v, tz_name: str) -> str:
    d = parse_sheet_date(v)
    if not d:
        return "—"
    # salida tipo: 1/3/2026 (sin cero a la izquierda)
    return f"{d.day}/{d.month}/{d.year}"


def compute_status(vig_hasta, tz_name: str) -> Tuple[str, Optional[int], Optional[int]]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    hasta = parse_sheet_date(vig_hasta)

    if not hasta:
        return ("SIN FECHA", None, None)

    if today <= hasta:
        days_left = (hasta - today).days
        return ("ACTIVO", days_left, None)
    else:
        days_over = (today - hasta).days
        return ("VENCIDO", None, days_over)


# -----------------------------
# Búsquedas
# -----------------------------
def find_by_doc(data: List[Dict], doc: str) -> List[Dict]:
    target = normalize_doc(doc)
    out: List[Dict] = []
    for r in data:
        sheet_doc = normalize_doc(r.get("doc_norm", r.get("nro_doc", "")))
        if sheet_doc == target and sheet_doc != "":
            out.append(r)
    return out


def find_by_apellidos(data: List[Dict], paterno: str, materno: str) -> List[Dict]:
    p = norm_text(paterno)
    m = norm_text(materno)
    out: List[Dict] = []
    for r in data:
        rp = norm_text(str(r.get("apellido_paterno", "")))
        rm = norm_text(str(r.get("apellido_materno", "")))
        if rp == p and rm == m:
            out.append(r)
    return out


# -----------------------------
# Ficha final (CORREGIDA)
# -----------------------------
def build_ficha(r: Dict, tz_name: str) -> str:
    nombre = str(r.get("apellidos_y_nombres", "")).strip()
    empresa = str(r.get("empresa", "")).strip()

    vig_desde = r.get("vigencia_desde", "")
    vig_hasta = r.get("vigencia_hasta", "")

    inicio_txt = format_date_ddmmyyyy(vig_desde, tz_name)
    final_txt = format_date_ddmmyyyy(vig_hasta, tz_name)

    est, days_left, days_over = compute_status(vig_hasta, tz_name)

    lines = [
        "📋 **DATOS DEL ASEGURADO**",
        "",
        f"👤 Nombre: {nombre}",
        f"🏢 Empresa: {empresa}",
        f"📅 Inicio: {inicio_txt}",
        f"📅 Final: {final_txt}",
        f"📊 Estado: {est}",
    ]

    if est == "ACTIVO" and days_left is not None:
        lines.append(f"Días restantes: {days_left}")
    elif est == "VENCIDO" and days_over is not None:
        lines.append(f"Vencido hace: {days_over} días")

    return "\n".join(lines)
    
