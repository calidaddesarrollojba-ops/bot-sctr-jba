import re
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

def norm_text(s: str) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_digits(s: str) -> str:
    return re.sub(r"[^0-9]", "", (s or ""))

def mask_doc(doc_norm: str) -> str:
    d = clean_digits(doc_norm)
    if len(d) == 8:
        return "****" + d[-4:]
    if len(d) == 9:
        return "*****" + d[-4:]
    return "****" + (d[-4:] if len(d) >= 4 else d)

def parse_sheet_date(v) -> Optional[date]:
    # gspread get_all_records usually returns either a date-like string or empty.
    if v in (None, ""):
        return None
    s = str(v).strip()
    # Try common formats: YYYY-MM-DD, DD/MM/YYYY
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None

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

def find_by_doc(records: List[Dict], doc_norm: str) -> List[Dict]:
    target = clean_digits(doc_norm)
    out = []
    for r in records:
        dn = clean_digits(str(r.get("doc_norm", r.get("nro_doc", ""))))
        if dn == target and dn != "":
            out.append(r)
    return out

def find_by_apellidos(records: List[Dict], paterno: str, materno: str) -> List[Dict]:
    p = norm_text(paterno)
    m = norm_text(materno)
    out = []
    for r in records:
        rp = norm_text(str(r.get("apellido_paterno", "")))
        rm = norm_text(str(r.get("apellido_materno", "")))
        if rp == p and rm == m:
            out.append(r)
    return out

def build_ficha(r: Dict, tz_name: str) -> str:
    nombre = str(r.get("apellidos_y_nombres", "")).strip()
    empresa = str(r.get("empresa", "")).strip()
    desde = str(r.get("vigencia_desde", "")).strip()
    hasta = str(r.get("vigencia_hasta", "")).strip()

    est, days_left, days_over = compute_status(hasta, tz_name)
    lines = [
        f"Nombre: {nombre}",
        f"Empresa: {empresa}",
        f"Inicio: {desde}",
        f"Final: {hasta if hasta else '—'}",
        f"Estado: {est}",
    ]
    if est == "ACTIVO" and days_left is not None:
        lines.append(f"Días restantes: {days_left}")
    elif est == "VENCIDO" and days_over is not None:
        lines.append(f"Vencido hace: {days_over} días")
    else:
        lines.append("Días: N/A")

    return "\n".join(lines)