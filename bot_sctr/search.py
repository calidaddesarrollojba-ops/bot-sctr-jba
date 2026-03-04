import re
from typing import List, Dict
from datetime import datetime
import pytz


# ---------------------------------------------------------
# LIMPIAR DIGITOS
# ---------------------------------------------------------
def clean_digits(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\D", "", str(text))


# ---------------------------------------------------------
# NORMALIZAR DOCUMENTO (FIX CEROS INICIALES)
# ---------------------------------------------------------
def normalize_doc(doc: str) -> str:
    """
    Normaliza DNI / CE para evitar problemas con ceros iniciales.

    Reglas:
    DNI → 8 dígitos
    CE  → 9 dígitos
    """

    digits = clean_digits(doc)

    if len(digits) <= 8:
        return digits.zfill(8)

    return digits.zfill(9)


# ---------------------------------------------------------
# MASCAR DOCUMENTO
# ---------------------------------------------------------
def mask_doc(doc: str) -> str:

    doc = clean_digits(doc)

    if len(doc) <= 4:
        return doc

    return "*" * (len(doc) - 4) + doc[-4:]


# ---------------------------------------------------------
# FORMATEAR FECHA
# ---------------------------------------------------------
def format_fecha(fecha: str, tz_name: str = "America/Lima") -> str:

    if not fecha:
        return "—"

    try:
        dt = datetime.strptime(fecha[:10], "%Y-%m-%d")
        tz = pytz.timezone(tz_name)
        dt = tz.localize(dt)
        return dt.strftime("%d/%m/%Y")
    except:
        return fecha


# ---------------------------------------------------------
# FICHA DEL ASEGURADO
# ---------------------------------------------------------
def build_ficha(r: Dict, tz_name: str) -> str:

    nombre = r.get("apellidos_y_nombres", "")
    doc = mask_doc(r.get("doc_norm", r.get("nro_doc", "")))

    empresa = r.get("empresa", "—")
    modelo = r.get("modelo", "—")

    fecha_inicio = format_fecha(r.get("fecha_inicio", ""), tz_name)
    fecha_fin = format_fecha(r.get("fecha_fin", ""), tz_name)

    estado = r.get("estado", "—")

    ficha = f"""
📋 **DATOS DEL ASEGURADO**

👤 Nombre: {nombre}
🪪 Documento: {doc}

🏢 Empresa: {empresa}
📑 Modelo: {modelo}

📅 Inicio: {fecha_inicio}
📅 Fin: {fecha_fin}

📊 Estado: {estado}
"""

    return ficha.strip()


# ---------------------------------------------------------
# BUSCAR POR DOCUMENTO
# ---------------------------------------------------------
def find_by_doc(data: List[Dict], doc: str) -> List[Dict]:

    doc_norm = normalize_doc(doc)

    results = []

    for r in data:

        sheet_doc = normalize_doc(
            r.get("doc_norm", r.get("nro_doc", ""))
        )

        if sheet_doc == doc_norm:
            results.append(r)

    return results


# ---------------------------------------------------------
# BUSCAR POR APELLIDOS
# ---------------------------------------------------------
def find_by_apellidos(data: List[Dict], paterno: str, materno: str) -> List[Dict]:

    paterno = paterno.strip().lower()
    materno = materno.strip().lower()

    results = []

    for r in data:

        nombre = str(r.get("apellidos_y_nombres", "")).lower()

        if paterno in nombre and materno in nombre:
            results.append(r)

    return results
