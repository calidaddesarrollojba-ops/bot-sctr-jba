PRIVATE_ONLY_MSG = (
    "🔒 Por seguridad, este bot solo funciona en chat privado.\n"
    "Escríbeme directamente para realizar consultas."
)

START_MSG = (
    "🔎 Bot SCTR\n\n"
    "Este bot registra consultas para control operativo y auditoría.\n"
    "Usa /busqueda para iniciar.\n\n"
    "Si necesitas habilitación, usa /id y envía tu ID al administrador."
)

HELP_MSG = (
    "Comandos:\n"
    "/id - muestra tu user_id (para habilitación)\n"
    "/busqueda - consulta SCTR (solo autorizados)\n"
    "/cancelar - cancela la búsqueda\n"
)

NOT_AUTH_MSG = (
    "❌ No tienes autorización para usar este bot.\n"
    "Envía /id y solicita habilitación."
)

ASK_METHOD_MSG = "Elige un método de búsqueda:"
ASK_DOC_MSG = "Ingresa DNI (8 dígitos) o CE (9 dígitos):"
ASK_PAT_MSG = "Ingresa apellido paterno (ej: CASTILLO):"
ASK_MAT_MSG = "Ingresa apellido materno (ej: BALLENA):"

NO_FOUND_DOC = "No encontré ese documento en el padrón."
NO_FOUND_AP = "No encontré coincidencias exactas con esos apellidos."

TOO_MANY = "Hay demasiadas coincidencias. Para afinar, usa BÚSQUEDA POR DOCUMENTO."
EXPIRED_MSG = "⏳ Búsqueda expirada por inactividad. Escribe /busqueda para iniciar de nuevo."
CANCELLED_MSG = "✅ Búsqueda cancelada. Escribe /busqueda para iniciar."