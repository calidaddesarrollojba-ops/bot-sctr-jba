# bot_sctr/bot_sctr.py
# Procfile:
#   worker: python -m bot_sctr.bot_sctr

from __future__ import annotations

from typing import List, Dict, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from . import config
from . import messages as msg
from .sheets_repo import SheetsRepo
from .drive_repo import DriveRepo
from .authz import Authz
from .session import SessionManager
from .logging_repo import LoggingRepo
from .search import (
    clean_digits,
    mask_doc,
    build_ficha,
    find_by_doc,
    find_by_apellidos,
)

# Callback data constants
CB_DOC = "M_DOC"
CB_AP = "M_AP"
CB_MENU = "M_MENU"
CB_CANCEL = "M_CANCEL"
CB_PICK_PREFIX = "PICK_"  # PICK_0, PICK_1...

# Estados admin (asistente)
ADMIN_NEWUSER_WAIT = "ADMIN_NEWUSER_WAIT"


def now_str(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)


def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("BÚSQUEDA POR DOCUMENTO", callback_data=CB_DOC)],
            [InlineKeyboardButton("BÚSQUEDA POR APELLIDOS", callback_data=CB_AP)],
            [InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)],
        ]
    )


def kb_back_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ Menú", callback_data=CB_MENU)],
            [InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)],
        ]
    )


def kb_pick(n: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i in range(n):
        row.append(InlineKeyboardButton(str(i + 1), callback_data=f"{CB_PICK_PREFIX}{i}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("↩️ Menú", callback_data=CB_MENU)])
    rows.append([InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)])
    return InlineKeyboardMarkup(rows)


async def only_private_guard(update: Update, logger: LoggingRepo, authz: Authz) -> bool:
    """
    Bloquea uso en grupos/canales. En grupo responde el mensaje de seguridad y loggea.
    """
    if is_private(update):
        return True

    user = update.effective_user
    chat = update.effective_chat
    uid = user.id if user else 0
    role = authz.role(uid)

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.effective_message.reply_text(msg.PRIVATE_ONLY_MSG)
        except Exception:
            pass
    else:
        try:
            await update.effective_message.reply_text(msg.PRIVATE_ONLY_MSG)
        except Exception:
            pass

    text = ""
    try:
        text = (update.effective_message.text or "").strip()
    except Exception:
        pass

    logger.log(
        chat_id=chat.id if chat else 0,
        user_id=uid,
        username=(user.username if user else ""),
        rol_detectado=role,
        accion="intento_en_grupo",
        detalle=f"CMD:{text[:30]}",
        resultado="denegado",
    )
    return False


def load_caches(sheets: SheetsRepo, authz: Authz) -> List[Dict]:
    usuarios = sheets.get_all_records(config.TAB_USUARIOS)
    authz.load(usuarios)
    asegurados = sheets.get_all_records(config.TAB_ASEGURADOS)
    return asegurados


def parse_args(text: str) -> List[str]:
    parts = (text or "").strip().split()
    return parts[1:]


def get_forwarded_user_id(m: Message) -> Optional[int]:
    """
    Telegram a veces no expone forward_from por privacidad.
    Si está disponible, lo usamos. Si no, retorna None.
    """
    try:
        if m.forward_from:
            return m.forward_from.id
    except Exception:
        pass
    return None


def normalize_role(role: str) -> Optional[str]:
    r = (role or "").strip().lower()
    if r in ("superadmin", "admin", "user"):
        return r
    return None


async def require_admin(update: Update, logger: LoggingRepo, authz: Authz) -> Tuple[bool, str]:
    """
    Retorna (ok, role). Requiere que sea privado, autorizado y rol admin/superadmin.
    """
    if not await only_private_guard(update, logger, authz):
        return (False, "")

    u = update.effective_user
    chat = update.effective_chat

    if not authz.is_allowed(u.id):
        await update.effective_message.reply_text(msg.NOT_AUTH_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="admin_check",
            detalle="DENEGADO:no_autorizado",
            resultado="denegado",
        )
        return (False, "")

    role = authz.role(u.id)
    if role not in ("admin", "superadmin"):
        await update.effective_message.reply_text("❌ Solo administradores pueden usar este comando.")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="admin_check",
            detalle="DENEGADO:rol",
            resultado="denegado",
        )
        return (False, role)

    return (True, role)


# -------------------- Core user handlers --------------------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(msg.START_MSG)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(msg.HELP_MSG)


async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE, logger: LoggingRepo, authz: Authz) -> None:
    if not is_private(update):
        return
    u = update.effective_user
    await update.message.reply_text(f"Tu ID es: `{u.id}`", parse_mode="Markdown")
    logger.log(
        chat_id=update.effective_chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=authz.role(u.id),
        accion="cmd_id",
        detalle="MOSTRADO",
        resultado="ok",
    )


async def cmd_mi_rol(update: Update, ctx: ContextTypes.DEFAULT_TYPE, authz: Authz, logger: LoggingRepo) -> None:
    if not await only_private_guard(update, logger, authz):
        return
    u = update.effective_user
    chat = update.effective_chat
    role = authz.role(u.id)
    allowed = authz.is_allowed(u.id)
    await update.effective_message.reply_text(
        f"👤 ID: {u.id}\n🔐 Autorizado: {'SI' if allowed else 'NO'}\n🧩 Rol: {role or '—'}"
    )
    logger.log(
        chat_id=chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=role,
        accion="mi_rol",
        detalle=f"allowed={allowed}",
        resultado="ok",
    )


async def cmd_busqueda(update: Update, ctx: ContextTypes.DEFAULT_TYPE, authz: Authz, sessions: SessionManager, logger: LoggingRepo) -> None:
    if not await only_private_guard(update, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat

    if not authz.is_allowed(u.id):
        await update.message.reply_text(msg.NOT_AUTH_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="intento_no_autorizado",
            detalle="CMD:/busqueda",
            resultado="denegado",
        )
        return

    sessions.reset(u.id)
    s = sessions.get(u.id)
    s.state = "CHOOSE_METHOD"
    sessions.touch(u.id)

    await update.message.reply_text(msg.ASK_METHOD_MSG, reply_markup=kb_main())
    logger.log(
        chat_id=chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=authz.role(u.id),
        accion="cmd_busqueda",
        detalle="INICIO",
        resultado="ok",
    )


async def cmd_cancelar(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sessions: SessionManager, logger: LoggingRepo, authz: Authz) -> None:
    if not is_private(update):
        return
    u = update.effective_user
    chat = update.effective_chat
    st = sessions.get(u.id).state
    sessions.reset(u.id)
    await update.message.reply_text(msg.CANCELLED_MSG)
    logger.log(
        chat_id=chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=authz.role(u.id),
        accion="cancelar",
        detalle=f"CANCELADO_EN:{st}",
        resultado="ok",
    )


async def show_pick_list(update: Update, results: List[Dict], logger: LoggingRepo, authz: Authz, sessions: SessionManager) -> None:
    u = update.effective_user
    chat = update.effective_chat

    s = sessions.get(u.id)
    s.state = "WAIT_PICK"
    s.ctx["pick_results"] = results

    lines = ["Se encontraron varias coincidencias. Elige la persona correcta:"]
    for i, r in enumerate(results, start=1):
        nombre = str(r.get("apellidos_y_nombres", "")).strip()
        docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
        lines.append(f"{i}) {nombre} — DOC: {docm}")

    await update.effective_message.reply_text("\n".join(lines), reply_markup=kb_pick(len(results)))
    logger.log(
        chat_id=chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=authz.role(u.id),
        accion="lista_resultados",
        detalle=f"LISTA:{len(results)}",
        resultado="multiple",
    )


async def deliver_record(update: Update, ctx: ContextTypes.DEFAULT_TYPE, r: Dict, drive: DriveRepo, logger: LoggingRepo, authz: Authz) -> None:
    u = update.effective_user
    chat = update.effective_chat

    ficha = build_ficha(r, config.TZ_NAME)
    docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
    nombre = str(r.get("apellidos_y_nombres", "")).strip()

    await update.effective_message.reply_text(ficha, parse_mode="Markdown")
    logger.log(
        chat_id=chat.id,
        user_id=u.id,
        username=u.username or "",
        rol_detectado=authz.role(u.id),
        accion="respuesta_ficha",
        detalle=f"NOMBRE:{nombre} | DOC:{docm}",
        resultado="ok",
    )

    archivo_origen = str(r.get("archivo_origen", "")).strip()
    file_id = str(r.get("file_id_drive", "")).strip()

    if not file_id:
        await update.effective_message.reply_text("📎 PDF no disponible (falta file_id).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="envio_pdf",
            detalle=f"PDF_FAIL | motivo:file_id_vacio | ARCHIVO:{archivo_origen}",
            resultado="fallo_pdf",
            archivo_origen=archivo_origen,
        )
        return

    try:
        content, name = drive.download_file(file_id)
        await ctx.bot.send_document(
            chat_id=chat.id,
            document=content,
            filename=archivo_origen or name,
            caption=archivo_origen or name,
        )
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="envio_pdf",
            detalle=f"PDF_OK | ARCHIVO:{archivo_origen or name}",
            resultado="ok",
            archivo_origen=archivo_origen or name,
            file_id_drive=file_id[:10],
        )
    except Exception as e:
        await update.effective_message.reply_text("📎 No pude adjuntar el PDF (revisar permisos/archivo).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="envio_pdf",
            detalle=f"PDF_FAIL | motivo:{type(e).__name__} | ARCHIVO:{archivo_origen}",
            resultado="fallo_pdf",
            archivo_origen=archivo_origen,
            file_id_drive=file_id[:10],
        )


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, asegurados_cache: List[Dict], drive: DriveRepo, authz: Authz, sessions: SessionManager, logger: LoggingRepo) -> None:
    q = update.callback_query
    await q.answer()

    if not await only_private_guard(update, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat

    if not authz.is_allowed(u.id):
        await q.edit_message_text(msg.NOT_AUTH_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="intento_no_autorizado",
            detalle="BTN",
            resultado="denegado",
        )
        return

    if sessions.is_expired(u.id):
        sessions.reset(u.id)
        await q.edit_message_text(msg.EXPIRED_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="expirado",
            detalle="EXPIRA",
            resultado="expirado",
        )
        return

    s = sessions.get(u.id)
    sessions.touch(u.id)
    data = q.data or ""

    if data == CB_MENU:
        s.state = "CHOOSE_METHOD"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_METHOD_MSG, reply_markup=kb_main())
        return

    if data == CB_CANCEL:
        sessions.reset(u.id)
        await q.edit_message_text(msg.CANCELLED_MSG)
        return

    if data == CB_DOC:
        s.state = "WAIT_DOC"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_DOC_MSG, reply_markup=kb_back_cancel())
        return

    if data == CB_AP:
        s.state = "WAIT_AP_PATERNO"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_PAT_MSG, reply_markup=kb_back_cancel())
        return

    if data.startswith(CB_PICK_PREFIX):
        if s.state != "WAIT_PICK":
            await q.edit_message_text(msg.EXPIRED_MSG)
            return

        try:
            idx = int(data.replace(CB_PICK_PREFIX, ""))
        except Exception:
            idx = -1

        results: List[Dict] = s.ctx.get("pick_results", [])
        if idx < 0 or idx >= len(results):
            await q.edit_message_text("⚠️ Opción inválida. Vuelve al menú.", reply_markup=kb_main())
            return

        r = results[idx]
        await deliver_record(update, ctx, r, drive, logger, authz)
        sessions.reset(u.id)
        return


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE, asegurados_cache: List[Dict], drive: DriveRepo, authz: Authz, sessions: SessionManager, logger: LoggingRepo) -> None:
    if not await only_private_guard(update, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat
    text = (update.message.text or "").strip()

    # ----- Admin assistant: /nuevo_usuario (sin args) -----
    s_admin = sessions.get(u.id)
    if s_admin.state == ADMIN_NEWUSER_WAIT:
        ok, role = await require_admin(update, logger, authz)
        if not ok:
            sessions.reset(u.id)
            return

        # Espera: "123456789 user" o solo "123456789" (pedirá rol)
        parts = text.split()
        if not parts:
            await update.effective_message.reply_text("⚠️ Envíame el user_id (y opcional rol). Ej: `123456789 user`", parse_mode="Markdown")
            return

        uid = clean_digits(parts[0])
        if not uid:
            await update.effective_message.reply_text("⚠️ No veo un user_id válido. Ej: `123456789 user`", parse_mode="Markdown")
            return

        rol = normalize_role(parts[1]) if len(parts) >= 2 else "user"
        if not rol:
            await update.effective_message.reply_text("⚠️ Rol inválido. Usa: superadmin | admin | user")
            return

        s_admin.ctx["pending_user_id"] = uid
        s_admin.ctx["pending_role"] = rol
        s_admin.state = ""  # salimos del asistente
        sessions.touch(u.id)

        await update.effective_message.reply_text(f"✅ Listo. Ejecuta ahora:\n`/nuevo_usuario {uid} {rol}`", parse_mode="Markdown")
        return

    # ----- Flujo normal -----
    if not authz.is_allowed(u.id):
        await update.message.reply_text(msg.NOT_AUTH_MSG)
        return

    if sessions.is_expired(u.id):
        sessions.reset(u.id)
        await update.message.reply_text(msg.EXPIRED_MSG)
        return

    s = sessions.get(u.id)
    sessions.touch(u.id)

    if s.state == "WAIT_DOC":
        digits = clean_digits(text)
        if len(digits) not in (7, 8, 9):
            await update.message.reply_text("⚠️ Documento inválido. Ingresa DNI (8) o CE (9).", reply_markup=kb_back_cancel())
            return

        found = find_by_doc(asegurados_cache, digits)
        if not found:
            await update.message.reply_text(msg.NO_FOUND_DOC, reply_markup=kb_main())
            sessions.reset(u.id)
            return

        if len(found) > 1:
            if len(found) > config.MAX_RESULTS:
                await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
                sessions.reset(u.id)
                return
            await show_pick_list(update, found, logger, authz, sessions)
            return

        await deliver_record(update, ctx, found[0], drive, logger, authz)
        sessions.reset(u.id)
        return

    if s.state == "WAIT_AP_PATERNO":
        s.ctx["paterno"] = text
        s.state = "WAIT_AP_MATERNO"
        await update.message.reply_text(msg.ASK_MAT_MSG, reply_markup=kb_back_cancel())
        return

    if s.state == "WAIT_AP_MATERNO":
        paterno = s.ctx.get("paterno", "")
        materno = text
        found = find_by_apellidos(asegurados_cache, paterno, materno)

        if not found:
            await update.message.reply_text(msg.NO_FOUND_AP, reply_markup=kb_main())
            sessions.reset(u.id)
            return

        if len(found) == 1:
            await deliver_record(update, ctx, found[0], drive, logger, authz)
            sessions.reset(u.id)
            return

        if len(found) > config.MAX_RESULTS:
            await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
            sessions.reset(u.id)
            return

        await show_pick_list(update, found, logger, authz, sessions)
        return

    await update.message.reply_text("Usa /busqueda para iniciar.", reply_markup=kb_main())


# -------------------- Admin commands --------------------

async def cmd_reload_sheet(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz, cache: Dict[str, List[Dict]], logger: LoggingRepo) -> None:
    ok, role = await require_admin(update, logger, authz)
    if not ok:
        return

    u = update.effective_user
    chat = update.effective_chat

    try:
        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        authz.load(usuarios)
        asegurados = sheets.get_all_records(config.TAB_ASEGURADOS)
        cache["asegurados"] = asegurados

        await update.effective_message.reply_text(
            f"✅ Recargado.\nAsegurados: {len(asegurados)}\nUsuarios: {len(usuarios)}"
        )

        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="reload_sheet",
            detalle=f"OK asegurados={len(asegurados)} usuarios={len(usuarios)}",
            resultado="ok",
        )
    except Exception as e:
        await update.effective_message.reply_text("⚠️ Error recargando datos (revisar permisos/Sheet).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="reload_sheet",
            detalle=f"ERROR:{type(e).__name__}",
            resultado="error",
        )


async def cmd_nuevo_usuario(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz, sessions: SessionManager, logger: LoggingRepo) -> None:
    ok, role = await require_admin(update, logger, authz)
    if not ok:
        return

    u = update.effective_user
    chat = update.effective_chat
    args = parse_args(update.effective_message.text or "")

    # Modo asistente (sin args)
    if len(args) == 0:
        s = sessions.get(u.id)
        s.state = ADMIN_NEWUSER_WAIT
        s.ctx.clear()
        sessions.touch(u.id)
        await update.effective_message.reply_text(
            "🧩 *Nuevo usuario*\n\n"
            "Envíame:\n"
            "• `user_id rol` (ej: `123456789 user`)\n"
            "o\n"
            "• Reenvíame un mensaje del usuario (si Telegram deja ver el ID)\n\n"
            "Roles: `superadmin | admin | user`",
            parse_mode="Markdown",
        )
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="nuevo_usuario",
            detalle="ASISTENTE_INICIADO",
            resultado="ok",
        )
        return

    # Modo manual /nuevo_usuario <id> <rol?>
    raw_id = args[0]
    uid_target = clean_digits(raw_id)
    if not uid_target:
        await update.effective_message.reply_text("⚠️ Formato: `/nuevo_usuario <user_id> <rol>`", parse_mode="Markdown")
        return

    rol = normalize_role(args[1]) if len(args) >= 2 else "user"
    if not rol:
        await update.effective_message.reply_text("⚠️ Rol inválido. Usa: superadmin | admin | user")
        return

    # Si no eres superadmin, no puedes crear superadmin
    if role != "superadmin" and rol == "superadmin":
        await update.effective_message.reply_text("❌ Solo un superadmin puede asignar rol superadmin.")
        return

    # Guardar en Sheet por upsert
    row = {
        "user_id": str(uid_target),
        "rol": rol,
        "activo": 1,
        "nombre": "",
        "username": "",
        "updated_at": now_str(config.TZ_NAME),
    }

    try:
        res = sheets.upsert_by_key(config.TAB_USUARIOS, "user_id", row)
        # refresca authz en caliente (sin /reload_sheet)
        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        authz.load(usuarios)

        await update.effective_message.reply_text(f"✅ Usuario {res}: {uid_target} | rol={rol} | activo=1")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="nuevo_usuario",
            detalle=f"{res} user_id={uid_target} rol={rol}",
            resultado="ok",
        )
    except Exception as e:
        await update.effective_message.reply_text("⚠️ No pude guardar el usuario (revisar Sheet/headers).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="nuevo_usuario",
            detalle=f"ERROR:{type(e).__name__}",
            resultado="error",
        )


async def cmd_bloquear_usuario(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz, logger: LoggingRepo) -> None:
    ok, role = await require_admin(update, logger, authz)
    if not ok:
        return

    u = update.effective_user
    chat = update.effective_chat
    args = parse_args(update.effective_message.text or "")
    if len(args) < 1:
        await update.effective_message.reply_text("Formato: `/bloquear_usuario <user_id>`", parse_mode="Markdown")
        return

    uid_target = clean_digits(args[0])
    if not uid_target:
        await update.effective_message.reply_text("⚠️ user_id inválido.")
        return

    # Evitar que un admin bloquee a un superadmin (seguridad)
    if role != "superadmin":
        # recarga usuarios en memoria para verificar rol target
        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        for r in usuarios:
            if str(r.get("user_id", "")).strip() == uid_target and str(r.get("rol", "")).strip().lower() == "superadmin":
                await update.effective_message.reply_text("❌ No puedes bloquear a un superadmin.")
                return

    try:
        row = {
            "user_id": str(uid_target),
            "activo": 0,
            "updated_at": now_str(config.TZ_NAME),
        }
        res = sheets.upsert_by_key(config.TAB_USUARIOS, "user_id", row)

        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        authz.load(usuarios)

        await update.effective_message.reply_text(f"🚫 Usuario bloqueado: {uid_target} (activo=0)")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="bloquear_usuario",
            detalle=f"{res} user_id={uid_target}",
            resultado="ok",
        )
    except Exception as e:
        await update.effective_message.reply_text("⚠️ No pude bloquear (revisar Sheet/headers).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="bloquear_usuario",
            detalle=f"ERROR:{type(e).__name__}",
            resultado="error",
        )


async def cmd_activar_usuario(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz, logger: LoggingRepo) -> None:
    ok, role = await require_admin(update, logger, authz)
    if not ok:
        return

    u = update.effective_user
    chat = update.effective_chat
    args = parse_args(update.effective_message.text or "")
    if len(args) < 1:
        await update.effective_message.reply_text("Formato: `/activar_usuario <user_id>`", parse_mode="Markdown")
        return

    uid_target = clean_digits(args[0])
    if not uid_target:
        await update.effective_message.reply_text("⚠️ user_id inválido.")
        return

    try:
        row = {
            "user_id": str(uid_target),
            "activo": 1,
            "updated_at": now_str(config.TZ_NAME),
        }
        res = sheets.upsert_by_key(config.TAB_USUARIOS, "user_id", row)

        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        authz.load(usuarios)

        await update.effective_message.reply_text(f"✅ Usuario activado: {uid_target} (activo=1)")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="activar_usuario",
            detalle=f"{res} user_id={uid_target}",
            resultado="ok",
        )
    except Exception as e:
        await update.effective_message.reply_text("⚠️ No pude activar (revisar Sheet/headers).")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="activar_usuario",
            detalle=f"ERROR:{type(e).__name__}",
            resultado="error",
        )


async def cmd_listar_usuarios(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz, logger: LoggingRepo) -> None:
    ok, role = await require_admin(update, logger, authz)
    if not ok:
        return

    u = update.effective_user
    chat = update.effective_chat

    try:
        usuarios = sheets.get_all_records(config.TAB_USUARIOS)
        # Orden: activos primero, luego rol, luego id
        def keyfn(r):
            activo = str(r.get("activo", "")).strip()
            activo_num = 1 if activo in ("1", "TRUE", "true") else 0
            rol = str(r.get("rol", "")).strip().lower()
            uid = str(r.get("user_id", "")).strip()
            return (-activo_num, rol, uid)

        usuarios_sorted = sorted(usuarios, key=keyfn)

        lines = ["👥 *USUARIOS AUTORIZADOS* (máx 20)\n"]
        count = 0
        for r in usuarios_sorted:
            uid = str(r.get("user_id", "")).strip()
            if not uid:
                continue
            rol_u = str(r.get("rol", "")).strip().lower() or "user"
            activo = str(r.get("activo", "")).strip()
            activo_ok = "✅" if activo in ("1", "TRUE", "true") else "🚫"
            lines.append(f"{activo_ok} `{uid}` — {rol_u}")
            count += 1
            if count >= 20:
                break

        await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="listar_usuarios",
            detalle=f"count={count}",
            resultado="ok",
        )
    except Exception as e:
        await update.effective_message.reply_text("⚠️ No pude listar usuarios.")
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=role,
            accion="listar_usuarios",
            detalle=f"ERROR:{type(e).__name__}",
            resultado="error",
        )


# -------------------- Bootstrap --------------------

def main() -> None:
    sheets = SheetsRepo(config.GOOGLE_CREDS_JSON_TEXT, config.SHEET_ID)
    drive = DriveRepo(config.GOOGLE_CREDS_JSON_TEXT)
    authz = Authz()
    sessions = SessionManager(config.SESSION_TTL_MIN)
    logger = LoggingRepo(sheets, config.TAB_LOG, config.TZ_NAME)

    cache: Dict[str, List[Dict]] = {"asegurados": load_caches(sheets, authz)}

    app = Application.builder().token(config.BOT_TOKEN).build()

    # Base
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))

    async def _id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await only_private_guard(update, logger, authz):
            return
        await cmd_id(update, ctx, logger, authz)

    app.add_handler(CommandHandler("id", _id))

    async def _mi_rol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_mi_rol(update, ctx, authz, logger)

    app.add_handler(CommandHandler("mi_rol", _mi_rol))

    async def _busqueda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_busqueda(update, ctx, authz, sessions, logger)

    app.add_handler(CommandHandler("busqueda", _busqueda))

    async def _cancelar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_cancelar(update, ctx, sessions, logger, authz)

    app.add_handler(CommandHandler("cancelar", _cancelar))

    # Admin
    async def _reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_reload_sheet(update, ctx, sheets, authz, cache, logger)

    app.add_handler(CommandHandler("reload_sheet", _reload))

    async def _nuevo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_nuevo_usuario(update, ctx, sheets, authz, sessions, logger)

    app.add_handler(CommandHandler("nuevo_usuario", _nuevo))

    async def _bloquear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_bloquear_usuario(update, ctx, sheets, authz, logger)

    app.add_handler(CommandHandler("bloquear_usuario", _bloquear))

    async def _activar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_activar_usuario(update, ctx, sheets, authz, logger)

    app.add_handler(CommandHandler("activar_usuario", _activar))

    async def _listar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_listar_usuarios(update, ctx, sheets, authz, logger)

    app.add_handler(CommandHandler("listar_usuarios", _listar))

    # Callbacks
    async def _cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_callback(update, ctx, cache["asegurados"], drive, authz, sessions, logger)

    app.add_handler(CallbackQueryHandler(_cb))

    # Texto normal
    async def _text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_text(update, ctx, cache["asegurados"], drive, authz, sessions, logger)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _text))

    print("Bot running (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
