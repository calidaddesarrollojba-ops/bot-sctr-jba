# bot_sctr/bot_sctr.py
# Ejecuta como módulo (Railway Procfile):
#   worker: python -m bot_sctr.bot_sctr

from __future__ import annotations

from typing import List, Dict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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


async def only_private_guard(
    update: Update,
    logger: LoggingRepo,
    authz: Authz,
) -> bool:
    """
    Bloquea cualquier uso en grupos/canales. En grupo responde el mensaje de seguridad y loggea.
    """
    if is_private(update):
        return True

    user = update.effective_user
    chat = update.effective_chat
    uid = user.id if user else 0
    role = authz.role(uid)

    # Responde en el grupo con mensaje estándar
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

    # Log
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
    """
    Carga Asegurados y Usuarios en memoria al arranque.
    """
    usuarios = sheets.get_all_records(config.TAB_USUARIOS)
    authz.load(usuarios)
    asegurados = sheets.get_all_records(config.TAB_ASEGURADOS)
    return asegurados


# -------------------- Handlers --------------------


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


async def cmd_busqueda(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    authz: Authz,
    sessions: SessionManager,
    logger: LoggingRepo,
) -> None:
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


async def cmd_cancelar(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    sessions: SessionManager,
    logger: LoggingRepo,
    authz: Authz,
) -> None:
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


async def show_pick_list(
    update: Update,
    results: List[Dict],
    logger: LoggingRepo,
    authz: Authz,
    sessions: SessionManager,
) -> None:
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


async def deliver_record(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    r: Dict,
    drive: DriveRepo,
    logger: LoggingRepo,
    authz: Authz,
) -> None:
    u = update.effective_user
    chat = update.effective_chat

    ficha = build_ficha(r, config.TZ_NAME)
    docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
    nombre = str(r.get("apellidos_y_nombres", "")).strip()

    # 1) Ficha
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

    # 2) PDF
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
            file_id_drive=file_id[:8],  # opcional: solo primeros 8
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
            file_id_drive=file_id[:8],
        )


async def on_callback(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    asegurados_cache: List[Dict],
    drive: DriveRepo,
    authz: Authz,
    sessions: SessionManager,
    logger: LoggingRepo,
) -> None:
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
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="menu_busqueda",
            detalle="MOSTRADO",
            resultado="ok",
        )
        return

    if data == CB_CANCEL:
        st = s.state
        sessions.reset(u.id)
        await q.edit_message_text(msg.CANCELLED_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="cancelar",
            detalle=f"CANCELADO_EN:{st}",
            resultado="ok",
        )
        return

    if data == CB_DOC:
        s.state = "WAIT_DOC"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_DOC_MSG, reply_markup=kb_back_cancel())
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="metodo_seleccionado",
            detalle="METODO:DOCUMENTO",
            resultado="ok",
        )
        return

    if data == CB_AP:
        s.state = "WAIT_AP_PATERNO"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_PAT_MSG, reply_markup=kb_back_cancel())
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="metodo_seleccionado",
            detalle="METODO:APELLIDOS",
            resultado="ok",
        )
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
            await q.edit_message_text("⚠️ Opción inválida. Vuelve a intentar.", reply_markup=kb_main())
            return

        r = results[idx]
        docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
        nombre = str(r.get("apellidos_y_nombres", "")).strip()

        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="seleccion_opcion",
            detalle=f"SEL:{idx+1} | NOMBRE:{nombre} | DOC:{docm}",
            resultado="ok",
        )

        await deliver_record(update, ctx, r, drive, logger, authz)
        sessions.reset(u.id)
        return


async def on_text(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    asegurados_cache: List[Dict],
    drive: DriveRepo,
    authz: Authz,
    sessions: SessionManager,
    logger: LoggingRepo,
) -> None:
    if not await only_private_guard(update, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat
    text = (update.message.text or "").strip()

    if not authz.is_allowed(u.id):
        await update.message.reply_text(msg.NOT_AUTH_MSG)
        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="intento_no_autorizado",
            detalle="TXT",
            resultado="denegado",
        )
        return

    if sessions.is_expired(u.id):
        sessions.reset(u.id)
        await update.message.reply_text(msg.EXPIRED_MSG)
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

    if s.state == "WAIT_DOC":
        digits = clean_digits(text)
        if len(digits) not in (8, 9, 7):
            await update.message.reply_text(
                "⚠️ Documento inválido. Ingresa DNI (8) o CE (9).",
                reply_markup=kb_back_cancel(),
            )
            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="input_documento",
                detalle=f"DOC_INVALIDO:len={len(digits)}",
                resultado="error_formato",
            )
            return

        docm = mask_doc(digits)
        found = find_by_doc(asegurados_cache, digits)

        if not found:
            await update.message.reply_text(msg.NO_FOUND_DOC, reply_markup=kb_main())
            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="buscar_doc",
                detalle=f"HIT:0 DOC:{docm}",
                resultado="no_encontrado",
            )
            sessions.reset(u.id)
            return

        if len(found) > 1:
            if len(found) > config.MAX_RESULTS:
                await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
                logger.log(
                    chat_id=chat.id,
                    user_id=u.id,
                    username=u.username or "",
                    rol_detectado=authz.role(u.id),
                    accion="buscar_doc",
                    detalle=f"HIT:{len(found)} DOC:{docm}",
                    resultado="demasiados",
                )
                sessions.reset(u.id)
                return

            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="buscar_doc",
                detalle=f"HIT:{len(found)} DOC:{docm}",
                resultado="multiple",
            )
            await show_pick_list(update, found, logger, authz, sessions)
            return

        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="buscar_doc",
            detalle=f"HIT:1 DOC:{docm}",
            resultado="ok",
        )
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
            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="buscar_apellidos",
                detalle=f"APELLIDOS:{paterno} {materno} | HIT:0",
                resultado="no_encontrado",
            )
            sessions.reset(u.id)
            return

        if len(found) == 1:
            r = found[0]
            docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="buscar_apellidos",
                detalle=f"APELLIDOS:{paterno} {materno} | HIT:1 | DOC:{docm}",
                resultado="ok",
            )
            await deliver_record(update, ctx, r, drive, logger, authz)
            sessions.reset(u.id)
            return

        if len(found) > config.MAX_RESULTS:
            await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
            logger.log(
                chat_id=chat.id,
                user_id=u.id,
                username=u.username or "",
                rol_detectado=authz.role(u.id),
                accion="buscar_apellidos",
                detalle=f"APELLIDOS:{paterno} {materno} | HIT:{len(found)}",
                resultado="demasiados",
            )
            sessions.reset(u.id)
            return

        logger.log(
            chat_id=chat.id,
            user_id=u.id,
            username=u.username or "",
            rol_detectado=authz.role(u.id),
            accion="buscar_apellidos",
            detalle=f"APELLIDOS:{paterno} {materno} | HIT:{len(found)}",
            resultado="multiple",
        )
        await show_pick_list(update, found, logger, authz, sessions)
        return

    await update.message.reply_text("Usa /busqueda para iniciar.", reply_markup=kb_main())


# -------------------- Bootstrap --------------------


def main() -> None:
    sheets = SheetsRepo(config.GOOGLE_CREDS_JSON_TEXT, config.SHEET_ID)
    drive = DriveRepo(config.GOOGLE_CREDS_JSON_TEXT)
    authz = Authz()
    sessions = SessionManager(config.SESSION_TTL_MIN)
    logger = LoggingRepo(sheets, config.TAB_LOG, config.TZ_NAME)

    # Cache mutable (para /reload_sheet)
    cache: Dict[str, List[Dict]] = {"asegurados": load_caches(sheets, authz)}

    app = Application.builder().token(config.BOT_TOKEN).build()

    # /start /help
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))

    # /id
    async def _id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await only_private_guard(update, logger, authz):
            return
        await cmd_id(update, ctx, logger, authz)

    app.add_handler(CommandHandler("id", _id))

    # /busqueda
    async def _busqueda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_busqueda(update, ctx, authz, sessions, logger)

    app.add_handler(CommandHandler("busqueda", _busqueda))

    # /cancelar
    async def _cancelar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_cancelar(update, ctx, sessions, logger, authz)

    app.add_handler(CommandHandler("cancelar", _cancelar))

    # ✅ /reload_sheet (solo admin/superadmin)
    async def _reload_sheet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await only_private_guard(update, logger, authz):
            return

        u = update.effective_user
        chat = update.effective_chat

        if not authz.is_allowed(u.id):
            await update.effective_message.reply_text(msg.NOT_AUTH_MSG)
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "reload_sheet", "DENEGADO: no_autorizado", "denegado")
            return

        role = authz.role(u.id)
        if role not in ("admin", "superadmin"):
            await update.effective_message.reply_text("❌ Solo administradores pueden recargar datos.")
            logger.log(chat.id, u.id, u.username or "", role,
                       "reload_sheet", "DENEGADO: rol_sin_permiso", "denegado")
            return

        try:
            usuarios = sheets.get_all_records(config.TAB_USUARIOS)
            authz.load(usuarios)
            asegurados = sheets.get_all_records(config.TAB_ASEGURADOS)
            cache["asegurados"] = asegurados

            await update.effective_message.reply_text(
                f"✅ Recargado.\nAsegurados: {len(asegurados)}\nUsuarios: {len(usuarios)}"
            )
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "reload_sheet", f"OK asegurados={len(asegurados)} usuarios={len(usuarios)}", "ok")
        except Exception as e:
            await update.effective_message.reply_text("⚠️ Error recargando datos (revisar permisos/Sheet).")
            logger.log(chat.id, u.id, u.username or "", role,
                       "reload_sheet", f"ERROR:{type(e).__name__}", "error")

    app.add_handler(CommandHandler("reload_sheet", _reload_sheet))

    # callbacks
    async def _cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_callback(update, ctx, cache["asegurados"], drive, authz, sessions, logger)

    app.add_handler(CallbackQueryHandler(_cb))

    # texto (no comandos)
    async def _text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_text(update, ctx, cache["asegurados"], drive, authz, sessions, logger)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _text))

    print("Bot running (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
