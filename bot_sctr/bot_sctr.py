import asyncio
from typing import List, Dict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

from . import config
import messages as msg
from . import config
from . import messages as msg
from .sheets_repo import SheetsRepo
from .drive_repo import DriveRepo
from .authz import Authz
from .session import SessionManager
from .logging_repo import LoggingRepo
from .search import clean_digits, mask_doc, build_ficha, find_by_doc, find_by_apellidos

)

# Callback data constants
CB_DOC = "M_DOC"
CB_AP = "M_AP"
CB_MENU = "M_MENU"
CB_CANCEL = "M_CANCEL"
CB_PICK_PREFIX = "PICK_"  # PICK_0, PICK_1...

def is_private(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.type == ChatType.PRIVATE

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("BÚSQUEDA POR DOCUMENTO", callback_data=CB_DOC)],
        [InlineKeyboardButton("BÚSQUEDA POR APELLIDOS", callback_data=CB_AP)],
        [InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)],
    ])

def kb_back_cancel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("↩️ Menú", callback_data=CB_MENU)],
        [InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)],
    ])

def kb_pick(n: int):
    rows = []
    row = []
    for i in range(n):
        row.append(InlineKeyboardButton(str(i+1), callback_data=f"{CB_PICK_PREFIX}{i}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("↩️ Menú", callback_data=CB_MENU)])
    rows.append([InlineKeyboardButton("❌ Cancelar", callback_data=CB_CANCEL)])
    return InlineKeyboardMarkup(rows)

async def reload_caches(sheets: SheetsRepo, authz: Authz) -> List[Dict]:
    usuarios = sheets.get_all_records(config.TAB_USUARIOS)
    authz.load(usuarios)
    asegurados = sheets.get_all_records(config.TAB_ASEGURADOS)
    return asegurados

async def only_private_guard(update: Update, ctx: ContextTypes.DEFAULT_TYPE, logger: LoggingRepo, authz: Authz) -> bool:
    if is_private(update):
        return True
    # group/other: respond with private-only message
    user = update.effective_user
    chat = update.effective_chat
    uid = user.id if user else 0
    role = authz.role(uid)
    await update.effective_message.reply_text(msg.PRIVATE_ONLY_MSG)
    logger.log(chat.id, uid, user.username if user else "", role, "intento_en_grupo",
               f"CMD:{(update.effective_message.text or '').strip()[:30]}", "denegado")
    return False

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(msg.START_MSG)

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(msg.HELP_MSG)

async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE, logger: LoggingRepo, authz: Authz):
    if not is_private(update):
        # handled by guard elsewhere usually
        return
    u = update.effective_user
    await update.message.reply_text(f"Tu ID es: `{u.id}`", parse_mode="Markdown")
    logger.log(update.effective_chat.id, u.id, u.username or "", authz.role(u.id), "cmd_id", "MOSTRADO", "ok")

async def cmd_busqueda(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sheets: SheetsRepo, authz: Authz,
                      sessions: SessionManager, logger: LoggingRepo):
    if not await only_private_guard(update, ctx, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat

    if not authz.is_allowed(u.id):
        await update.message.reply_text(msg.NOT_AUTH_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "intento_no_autorizado", "CMD:/busqueda", "denegado")
        return

    # Reset and show menu
    sessions.reset(u.id)
    s = sessions.get(u.id)
    s.state = "CHOOSE_METHOD"
    sessions.touch(u.id)

    await update.message.reply_text(msg.ASK_METHOD_MSG, reply_markup=kb_main())
    logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
               "cmd_busqueda", "INICIO", "ok")

async def cmd_cancelar(update: Update, ctx: ContextTypes.DEFAULT_TYPE, sessions: SessionManager,
                       logger: LoggingRepo, authz: Authz):
    if not is_private(update):
        return
    u = update.effective_user
    chat = update.effective_chat
    st = sessions.get(u.id).state
    sessions.reset(u.id)
    await update.message.reply_text(msg.CANCELLED_MSG)
    logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
               "cancelar", f"CANCELADO_EN:{st}", "ok")

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, asegurados_cache: List[Dict],
                      sheets: SheetsRepo, drive: DriveRepo, authz: Authz, sessions: SessionManager,
                      logger: LoggingRepo):
    q = update.callback_query
    await q.answer()

    if not await only_private_guard(update, ctx, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat

    if not authz.is_allowed(u.id):
        await q.edit_message_text(msg.NOT_AUTH_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "intento_no_autorizado", "BTN", "denegado")
        return

    # Expiration check
    if sessions.is_expired(u.id):
        sessions.reset(u.id)
        await q.edit_message_text(msg.EXPIRED_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "expirado", "EXPIRA", "expirado")
        return

    s = sessions.get(u.id)
    sessions.touch(u.id)

    data = q.data or ""

    if data == CB_MENU:
        s.state = "CHOOSE_METHOD"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_METHOD_MSG, reply_markup=kb_main())
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "menu_busqueda", "MOSTRADO", "ok")
        return

    if data == CB_CANCEL:
        st = s.state
        sessions.reset(u.id)
        await q.edit_message_text(msg.CANCELLED_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "cancelar", f"CANCELADO_EN:{st}", "ok")
        return

    if data == CB_DOC:
        s.state = "WAIT_DOC"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_DOC_MSG, reply_markup=kb_back_cancel())
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "metodo_seleccionado", "METODO:DOCUMENTO", "ok")
        return

    if data == CB_AP:
        s.state = "WAIT_AP_PATERNO"
        s.ctx.clear()
        await q.edit_message_text(msg.ASK_PAT_MSG, reply_markup=kb_back_cancel())
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "metodo_seleccionado", "METODO:APELLIDOS", "ok")
        return

    if data.startswith(CB_PICK_PREFIX):
        if s.state != "WAIT_PICK":
            await q.edit_message_text(msg.EXPIRED_MSG)
            return
        try:
            idx = int(data.replace(CB_PICK_PREFIX, ""))
        except:
            idx = -1

        results: List[Dict] = s.ctx.get("pick_results", [])
        if idx < 0 or idx >= len(results):
            await q.edit_message_text("⚠️ Opción inválida. Vuelve a intentar.", reply_markup=kb_main())
            return

        r = results[idx]
        docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
        nombre = str(r.get("apellidos_y_nombres", "")).strip()
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "seleccion_opcion", f"SEL:{idx+1} | NOMBRE:{nombre} | DOC:{docm}", "ok")

        # Deliver: ficha then PDF
        await deliver_record(update, ctx, r, drive, logger, authz)
        sessions.reset(u.id)
        return

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE, asegurados_cache: List[Dict],
                  drive: DriveRepo, authz: Authz, sessions: SessionManager,
                  logger: LoggingRepo):
    if not await only_private_guard(update, ctx, logger, authz):
        return

    u = update.effective_user
    chat = update.effective_chat
    text = (update.message.text or "").strip()

    if not authz.is_allowed(u.id):
        await update.message.reply_text(msg.NOT_AUTH_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "intento_no_autorizado", "TXT", "denegado")
        return

    # expiration
    if sessions.is_expired(u.id):
        sessions.reset(u.id)
        await update.message.reply_text(msg.EXPIRED_MSG)
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "expirado", "EXPIRA", "expirado")
        return

    s = sessions.get(u.id)
    sessions.touch(u.id)

    if s.state == "WAIT_DOC":
        digits = clean_digits(text)
        if len(digits) not in (8, 9):
            await update.message.reply_text("⚠️ Documento inválido. Ingresa DNI (8) o CE (9).", reply_markup=kb_back_cancel())
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "input_documento", f"DOC_INVALIDO:len={len(digits)}", "error_formato")
            return

        docm = mask_doc(digits)
        found = find_by_doc(asegurados_cache, digits)
        if not found:
            await update.message.reply_text(msg.NO_FOUND_DOC, reply_markup=kb_main())
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "buscar_doc", f"HIT:0 DOC:{docm}", "no_encontrado")
            sessions.reset(u.id)
            return

        # If more than 1 (shouldn't), show pick
        if len(found) > 1:
            if len(found) > config.MAX_RESULTS:
                await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
                logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                           "buscar_doc", f"HIT:{len(found)} DOC:{docm}", "demasiados")
                sessions.reset(u.id)
                return
            await show_pick_list(update, found, logger, authz, sessions)
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "buscar_doc", f"HIT:{len(found)} DOC:{docm}", "multiple")
            return

        r = found[0]
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "buscar_doc", f"HIT:1 DOC:{docm}", "ok")
        await deliver_record(update, ctx, r, drive, logger, authz)
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
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "buscar_apellidos", f"APELLIDOS:{paterno} {materno} | HIT:0", "no_encontrado")
            sessions.reset(u.id)
            return

        if len(found) == 1:
            r = found[0]
            docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "buscar_apellidos", f"APELLIDOS:{paterno} {materno} | HIT:1 | DOC:{docm}", "ok")
            await deliver_record(update, ctx, r, drive, logger, authz)
            sessions.reset(u.id)
            return

        if len(found) > config.MAX_RESULTS:
            await update.message.reply_text(msg.TOO_MANY, reply_markup=kb_main())
            logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                       "buscar_apellidos", f"APELLIDOS:{paterno} {materno} | HIT:{len(found)}", "demasiados")
            sessions.reset(u.id)
            return

        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "buscar_apellidos", f"APELLIDOS:{paterno} {materno} | HIT:{len(found)}", "multiple")
        await show_pick_list(update, found, logger, authz, sessions)
        return

    # Default (not in a flow)
    await update.message.reply_text("Usa /busqueda para iniciar.", reply_markup=kb_main())

async def show_pick_list(update: Update, results: List[Dict], logger: LoggingRepo, authz: Authz, sessions: SessionManager):
    u = update.effective_user
    chat = update.effective_chat

    # Save results in session
    s = sessions.get(u.id)
    s.state = "WAIT_PICK"
    s.ctx["pick_results"] = results

    lines = ["Se encontraron varias coincidencias. Elige la persona correcta:"]
    for i, r in enumerate(results, start=1):
        nombre = str(r.get("apellidos_y_nombres", "")).strip()
        docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
        lines.append(f"{i}) {nombre} — DOC: {docm}")

    await update.message.reply_text("\n".join(lines), reply_markup=kb_pick(len(results)))
    logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
               "lista_resultados", f"LISTA:{len(results)}", "multiple")

async def deliver_record(update: Update, ctx: ContextTypes.DEFAULT_TYPE, r: Dict,
                         drive: DriveRepo, logger: LoggingRepo, authz: Authz):
    u = update.effective_user
    chat = update.effective_chat

    # Ficha
    ficha = build_ficha(r, config.TZ_NAME)
    docm = mask_doc(str(r.get("doc_norm", r.get("nro_doc", ""))))
    nombre = str(r.get("apellidos_y_nombres", "")).strip()

    await update.effective_message.reply_text(ficha)
    logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
               "respuesta_ficha", f"NOMBRE:{nombre} | DOC:{docm}", "ok")

    # PDF
    archivo_origen = str(r.get("archivo_origen", "")).strip()
    file_id = str(r.get("file_id_drive", "")).strip()

    if not file_id:
        await update.effective_message.reply_text("📎 PDF no disponible (falta file_id).")
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "envio_pdf", f"PDF_FAIL | motivo:file_id_vacio | ARCHIVO:{archivo_origen}", "fallo_pdf",
                   archivo_origen=archivo_origen)
        return

    try:
        content, name = drive.download_file(file_id)
        # send as document
        await ctx.bot.send_document(
            chat_id=chat.id,
            document=content,
            filename=archivo_origen or name,
            caption=archivo_origen or name
        )
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "envio_pdf", f"PDF_OK | ARCHIVO:{archivo_origen or name}", "ok",
                   archivo_origen=archivo_origen or name,
                   file_id_drive=file_id[:8]  # opcional: solo primeros 8
                   )
    except Exception as e:
        await update.effective_message.reply_text("📎 No pude adjuntar el PDF (revisar permisos/archivo).")
        logger.log(chat.id, u.id, u.username or "", authz.role(u.id),
                   "envio_pdf", f"PDF_FAIL | motivo:{type(e).__name__} | ARCHIVO:{archivo_origen}", "fallo_pdf",
                   archivo_origen=archivo_origen,
                   file_id_drive=file_id[:8])

async def main():
    sheets = SheetsRepo(config.GOOGLE_CREDS_JSON_TEXT, config.SHEET_ID)
    drive = DriveRepo(config.GOOGLE_CREDS_JSON_TEXT)
    authz = Authz()
    sessions = SessionManager(config.SESSION_TTL_MIN)
    logger = LoggingRepo(sheets, config.TAB_LOG, config.TZ_NAME)

    # Load caches
    asegurados_cache = await reload_caches(sheets, authz)

    app = Application.builder().token(config.BOT_TOKEN).build()

    # Basic commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))

    # /id
    async def _id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await only_private_guard(update, ctx, logger, authz):
            return
        await cmd_id(update, ctx, logger, authz)
    app.add_handler(CommandHandler("id", _id))

    # /busqueda
    async def _busqueda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_busqueda(update, ctx, sheets, authz, sessions, logger)
    app.add_handler(CommandHandler("busqueda", _busqueda))

    # /cancelar
    async def _cancelar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await cmd_cancelar(update, ctx, sessions, logger, authz)
    app.add_handler(CommandHandler("cancelar", _cancelar))

    # Callbacks
    async def _cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_callback(update, ctx, asegurados_cache, sheets, drive, authz, sessions, logger)
    app.add_handler(CallbackQueryHandler(_cb))

    # Text handler
    async def _text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await on_text(update, ctx, asegurados_cache, drive, authz, sessions, logger)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _text))

    # (Opcional) refresco simple cada X minutos: aquí no lo hago automático para mantener skeleton simple.

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    print("Bot running (polling)...")
    await app.updater.idle()

if __name__ == "__main__":

    asyncio.run(main())

