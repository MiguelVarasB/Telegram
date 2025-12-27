import os
import asyncio
import aiosqlite
import sys
from pyrogram import Client, idle
from pyrogram.errors import FloodWait, PeerIdInvalid, MessageIdInvalid

from pyrogram.raw.functions.messages import GetDialogs
from pyrogram.raw.types import (
    InputPeerEmpty,
    InputPeerUser,
    InputPeerChat,
    InputPeerChannel,
    PeerUser,
    PeerChat,
    PeerChannel,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importamos tu configuración
from config import (
    DB_PATH,
    API_ID2,
    API_HASH2,
    SESSION_NAME2,
    CANALES_CON_ACCESO_FREE,
    CACHE_DUMP_VIDEOS_CHANNEL_ID,
    FOLDER_SESSIONS,
)

# --- CONFIGURACIÓN ---
LIMITE = 2000 
BATCH = 30  
SLEEP_ENVIO = 20 # Subido un poco para ser más orgánico
ESPERA_CICLOS = 180

async def check_database_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
            columns = await cursor.fetchall()
            col_names = [col[1] for col in columns]
            if "dump_fail" not in col_names:
                await db.execute("ALTER TABLE videos_telegram ADD COLUMN dump_fail INTEGER DEFAULT 0")
                await db.commit()

async def marcar_fallidos(video_ids, razon):
    if not video_ids: return
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join(["?"] * len(video_ids))
        await db.execute(
            f"UPDATE videos_telegram SET dump_fail = 1 WHERE id IN ({placeholders})",
            tuple(video_ids),
        )
        await db.commit()
    print(f"\n🗑️ Marcados {len(video_ids)} registros como dump_fail=1. Razón: {razon}")

async def marcar_chat_fallido(chat_id, razon):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE videos_telegram SET dump_fail = 1 WHERE chat_id = ? AND has_thumb = 0 AND dump_message_id IS NULL",
            (chat_id,),
        )
        await db.commit()
    print(f"\n🗑️ Marcado chat {chat_id} como NO reenviable. Razón: {razon}")

# ... (Las funciones _raw_next_offset_peer y _warmup_dialogs_for_peer se mantienen igual) ...
# Para ahorrar espacio, asumo que las copias del código original o las importas.
# Si las necesitas completas aquí, dímelo, pero la lógica clave cambia abajo.

async def _ensure_peer(app: Client, chat_id: int) -> bool:
    try:
        await app.get_chat(chat_id)
        return True
    except Exception:
        return False

# --- MODIFICACIÓN PRINCIPAL: main AHORA RECIBE 'app' ---
async def main(app: Client):
    # Ya no iniciamos el cliente aquí, asumimos que viene conectado.
    
    if not CACHE_DUMP_VIDEOS_CHANNEL_ID or not CANALES_CON_ACCESO_FREE:
        print("❌ ERROR: Configuración incompleta en config.py")
        return

    print(f"🚀 Buscando hasta {LIMITE} videos pendientes en DB...")

    # 1. Buscar videos pendientes
    placeholders_chats = ",".join(["?"] * len(CANALES_CON_ACCESO_FREE))
    query = f"""
        SELECT id, chat_id, message_id
        FROM videos_telegram
        WHERE has_thumb = 0 AND dump_message_id IS NULL
          AND (dump_fail IS NULL OR dump_fail = 0)
          AND chat_id IN ({placeholders_chats})
        LIMIT ?
    """
    params = tuple(CANALES_CON_ACCESO_FREE) + (LIMITE,)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cursor:
            pendientes = await cursor.fetchall()

    if not pendientes:
        print("✅ No hay videos pendientes por ahora.")
        return

    print(f"📦 Encontrados {len(pendientes)} videos. Procesando...")

    # 2. Validar destino una vez por ciclo
    try:
        await app.get_chat(CACHE_DUMP_VIDEOS_CHANNEL_ID)
    except Exception as e:
        print(f"❌ Error accediendo al canal DUMP: {e}")
        return

    # 3. Agrupar y Reenviar
    lotes = {}
    for vid in pendientes:
        lotes.setdefault(vid[1], []).append(vid)

    total_ok = 0

    for chat_origin, videos in lotes.items():
        # Validar que el peer exista/esté resolvible antes de enviar
        if not await _ensure_peer(app, chat_origin):
            await marcar_chat_fallido(chat_origin, "PEER_NOT_FOUND")
            continue

        # Procesar por lotes
        for i in range(0, len(videos), BATCH):
            chunk = videos[i : i + BATCH]
            msg_ids = [v[2] for v in chunk]

            try:
                print(f"   ➡️ Reenviando {len(chunk)} videos del chat {chat_origin}...", end=" ")
                
                nuevos_msgs = await app.forward_messages(
                    chat_id=CACHE_DUMP_VIDEOS_CHANNEL_ID,
                    from_chat_id=chat_origin,
                    message_ids=msg_ids
                )
                
                if not isinstance(nuevos_msgs, list): nuevos_msgs = [nuevos_msgs]

                async with aiosqlite.connect(DB_PATH) as db:
                    for j, msg in enumerate(nuevos_msgs):
                        if msg:
                            await db.execute(
                                "UPDATE videos_telegram SET dump_message_id = ? WHERE id = ?",
                                (msg.id, chunk[j][0]),
                            )
                    await db.commit()

                total_ok += len(nuevos_msgs)
                print("✅")
                await asyncio.sleep(SLEEP_ENVIO)

            except FloodWait as e:
                print(f"\n⏳ FloodWait real de Telegram: Esperando {e.value} segundos...")
                await asyncio.sleep(e.value) # Respetamos el wait sin cerrar el cliente
            except PeerIdInvalid:
                await marcar_chat_fallido(chat_origin, "PEER_ID_INVALID")
                break
            except MessageIdInvalid:
                await marcar_fallidos([v[0] for v in chunk], "MESSAGE_ID_INVALID")
                print("⚠️ Lote omitido por mensajes inválidos")
                continue
            except ValueError as e:
                if "Peer id invalid" in str(e):
                    await marcar_chat_fallido(chat_origin, "PEER_ID_INVALID")
                    break
                raise
            except Exception as e:
                if "CHAT_FORWARDS_RESTRICTED" in str(e):
                    await marcar_chat_fallido(chat_origin, "RESTRICTED")
                    break # Saltamos al siguiente chat
                print(f"\n❌ Error enviando lote: {e}")

    print(f"🏁 Ciclo terminado. Total reenviados: {total_ok}")


# --- NUEVO LOOP DE CONTROL ---
if __name__ == "__main__":
    # Inicializamos el cliente UNA SOLA VEZ fuera del bucle
    os.makedirs(FOLDER_SESSIONS, exist_ok=True)
    session_path = os.path.join(FOLDER_SESSIONS, SESSION_NAME2)
    
    app = Client(session_path, api_id=API_ID2, api_hash=API_HASH2)

    async def run_ciclico():
        # Usamos el context manager para iniciar/cerrar la sesión correctamente
        async with app:
            me = await app.get_me()
            print(f"👤 Cliente iniciado: {me.first_name} (@{me.username})")
            await check_database_schema()

            ciclo = 0
            while True:
                ciclo += 1
                try:
                    print(f"\n--- CICLO #{ciclo} ---")
                    # Pasamos 'app' que ya está conectada
                    await main(app) 
                except Exception as e:
                    print(f"❌ Error crítico en ciclo: {e}")
                
                print(f"⏳ Durmiendo {ESPERA_CICLOS}s...")
                await asyncio.sleep(ESPERA_CICLOS)

    try:
        asyncio.run(run_ciclico())
    except KeyboardInterrupt:
        print("\n🛑 Detenido por usuario.")