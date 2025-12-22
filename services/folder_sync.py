"""
Sincronización de carpetas con Telegram.
OPTIMIZADO: Manejo rápido de errores (Skip Fail-Fast) para chats inaccesibles.
"""
import os
import json
import asyncio
from pyrogram.raw.functions.messages import GetDialogFilters
from pyrogram.errors import (
    PeerIdInvalid, 
    ChannelPrivate, 
    ChannelInvalid, 
    ChannelBanned, 
    FloodWait,
    ChatAdminRequired
)

from config import JSON_FOLDER
from database import db_upsert_chat_from_ci, db_add_chat_folder, get_folder_items_from_db
from utils import obtener_id_limpio, json_serial, force_resolve_peer
from utils.websocket import ws_manager
from .telegram_client import get_client


async def refresh_manual_folder_from_telegram(folder_id: int, name: str) -> None:
    """Refresca el contenido de una carpeta manual desde Telegram y actualiza la BD/local + WS."""
    client = get_client()
    
    try:
        print(f"♻️ Refrescando carpeta manual {folder_id} - {name}...")

        # 1) Localizar la definición de la carpeta en los filtros de diálogos
        try:
            filtros = await client.invoke(GetDialogFilters())
        except Exception as e:
            print(f"⚠️ No se pudo obtener GetDialogFilters: {e}")
            return

        target_folder = None
        for f in filtros or []:
            if hasattr(f, "id") and f.id == folder_id:
                target_folder = f
                break

        if not target_folder:
            print(f"⚠️ Carpeta manual {folder_id} no encontrada en filtros.")
            return

        include_peers = getattr(target_folder, "include_peers", []) or []
        pinned_peers = getattr(target_folder, "pinned_peers", []) or []

        # Combinar include + pinned, evitando duplicados por ID limpio
        peers_dict = {}
        for p in pinned_peers + include_peers:
            pid = obtener_id_limpio(p)
            if pid:
                peers_dict[pid] = p
        peers = list(peers_dict.values())

        print(f"📂 Carpeta {name}: Procesando {len(peers)} chats...")

        # 2) Resolver cada chat y guardarlo en la BD
        processed_count = 0
        
        for raw_peer in peers:
            chat_id = obtener_id_limpio(raw_peer)
            if not chat_id:
                continue

            ci = None
            
            # --- LÓGICA DE RESOLUCIÓN ROBUSTA ---
            try:
                # Intento directo
                ci = await client.get_chat(chat_id)
            
            except (ChannelPrivate, ChannelInvalid, ChannelBanned, ChatAdminRequired):
                # ERRORES PERMANENTES: El usuario no tiene acceso.
                # No reintentamos ni usamos force_resolve. Saltamos inmediatamente.
                print(f"🚫 Acceso denegado a {chat_id} (Private/Banned/Invalid). Saltando.")
                continue
            
            except PeerIdInvalid:
                # Pyrogram no conoce este ID (no está en cache).
                # Intentamos forzar la resolución usando el InputPeer (raw_peer) que tiene el hash.
                try:
                    await force_resolve_peer(client, raw_peer)
                    # Segundo intento tras refrescar cache
                    ci = await client.get_chat(chat_id)
                except (ChannelPrivate, ChannelInvalid, ChannelBanned):
                    print(f"🚫 Acceso denegado a {chat_id} tras resolución. Saltando.")
                    continue
                except Exception as e:
                    print(f"⚠️ Falló resolución de {chat_id}: {e}")
                    continue
            
            except FloodWait as e:
                # Respetar límites de Telegram
                print(f"⏳ FloodWait de {e.value}s... Esperando.")
                await asyncio.sleep(e.value)
                # Un último intento tras la espera
                try:
                    ci = await client.get_chat(chat_id)
                except:
                    continue
            
            except Exception as e:
                print(f"⚠️ Error genérico en chat {chat_id}: {e}")
                continue

            if not ci:
                continue

            # --- SI LLEGAMOS AQUÍ, EL CHAT ES VÁLIDO ---
            
            # Obtener fecha del último mensaje (opcional, falla silenciosamente)
            last_msg_date_str = None
            try:
                # Optimización: Solo buscar si es necesario o usar el top message si ya se tiene
                async for m in client.search_messages(chat_id, limit=1):
                    if getattr(m, "date", None):
                        last_msg_date_str = m.date.isoformat()
                    break
            except Exception:
                pass

            # Guardar en BD
            await db_upsert_chat_from_ci(ci, last_msg_date_str)
            await db_add_chat_folder(chat_id, folder_id)
            
            # --- LOG DE ÉXITO AGREGADO ---
            print(f"🔹 Procesado OK: {chat_id} | {getattr(ci, 'title', 'Sin título')}")
            # -----------------------------
            
            processed_count += 1

        # 3) Regenerar el JSON de la carpeta desde la BD
        items = await get_folder_items_from_db(folder_id, name)
        
        dump_data_folder = {
            "folder_id": folder_id,
            "name": name,
            "items": items,
            "raw": [],
        }
        dump_path = os.path.join(JSON_FOLDER, f"folder_dump_{folder_id}.json")
        with open(dump_path, "w", encoding="utf-8") as f:
            json.dump(dump_data_folder, f, indent=4, default=json_serial, ensure_ascii=False)

        # 4) Avisar a los clientes conectados
        await ws_manager.broadcast_refresh(folder_id)
        print(f"✅ Carpeta manual {folder_id} refrescada: {processed_count}/{len(peers)} chats válidos.")

    except Exception as e:
        print(f"⚠️ Error CRÍTICO en refresh_manual_folder_from_telegram({folder_id}): {e}")