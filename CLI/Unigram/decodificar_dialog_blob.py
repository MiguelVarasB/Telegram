import sqlite3
import struct
import json
from datetime import datetime

DB_PATH = r"C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram\unigram_abierta.sqlite"
DIALOG_ID = -1001152304240
def decodificar_blob_dialog(blob):
    """
    Decodifica el BLOB de la tabla dialogs a formato legible.
    Basado en el formato TL (Type Language) de Telegram.
    """
    if not blob:
        return {"error": "BLOB vacío"}
    
    resultado = {
        "tamaño_total": len(blob),
        "hex_completo": blob.hex(),
        "datos_decodificados": {}
    }
    
    try:
        offset = 0
        datos = resultado["datos_decodificados"]
        
        # Byte 0-3: Constructor ID o tamaño
        if len(blob) >= 4:
            datos["constructor_id"] = struct.unpack('<I', blob[0:4])[0]
            datos["constructor_id_hex"] = blob[0:4].hex()
            offset = 4
        
        # Byte 4-7: Flags o tipo
        if len(blob) >= 8:
            datos["flags"] = struct.unpack('<I', blob[4:8])[0]
            datos["flags_hex"] = blob[4:8].hex()
            datos["flags_binario"] = bin(datos["flags"])[2:].zfill(32)
            offset = 8
        
        # Byte 8-15: Peer (dialog_id en formato TL)
        if len(blob) >= 16:
            peer_type = struct.unpack('<I', blob[8:12])[0]
            peer_id = struct.unpack('<i', blob[12:16])[0]  # signed int
            datos["peer"] = {
                "type_id": peer_type,
                "type_hex": blob[8:12].hex(),
                "id": peer_id,
                "id_hex": blob[12:16].hex()
            }
            offset = 16
        
        # Byte 16-19: Top message ID
        if len(blob) >= 20:
            top_msg = struct.unpack('<I', blob[16:20])[0]
            datos["top_message_id"] = top_msg
            datos["top_message_id_decodificado"] = top_msg // 1048576 if top_msg > 0 else 0
            offset = 20
        
        # Byte 20-23: Read inbox max ID
        if len(blob) >= 24:
            read_inbox = struct.unpack('<I', blob[20:24])[0]
            datos["read_inbox_max_id"] = read_inbox
            datos["read_inbox_decodificado"] = read_inbox // 1048576 if read_inbox > 0 else 0
            offset = 24
        
        # Byte 24-27: Read outbox max ID
        if len(blob) >= 28:
            read_outbox = struct.unpack('<I', blob[24:28])[0]
            datos["read_outbox_max_id"] = read_outbox
            datos["read_outbox_decodificado"] = read_outbox // 1048576 if read_outbox > 0 else 0
            offset = 28
        
        # Byte 28-31: Unread count
        if len(blob) >= 32:
            unread = struct.unpack('<I', blob[28:32])[0]
            datos["unread_count"] = unread
            offset = 32
        
        # Byte 32-35: Unread mentions count
        if len(blob) >= 36:
            unread_mentions = struct.unpack('<I', blob[32:36])[0]
            datos["unread_mentions_count"] = unread_mentions
            offset = 36
        
        # Byte 36-39: Unread reactions count
        if len(blob) >= 40:
            unread_reactions = struct.unpack('<I', blob[36:40])[0]
            datos["unread_reactions_count"] = unread_reactions
            offset = 40
        
        # Byte 40-43: Notify settings (puede ser un constructor)
        if len(blob) >= 44:
            notify_settings = struct.unpack('<I', blob[40:44])[0]
            datos["notify_settings"] = {
                "value": notify_settings,
                "hex": blob[40:44].hex()
            }
            offset = 44
        
        # Byte 44-47: PTS (para canales)
        if len(blob) >= 48:
            pts = struct.unpack('<I', blob[44:48])[0]
            datos["pts"] = pts
            offset = 48
        
        # Byte 48-51: Draft (puede contener un constructor)
        if len(blob) >= 52:
            draft = struct.unpack('<I', blob[48:52])[0]
            datos["draft"] = {
                "value": draft,
                "hex": blob[48:52].hex()
            }
            offset = 52
        
        # Byte 52-55: Folder ID
        if len(blob) >= 56:
            folder = struct.unpack('<i', blob[52:56])[0]
            datos["folder_id_blob"] = folder
            offset = 56
        
        # Intentar buscar timestamps en el resto del BLOB
        datos["posibles_timestamps"] = []
        for i in range(offset, len(blob) - 3, 4):
            try:
                ts = struct.unpack('<I', blob[i:i+4])[0]
                # Rango válido: 2013-01-01 hasta 2026-12-31
                if 1356998400 <= ts <= 1798761600:
                    fecha = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    datos["posibles_timestamps"].append({
                        "offset": i,
                        "timestamp": ts,
                        "fecha": fecha
                    })
            except:
                pass
        
        # Ordenar por timestamp de mayor a menor (más reciente primero)
        datos["posibles_timestamps"].sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Buscar strings (secuencias de caracteres imprimibles)
        datos["strings_encontrados"] = []
        current_string = ""
        for i in range(offset, len(blob)):
            byte = blob[i]
            if 32 <= byte <= 126:  # Caracteres ASCII imprimibles
                current_string += chr(byte)
            else:
                if len(current_string) >= 4:
                    datos["strings_encontrados"].append({
                        "offset": i - len(current_string),
                        "texto": current_string
                    })
                current_string = ""
        
        if len(current_string) >= 4:
            datos["strings_encontrados"].append({
                "offset": len(blob) - len(current_string),
                "texto": current_string
            })
        
    except Exception as e:
        resultado["error_decodificacion"] = str(e)
    
    return resultado

def decodificar_blob_mensaje(blob):
    """Decodifica el BLOB de un mensaje de la tabla messages"""
    if not blob:
        return {"error": "BLOB vacío"}
    
    resultado = {
        "tamaño_total": len(blob),
        "hex_preview": blob[:128].hex(),
        "datos_decodificados": {}
    }
    
    try:
        datos = resultado["datos_decodificados"]
        offset = 0
        
        # Byte 0-3: Constructor ID
        if len(blob) >= 4:
            datos["constructor_id"] = struct.unpack('<I', blob[0:4])[0]
            datos["constructor_id_hex"] = blob[0:4].hex()
            offset = 4
        
        # Byte 4-7: Flags
        if len(blob) >= 8:
            datos["flags"] = struct.unpack('<I', blob[4:8])[0]
            datos["flags_hex"] = blob[4:8].hex()
            datos["flags_binario"] = bin(datos["flags"])[2:].zfill(32)
            offset = 8
        
        # Byte 8-11: Message ID interno
        if len(blob) >= 12:
            datos["message_id_interno"] = struct.unpack('<I', blob[8:12])[0]
            offset = 12
        
        # Byte 12-15: From ID (puede ser peer)
        if len(blob) >= 16:
            datos["from_id"] = struct.unpack('<I', blob[12:16])[0]
            offset = 16
        
        # Byte 16-19: Peer ID
        if len(blob) >= 20:
            datos["peer_id"] = struct.unpack('<I', blob[16:20])[0]
            offset = 20
        
        # Byte 20-27: Forward info (opcional)
        if len(blob) >= 28:
            datos["fwd_from"] = struct.unpack('<Q', blob[20:28])[0]
            offset = 28
        
        # Byte 28-31: Date (timestamp)
        if len(blob) >= 32:
            ts = struct.unpack('<I', blob[28:32])[0]
            # Rango válido: 2013-01-01 hasta 2026-12-31
            if 1356998400 <= ts <= 1798761600:
                datos["timestamp"] = ts
                datos["fecha"] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            offset = 32
        
        # Buscar más timestamps en el resto del BLOB
        datos["otros_timestamps"] = []
        for i in range(offset, len(blob) - 3, 4):
            try:
                ts = struct.unpack('<I', blob[i:i+4])[0]
                # Rango válido: 2013-01-01 hasta 2026-12-31
                if 1356998400 <= ts <= 1798761600:
                    fecha = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    datos["otros_timestamps"].append({
                        "offset": i,
                        "timestamp": ts,
                        "fecha": fecha
                    })
            except:
                pass
        
        # Ordenar timestamps de mayor a menor
        datos["otros_timestamps"].sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Buscar strings
        datos["strings_encontrados"] = []
        current_string = ""
        for i in range(offset, len(blob)):
            byte = blob[i]
            if 32 <= byte <= 126:
                current_string += chr(byte)
            else:
                if len(current_string) >= 4:
                    datos["strings_encontrados"].append({
                        "offset": i - len(current_string),
                        "texto": current_string
                    })
                current_string = ""
        
        if len(current_string) >= 4:
            datos["strings_encontrados"].append({
                "offset": len(blob) - len(current_string),
                "texto": current_string
            })
        
    except Exception as e:
        resultado["error_decodificacion"] = str(e)
    
    return resultado

def obtener_dialog_blob(dialog_id):
    """Obtiene el BLOB de un dialog específico"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT dialog_id, dialog_order, data, folder_id FROM dialogs WHERE dialog_id = ?", (dialog_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return {"error": f"No se encontró el dialog_id {dialog_id}"}
    
    d_id, d_order, blob, folder = row
    
    return {
        "dialog_id": d_id,
        "dialog_order": d_order,
        "folder_id": folder,
        "blob_decodificado": decodificar_blob_dialog(blob)
    }

def obtener_ultimo_mensaje(dialog_id):
    """Obtiene el último mensaje de un dialog específico"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT message_id, data FROM messages WHERE dialog_id = ? ORDER BY message_id DESC LIMIT 1",
        (dialog_id,)
    )
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return {"error": f"No se encontraron mensajes para dialog_id {dialog_id}"}
    
    msg_id, blob = row
    
    return {
        "message_id": msg_id,
        "message_id_decodificado": msg_id // 1048576,
        "blob_decodificado": decodificar_blob_mensaje(blob)
    }

if __name__ == "__main__":
    print("=" * 80)
    print("DECODIFICADOR DE BLOB - TABLA DIALOGS Y MESSAGES")
    print("=" * 80)
    
    dialog_id = DIALOG_ID
    print(f"\n🔍 Decodificando dialog_id: {dialog_id}\n")
    
    # ===== DIALOG =====
    print("\n" + "=" * 80)
    print("📂 TABLA DIALOGS")
    print("=" * 80)
    
    resultado_dialog = obtener_dialog_blob(dialog_id)
    print(json.dumps(resultado_dialog, indent=2, ensure_ascii=False))
    
    # Resumen legible del dialog
    if "blob_decodificado" in resultado_dialog and "datos_decodificados" in resultado_dialog["blob_decodificado"]:
        print("\n" + "=" * 80)
        print("📋 RESUMEN LEGIBLE - DIALOG")
        print("=" * 80)
        datos = resultado_dialog["blob_decodificado"]["datos_decodificados"]
        
        print(f"\n📊 Información del Dialog:")
        print(f"  • Dialog ID: {resultado_dialog['dialog_id']}")
        print(f"  • Folder ID: {resultado_dialog['folder_id']}")
        print(f"  • Dialog Order: {resultado_dialog['dialog_order']}")
        
        if "top_message_id_decodificado" in datos:
            print(f"\n💬 Mensajes:")
            print(f"  • Último mensaje ID: {datos.get('top_message_id_decodificado', 'N/A')}")
            print(f"  • Leídos (inbox): {datos.get('read_inbox_decodificado', 'N/A')}")
            print(f"  • Leídos (outbox): {datos.get('read_outbox_decodificado', 'N/A')}")
        
        if "unread_count" in datos:
            print(f"\n🔔 Notificaciones:")
            print(f"  • Mensajes no leídos: {datos.get('unread_count', 0)}")
            print(f"  • Menciones no leídas: {datos.get('unread_mentions_count', 0)}")
            print(f"  • Reacciones no leídas: {datos.get('unread_reactions_count', 0)}")
        
        if datos.get("posibles_timestamps"):
            print(f"\n📅 Fechas encontradas:")
            for ts_info in datos["posibles_timestamps"]:
                print(f"  • {ts_info['fecha']} (offset: {ts_info['offset']})")
        
        if datos.get("strings_encontrados"):
            print(f"\n📝 Textos encontrados:")
            for str_info in datos["strings_encontrados"]:
                print(f"  • '{str_info['texto']}' (offset: {str_info['offset']})")
    
    # ===== ÚLTIMO MENSAJE =====
    print("\n\n" + "=" * 80)
    print("💬 TABLA MESSAGES - ÚLTIMO MENSAJE")
    print("=" * 80)
    
    resultado_mensaje = obtener_ultimo_mensaje(dialog_id)
    print(json.dumps(resultado_mensaje, indent=2, ensure_ascii=False))
    
    # Resumen legible del mensaje
    if "blob_decodificado" in resultado_mensaje and "datos_decodificados" in resultado_mensaje["blob_decodificado"]:
        print("\n" + "=" * 80)
        print("📋 RESUMEN LEGIBLE - MENSAJE")
        print("=" * 80)
        datos_msg = resultado_mensaje["blob_decodificado"]["datos_decodificados"]
        
        print(f"\n📨 Información del Mensaje:")
        print(f"  • Message ID: {resultado_mensaje['message_id']}")
        print(f"  • Message ID decodificado: {resultado_mensaje['message_id_decodificado']}")
        
        if "fecha" in datos_msg:
            print(f"  • Fecha principal: {datos_msg['fecha']}")
        
        if datos_msg.get("otros_timestamps"):
            print(f"\n📅 Otras fechas encontradas:")
            for ts_info in datos_msg["otros_timestamps"]:
                print(f"  • {ts_info['fecha']} (offset: {ts_info['offset']})")
        
        if datos_msg.get("strings_encontrados"):
            print(f"\n📝 Textos encontrados:")
            for str_info in datos_msg["strings_encontrados"]:
                print(f"  • '{str_info['texto']}' (offset: {str_info['offset']})")
