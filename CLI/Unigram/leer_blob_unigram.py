import sqlite3
import struct
import json
from datetime import datetime

DB_PATH = r"C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram\unigram_abierta.sqlite"

def explorar_tablas():
    """Muestra todas las tablas disponibles en la base de datos"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    conn.close()
    return [t[0] for t in tablas]

def ver_estructura_tabla(nombre_tabla):
    """Muestra la estructura de una tabla específica"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({nombre_tabla});")
    columnas = cursor.fetchall()
    conn.close()
    return columnas

def leer_dialogs():
    """Lee la tabla dialogs con el campo BLOB 'data'"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT dialog_id, dialog_order, data, folder_id FROM dialogs LIMIT 10;")
    rows = cursor.fetchall()
    conn.close()
    
    resultados = []
    for dialog_id, dialog_order, blob_data, folder_id in rows:
        info = {
            "dialog_id": dialog_id,
            "dialog_order": dialog_order,
            "folder_id": folder_id,
            "blob_size": len(blob_data) if blob_data else 0,
            "blob_hex_preview": blob_data[:50].hex() if blob_data else None
        }
        resultados.append(info)
    
    return resultados

def decodificar_blob_mensaje(blob):
    """
    Intenta decodificar información del BLOB de un mensaje.
    Basado en el patrón observado en obtenerFechaUltimoMensaje.py
    """
    info = {}
    
    if not blob or len(blob) < 32:
        return {"error": "BLOB muy pequeño"}
    
    try:
        # Posición 28: timestamp (4 bytes, little-endian unsigned int)
        if len(blob) >= 32:
            ts = struct.unpack('<I', blob[28:32])[0]
            if 1356998400 <= ts <= 2000000000:  # Rango válido 2013-2033
                info["timestamp"] = ts
                info["fecha"] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        
        # Primeros bytes (pueden contener IDs u otros datos)
        info["primeros_4_bytes"] = struct.unpack('<I', blob[0:4])[0] if len(blob) >= 4 else None
        info["bytes_4_8"] = struct.unpack('<I', blob[4:8])[0] if len(blob) >= 8 else None
        
        # Vista hexadecimal de los primeros bytes
        info["hex_preview"] = blob[:64].hex()
        
    except Exception as e:
        info["error_decodificacion"] = str(e)
    
    return info

def leer_mensajes(dialog_id=None, limit=5):
    """Lee mensajes de la tabla messages"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if dialog_id:
        query = "SELECT dialog_id, message_id, data FROM messages WHERE dialog_id = ? LIMIT ?"
        cursor.execute(query, (dialog_id, limit))
    else:
        query = "SELECT dialog_id, message_id, data FROM messages LIMIT ?"
        cursor.execute(query, (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    
    resultados = []
    for d_id, m_id, blob_data in rows:
        info = {
            "dialog_id": d_id,
            "message_id": m_id,
            "message_id_decodificado": m_id // 1048576,
            "blob_info": decodificar_blob_mensaje(blob_data)
        }
        resultados.append(info)
    
    return resultados

def obtener_ultimo_mensaje_por_dialog(dialog_id):
    """Obtiene el último mensaje de un dialog específico"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT dialog_id, MAX(message_id), data 
        FROM messages 
        WHERE dialog_id = ?
        GROUP BY dialog_id
    """
    cursor.execute(query, (dialog_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return {"error": "No se encontraron mensajes para este dialog_id"}
    
    d_id, m_id, blob = row
    return {
        "dialog_id": d_id,
        "message_id": m_id,
        "message_id_decodificado": m_id // 1048576,
        "blob_info": decodificar_blob_mensaje(blob)
    }

# --- MENÚ PRINCIPAL ---
if __name__ == "__main__":
    print("=" * 60)
    print("EXPLORADOR DE BASE DE DATOS UNIGRAM")
    print("=" * 60)
    
    # 1. Mostrar tablas disponibles
    print("\n📋 TABLAS DISPONIBLES:")
    tablas = explorar_tablas()
    for i, tabla in enumerate(tablas, 1):
        print(f"  {i}. {tabla}")
    
    # 2. Estructura de la tabla 'dialogs'
    print("\n📊 ESTRUCTURA DE LA TABLA 'dialogs':")
    estructura = ver_estructura_tabla('dialogs')
    for col in estructura:
        print(f"  - {col[1]} ({col[2]})")
    
    # 3. Leer algunos dialogs
    print("\n💬 PRIMEROS 10 DIALOGS:")
    dialogs = leer_dialogs()
    print(json.dumps(dialogs, indent=2, ensure_ascii=False))
    
    # 4. Leer algunos mensajes
    print("\n📨 PRIMEROS 5 MENSAJES:")
    mensajes = leer_mensajes(limit=5)
    print(json.dumps(mensajes, indent=2, ensure_ascii=False))
    
    # 5. Ejemplo: Obtener último mensaje de un dialog específico
    print("\n🔍 ÚLTIMO MENSAJE DEL DIALOG -1003292311333:")
    ultimo = obtener_ultimo_mensaje_por_dialog(-1003292311333)
    print(json.dumps(ultimo, indent=2, ensure_ascii=False))
