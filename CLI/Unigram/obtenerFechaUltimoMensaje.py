import os
import struct
import json
from datetime import datetime
from sqlcipher3 import dbapi2 as sqlite

# --- PARÁMETROS ---
MASTER_KEY = "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63"
DB_PATH = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\db.sqlite"

def obtener_fechas_y_ids(canales=None):
    """
    Retorna { dialog_id: { "fecha": "...", "id_mensaje": ... } }
    """
    resultado = {}
    
    if not os.path.exists(DB_PATH):
        return {"error": "DB no encontrada"}

    try:
        conn = sqlite.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
        cursor.execute("PRAGMA cipher_compatibility = 4;")

        # 1. Construcción de la consulta
        # Buscamos el último mensaje (MAX message_id) para obtener la fecha más reciente
        query = """
            SELECT dialog_id, MAX(message_id), data 
            FROM messages 
        """
        params = []

        if isinstance(canales, (int, str)):
            query += " WHERE dialog_id = ?"
            params.append(canales)
        elif isinstance(canales, list):
            placeholders = ','.join(['?'] * len(canales))
            query += f" WHERE dialog_id IN ({placeholders})"
            params.extend(canales)
        
        query += " GROUP BY dialog_id"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        for d_id, m_id, blob in rows:
            # A) Decodificar ID del mensaje
            id_decodificado = m_id // 1048576
            
            # B) Extraer Fecha (Posición 28 del BLOB)
            fecha_humana = "N/A"
            if blob and len(blob) >= 32:
                try:
                    ts = struct.unpack('<I', blob[28:32])[0]
                    # Solo convertimos si parece un timestamp válido (2013-2033)
                    if 1356998400 <= ts <= 2000000000:
                        fecha_humana = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                except: pass

            # Estructura final solicitada
            resultado[d_id] = {
                "fecha": fecha_humana,
                "id_mensaje_decodificado": id_decodificado
            }

        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")
        
    return resultado

# --- PRUEBA DE EJECUCIÓN ---
if __name__ == "__main__":
    # Ejemplo con tu canal específico
    canal_test = [-1001516713920,-1001081759083,-1001487754982]
    data = obtener_fechas_y_ids(canales=canal_test)
    
    print(json.dumps(data, indent=4))