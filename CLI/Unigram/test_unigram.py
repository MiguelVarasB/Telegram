import sqlite3
import re
import pandas as pd

# CONFIGURACIÓN
DB_PATH = "unigram_abierta.sqlite"
OUTPUT_FILE = "chats_extraidos.txt"

def extraer_texto_limpio(blob_data):
    """
    Intenta decodificar bytes a UTF-8 y filtra caracteres no imprimibles.
    Funciona como el comando 'strings' de Linux pero para Python.
    """
    if not blob_data:
        return ""
    
    try:
        # 1. Decodificar ignorando errores (para saltar bytes binarios que no son texto)
        texto_sucio = blob_data.decode('utf-8', errors='ignore')
        
        # 2. Usar Regex para mantener solo caracteres imprimibles válidos en varios idiomas
        # Mantiene letras, números, puntuación básica, espacios y saltos de línea.
        # Filtra caracteres de control extraños (ASCII < 32 excepto \n y \r)
        texto_limpio = "".join(ch for ch in texto_sucio if ch.isprintable() or ch in ('\n', '\r', '\t'))
        
        # 3. Limpieza extra: Eliminar cadenas muy cortas que suelen ser ruido de metadatos
        # (Opcional) A veces aparecen nombres de clases como "messageText" o "user".
        return texto_limpio.strip()
    except Exception:
        return "[Error decodificando]"

def main():
    print(f"📂 Leyendo base de datos: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Seleccionamos ID y el BLOB de datos. Ordenamos por message_id (cronológico)
        # Filtramos solo aquellos que tienen data
        query = """
        SELECT dialog_id, sender_user_id, data 
        FROM messages 
        WHERE data IS NOT NULL 
        ORDER BY message_id DESC
        LIMIT 100
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        resultados = []
        
        print(f"🔍 Analizando los últimos {len(rows)} mensajes...")
        
        for dialog_id, sender_id, blob in rows:
            # Extraer texto del binario
            contenido = extraer_texto_limpio(blob)
            
            # Filtro simple: Si el contenido es muy corto o vacío, probablemente era una foto sin caption
            if len(contenido) > 3: 
                resultados.append({
                    "Chat ID": dialog_id,
                    "Sender ID": sender_id,
                    "Contenido Rescatado": contenido
                })

        conn.close()

        # Mostrar resultados en consola
        if resultados:
            df = pd.DataFrame(resultados)
            # Ajustar pandas para mostrar todo el texto
            pd.set_option('display.max_colwidth', None)
            print("\n✅ MENSAJES ENCONTRADOS (Muestra):")
            print(df.to_string(index=False))
            
            # Guardar en archivo para lectura cómoda
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                f.write(f"REPORTE DE EXTRACCIÓN - {len(resultados)} mensajes\n")
                f.write("="*50 + "\n\n")
                for item in resultados:
                    f.write(f"Chat: {item['Chat ID']} | User: {item['Sender ID']}\n")
                    f.write(f"Mensaje: {item['Contenido Rescatado']}\n")
                    f.write("-" * 20 + "\n")
            print(f"\n💾 Reporte completo guardado en: {OUTPUT_FILE}")
            
        else:
            print("⚠️ No se pudo rescatar texto legible. Es posible que los mensajes sean solo imágenes o metadatos puros.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()