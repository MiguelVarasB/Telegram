import sqlite3
import os

# Ruta de tu base de datos (según tu captura anterior)
DB_PATH = r"C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram\database\chats.db"

def obtener_peso_y_filas(db_path):
    if not os.path.exists(db_path):
        print(f"Error: No encuentro el archivo en: {db_path}")
        return

    print(f"--- Analizando: {os.path.basename(db_path)} ---")
    print("Contando filas y calculando tamaño (esto puede tomar unos segundos)...\n")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Obtener lista de tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        
        resultados = []

        for tabla in tablas:
            nombre_tabla = tabla[0]
            
            # Omitir tablas internas de SQLite
            if nombre_tabla.startswith("sqlite_"):
                continue

            # 2. Obtener columnas para poder sumar sus pesos
            cursor.execute(f"PRAGMA table_info(\"{nombre_tabla}\")")
            columnas = cursor.fetchall()
            nombres_col = [col[1] for col in columnas]
            
            if not nombres_col:
                resultados.append((nombre_tabla, 0, 0))
                continue

            # 3. Construir query híbrida: Cuenta filas Y suma bytes
            # COALESCE convierte nulos a 0 para no romper la suma
            suma_cols = " + ".join([f"COALESCE(LENGTH(CAST(\"{c}\" AS BLOB)), 0)" for c in nombres_col])
            
            # SELECT COUNT(*), SUM(...) FROM tabla
            query = f"SELECT COUNT(*), SUM({suma_cols}) FROM \"{nombre_tabla}\""
            
            cursor.execute(query)
            datos = cursor.fetchone()
            
            num_filas = datos[0]
            peso_bytes = datos[1]
            
            if peso_bytes is None: 
                peso_bytes = 0
            
            resultados.append((nombre_tabla, peso_bytes, num_filas))

        conn.close()

        # 4. Ordenar resultados por PESO (de mayor a menor)
        resultados.sort(key=lambda x: x[1], reverse=True)

        # 5. Imprimir tabla bonita
        header = f"{'TABLA':<25} | {'FILAS':<12} | {'PESO (MB)':<12} | {'PESO (GB)':<12}"
        print(header)
        print("-" * len(header))
        
        for nombre, bytes_val, filas in resultados:
            mb = bytes_val / (1024 * 1024)
            gb = bytes_val / (1024 * 1024 * 1024)
            # El format :, añade separadores de miles (ej: 1,000)
            print(f"{nombre:<25} | {filas:<12,} | {mb:,.2f} MB    | {gb:,.4f} GB")

    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    obtener_peso_y_filas(DB_PATH)