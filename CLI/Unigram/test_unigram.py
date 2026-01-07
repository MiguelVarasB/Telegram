import os
import io
import struct
import re
import json
from datetime import datetime
from sqlcipher3 import dbapi2 as sqlite
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

# --- CONFIGURACIÓN ---
MASTER_KEY = "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63"
DB_UNIGRAM = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\db.sqlite"
CANAL_ID = -1001147088141 # Canal de Índices

console = Console()

class ProcesadorIndices:
    @staticmethod
    def limpiar_basura(texto):
        """Limpia restos binarios y avisos de censura."""
        aviso = "This message couldn't be displayed on your device because it contains pornographic materials."
        texto = texto.replace(aviso, "")
        # Quitamos bytes de control al inicio del bloque
        return re.sub(r'^[^\w@#🤤🔞🛰👥🔥]+', '', texto).strip()

    @staticmethod
    def extraer_pares(blob: bytes):
        """Divide el BLOB en bloques de Texto + URL."""
        content = blob.decode('utf-8', errors='ignore')
        
        # 1. Buscamos la fecha real
        fecha = "N/A"
        try:
            ts = struct.unpack('<I', blob[28:32])[0]
            fecha = datetime.fromtimestamp(ts).strftime('%d.%m.%y %H:%M')
        except: pass

        # 2. Partimos el mensaje usando las URLs como divisores
        # Esto nos da: [Texto previo, URL 1, Texto intermedio, URL 2...]
        partes = re.split(r'(https?://[^\s<>"]+|t\.me/[^\s<>"]+)', content)
        
        pares = []
        # El primer elemento (índice 0) suele ser el título del mensaje global
        titulo_mensaje = ProcesadorIndices.limpiar_basura(partes[0]) if partes else ""

        # Recorremos de 2 en 2 para captar (Texto descriptivo + URL)
        for i in range(1, len(partes) - 1, 2):
            url = partes[i]
            # El texto descriptivo está en el bloque anterior, o es el título si es el primero
            descripcion = ProcesadorIndices.limpiar_basura(partes[i-1])
            
            # Limpiamos la URL de basura binaria al final
            url_limpia = re.sub(r'[^\w/.\-?=&#%]+$', '', url)
            
            pares.append({"texto": descripcion, "url": url_limpia})

        return {
            "fecha": fecha,
            "titulo_global": titulo_mensaje,
            "elementos": pares,
            "censurado": b"pornographic" in blob
        }

def renderizar_unigram_index(id_msg, datos):
    # Cabecera del mensaje principal
    console.print(f"\n[bold white on blue]  ID: {id_msg} | 📅 {datos['fecha']}  [/]")
    
    if datos['censurado']:
        console.print("[italic red]🚫 Contenido bloqueado por Telegram (Filtro Adultos)[/]")

    # Título principal del post (ej: INDICE DE GRUPOS)
    if datos['titulo_global']:
        console.print(f"\n[bold yellow]{datos['titulo_global']}[/]")

    # Listado emparejado (Igual que en image_5f91a2.png)
    for item in datos['elementos']:
        # Solo imprimimos si el texto no es demasiado corto (basura)
        if len(item['texto']) > 5:
            cuerpo = Text()
            # Dividimos el texto para poner la primera línea en negrita (Título del grupo)
            lineas = item['texto'].split('\n')
            cuerpo.append(f"• {lineas[0]}\n", style="bold white")
            
            # El resto es la descripción
            if len(lineas) > 1:
                desc = " ".join(lineas[1:]).strip()
                cuerpo.append(f"  {desc}\n", style="dim white")
            
            # El Link justo debajo del texto
            cuerpo.append(f"  {item['url']}", style="link blue underline")
            
            console.print(cuerpo)
            console.print(" ") # Espacio entre grupos

def ejecutar():
    try:
        conn = sqlite.connect(DB_UNIGRAM)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
        cursor.execute("PRAGMA cipher_compatibility = 4;")

        # Traemos el último mensaje de índice
        cursor.execute("SELECT message_id, data FROM messages WHERE dialog_id = ? ORDER BY message_id DESC LIMIT 5", (CANAL_ID,))
        
        row = cursor.fetchone()
        if row:
            m_id, blob = row
            datos = ProcesadorIndices.extraer_pares(blob)
            renderizar_unigram_index(m_id // 1048576, datos)
        
        conn.close()
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/]")

if __name__ == "__main__":
    ejecutar()