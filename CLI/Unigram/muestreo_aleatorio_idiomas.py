import os
import time
import sys
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from faster_whisper import WhisperModel
from obtener_videos import listar_videos_para_muestra
from pathlib import Path

# --- CONFIGURACIÓN ---
# Asegúrate de que esta ruta sea correcta en tu máquina
RUTA_CACHE = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\videos"

# Configuración para RTX 3050 8GB
# 5 workers suelen ser suficientes para saturar la GPU sin overhead excesivo de contexto

MODELO = "base" # Opciones: "tiny", "base", "small". "base" es un buen equilibrio.
COMPUTE_TYPE = "float16" # La serie 30xx soporta float16 nativo muy bien.
MAX_WORKERS = 6 
# Diccionario para forzar idioma según patrones en el nombre del archivo
# Formato: "patrón": "idioma_forzado"
# El patrón puede ser cualquier substring que aparezca en el nombre del archivo
IDIOMAS_FORZADOS_base = {
    "Salome Gil": "ES",    
    "msmartinasmith": "ES",
    "alejandra_diaz": "ES",  
    "your_dolls - Chaturbate": "ES",
    "Kourtney_Love": "ES",    
     "SofiaSmith": "ES",
     "Jennifer_White":"EN",
     "Nicolette_Shea":"EN",
   
}
#convertir a mayusculas
IDIOMAS_FORZADOS = {k.upper(): v.upper() for k, v in IDIOMAS_FORZADOS_base.items()}
#reemplazamos los espacios por _
IDIOMAS_FORZADOS = {k.replace(" ", "_"): v for k, v in IDIOMAS_FORZADOS.items()}
duracion_segundos_max = 600
if MODELO == "tiny":
    MAX_WORKERS = 10

LIMITE_DB = 3000

BASE_DIR = Path(__file__).resolve().parents[2]



BD_UNIGRAM = BASE_DIR / "database/unigram.db"

def obtener_videos_desde_db(limite=LIMITE_DB):
    """Obtiene videos desde la tabla cacheo que no tienen idioma, con límite especificado."""
    try:
        conn = sqlite3.connect(BD_UNIGRAM)
        cursor = conn.cursor()
        
        # Si el modelo es tiny, filtrar videos de menos de 5 minutos
        if MODELO == "tiny":
            cursor.execute("""
                SELECT archivo 
                FROM cacheo 
                WHERE (idioma IS NULL OR idioma = '' OR idioma = 'NULL')
                AND tipo = 'video'
                AND encontrado = 1
                AND duracion_segundos < ?
                ORDER BY id DESC
                LIMIT ?
            """, (duracion_segundos_max, limite))
            print(f"📋 Modelo 'tiny' - filtrando videos < 5 minutos")
        else:
            cursor.execute("""
                SELECT archivo 
                FROM cacheo 
                WHERE ((idioma IS NULL OR idioma = '' OR idioma = 'NULL')
                OR confianza < 0.8)
                AND tipo = 'video'
                AND encontrado = 1
                ORDER BY id DESC
                LIMIT ?
            """, (limite,))
            print(f"📋 Modelo '{MODELO}' - incluyendo videos con confianza < 80%")
        
        videos = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"📋 Se encontraron {len(videos)} videos sin idioma en la base de datos")
        return videos
        
    except Exception as e:
        print(f"❌ Error obteniendo videos desde DB: {e}")
        return []


def buscar_archivo(prefijo, lista):
    """Busca un archivo en la lista que comience con el prefijo dado."""
    for f in lista:
        if f.startswith(prefijo): return f
    return None

def forzar_idioma_segun_nombre(nombre_archivo):
    """Verifica si el nombre del archivo coincide con algún patrón para forzar idioma."""
    # Aplicar las mismas transformaciones que a los patrones
    nombre_transformado = nombre_archivo.upper().replace(" ", "_")
    
    # Buscar patrones en orden de prioridad (más específicos primero)
    for patron, idioma in IDIOMAS_FORZADOS.items():
        if patron in nombre_transformado:
            return idioma
    
    return None  # No hay patrón coincidente

def analizar_video_detallado(nombre_archivo, model):
    """Procesa un video individual para detectar idioma y texto inicial."""
    ruta_completa = os.path.join(RUTA_CACHE, nombre_archivo)
    inicio_ia = time.perf_counter()
    
    try:
        if not os.path.exists(ruta_completa):
            return {"archivo": nombre_archivo, "estado": "ERROR", "error": "Archivo no encontrado"}

        # Obtenemos el peso del archivo en MB
        peso_mb = os.path.getsize(ruta_completa) / (1024 * 1024)
        
        # PRIMERO: Verificar si hay idioma forzado por patrón en el nombre
        idioma_forzado = forzar_idioma_segun_nombre(nombre_archivo)
        
        if idioma_forzado:
            # Si hay idioma forzado, omitimos el análisis de IA y devolvemos resultado directo
            fin_ia = time.perf_counter()
            return {
                "archivo": nombre_archivo,
                "peso": peso_mb,
                "duracion": 0,  # No se analiza duración
                "idioma": idioma_forzado,
                "confianza": 1.0,  # 100% de confianza cuando se fuerza por patrón
                "tiempo_ia": fin_ia - inicio_ia,  # Tiempo mínimo (solo verificación de archivo)
                "texto": "[Idioma forzado por patrón - sin análisis IA]",
                "estado": "OK",
                "modelo": MODELO,
                "forzado": True
            }
        
        # Si no hay idioma forzado, procedemos con análisis normal de IA
        # Transcripción
        # vad_filter=True ayuda a saltar silencios iniciales (intro, música, etc.)
        # beam_size reducido a 2 para mayor velocidad en muestreo
        segments, info = model.transcribe(
            ruta_completa, 
            beam_size=2, 
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=500)
        )
        
        texto_detectado = ""
        # Solo leemos el primer segmento para identificar el idioma rápido
        for s in segments:
            texto_detectado = s.text.strip()
            break 
            
        fin_ia = time.perf_counter()
        
        # Verificar que info tenga los datos esperados
        if not hasattr(info, 'language') or not hasattr(info, 'language_probability'):
            return {"archivo": nombre_archivo, "estado": "ERROR", "error": "Modelo no devolvió información de idioma"}
        
        return {
            "archivo": nombre_archivo,
            "peso": peso_mb,
            "duracion": info.duration if hasattr(info, 'duration') else 0,
            "idioma": info.language.upper() if hasattr(info, 'language') else "NN",
            "confianza": info.language_probability if hasattr(info, 'language_probability') else 0.0,
            "tiempo_ia": fin_ia - inicio_ia,
            "texto": texto_detectado if texto_detectado else "[Sin voz/texto detectable]",
            "estado": "OK",
            "modelo": MODELO,
            "forzado": False
        }
    except Exception as e:
        return {"archivo": nombre_archivo, "estado": "ERROR", "error": f"Error procesando video: {str(e)}"}

def actualizar_idioma_en_db_lote(resultados_lote):
    """Actualiza idiomas, confianza, tiempo de IA y modelo en la base de datos para un lote de resultados."""
    try:
        conn = sqlite3.connect(BD_UNIGRAM)
        cursor = conn.cursor()
        
        # Preparar datos para inserción masiva
        datos_actualizar = []
        for res in resultados_lote:
            if res["estado"] == "OK":
                datos_actualizar.append((res['idioma'], res['confianza'], res['tiempo_ia'], res.get('modelo', MODELO), res['archivo']))
        
        if datos_actualizar:
            # Usar executemany para inserción masiva
            cursor.executemany(
                "UPDATE cacheo SET idioma = ?, confianza = ?, tiempo_ia = ?, modelo = ? WHERE archivo = ?",
                datos_actualizar
            )
            
            conn.commit()
            print(f"💾 Lote guardado: {len(datos_actualizar)} idiomas en DB")
            conn.close()
            return len(datos_actualizar)
        else:
            conn.close()
            return 0
            
    except Exception as e:
        print(f"❌ Error guardando lote en DB: {e}")
        return 0

def actualizar_idioma_en_db(nombre_archivo, idioma, confianza):
    """Actualiza el campo idioma y confianza en la tabla cacheo para el archivo especificado."""
    try:
        conn = sqlite3.connect(BD_UNIGRAM)
        cursor = conn.cursor()
        
        # Actualizar el idioma y confianza para el archivo
        cursor.execute(
            "UPDATE cacheo SET idioma = ?, confianza = ? WHERE archivo = ?",
            (idioma, confianza, nombre_archivo)
        )
        
        # Verificar si se actualizó alguna fila
        if cursor.rowcount > 0:
            print(f"💾 Idioma '{idioma}' (confianza: {confianza:.2%}) guardado en DB para: {nombre_archivo}")
        else:
            print(f"⚠️ No se encontró registro en DB para: {nombre_archivo}")
        
        conn.commit()
        conn.close()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"❌ Error guardando idioma/confianza en DB: {e}")
        return False

def ejecutar():
    # Configurar salida de consola para caracteres especiales (emojis)
    sys.stdout.reconfigure(encoding='utf-8')

    if not os.path.exists(RUTA_CACHE):
        print(f"Error crítico: Ruta no encontrada {RUTA_CACHE}")
        return

    print(f"🚀 Cargando modelo '{MODELO}' en GPU (RTX 3050)...")
    try:
        model = WhisperModel(MODELO, device="cuda", compute_type=COMPUTE_TYPE)
    except Exception as e:
        print(f"❌ Error cargando modelo en CUDA: {e}")
        print("Intentando cargar en CPU (lento)...")
        model = WhisperModel(MODELO, device="cpu", compute_type="int8")
    
    print("📂 Escaneando directorio de caché...")
    try:
        archivos_en_disco = os.listdir(RUTA_CACHE)
    except Exception as e:
        print(f"Error leyendo directorio: {e}")
        return

    # VIDEOS_TEST se obtiene dinámicamente desde la base de datos
    VIDEOS_TEST = obtener_videos_desde_db(LIMITE_DB)

    # Filtramos la lista para procesar solo los que existen
    muestra = []
    for p in VIDEOS_TEST:
        encontrado = buscar_archivo(p, archivos_en_disco)
        if encontrado:
            muestra.append(encontrado)
        else:
            # Opcional: Avisar si falta alguno de la lista TEST
            # print(f"⚠️ No encontrado: {p}")
            pass

    print(f"--- Análisis PARALELO ({MAX_WORKERS} hilos) de {len(muestra)} videos ---")
    print(f"{'ARCHIVO':<30} | {'PESO':<9} | {'DUR':<6} | {'LANG':<5} | {'CONF':<7} | {'IA TIME':<8} | {'TEXTO'}")
    print("-" * 120)
    
    tiempo_inicio_total = time.perf_counter()
    resultados = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviamos la referencia del modelo cargado a los hilos
        futures = [executor.submit(analizar_video_detallado, n, model) for n in muestra]
        
        for future in futures:
            res = future.result()
            resultados.append(res)
            
            if res["estado"] == "OK":
                nombre_corto = (res['archivo'][:27] + '..') if len(res['archivo']) > 27 else res['archivo']
                texto_corto = (res['texto'][:40] + '..') if len(res['texto']) > 40 else res['texto']
                
                # Mostrar indicador si el idioma fue forzado
                lang_indicator = f"{res['idioma']:<5}" + ("*" if res.get('forzado', False) else " ")
                
                print(f"{nombre_corto:<30} | {res['peso']:>6.1f} MB | {res['duracion']:>4.0f}s | {lang_indicator:<6} | {res['confianza']:.2%} | {res['tiempo_ia']:>6.2f}s | {texto_corto}")
            else:
                nombre_corto = (res['archivo'][:27] + '..') if len(res['archivo']) > 27 else res['archivo']
                print(f"❌ {nombre_corto:<30} | ERROR: {res['error']}")

    tiempo_fin_total = time.perf_counter()
    print("-" * 120)
    print(f"⏱️ TIEMPO TOTAL RELOJ: {tiempo_fin_total - tiempo_inicio_total:.2f} segundos")
    print("📝 * = Idioma forzado por patrón en nombre de archivo")
    
    # Guardar resultados en la base de datos por lotes
    print("\n💾 Guardando idiomas detectados en la base de datos por lotes...")
    guardados = 0
    
    # Procesar resultados en lotes de MAX_WORKERS
    for i in range(0, len(resultados), MAX_WORKERS):
        lote = resultados[i:i + MAX_WORKERS]
        guardados_lote = actualizar_idioma_en_db_lote(lote)
        guardados += guardados_lote
    
    print(f"✅ Se guardaron {guardados} idiomas en la base de datos")

if __name__ == "__main__":
    ejecutar()