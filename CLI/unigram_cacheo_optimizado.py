import time
import multiprocessing as mp

# --- CORRECCIÓN AQUÍ: Cambia 'concurrente' por 'optimizado' ---
from unigram_cacheo.etapa_indexar_optimizado import run_etapa_indexar_optimizado 
# --------------------------------------------------------------

from unigram_cacheo.etapa_completar_unique import completar_unique_ids
from unigram_cacheo.etapa_reportar_pendientes import reportar_thumbs_pendientes
from unigram_cacheo.config_optimizacion import (
    Tiempo_entre_escaneos, 
    NUM_CICLOS, 
    print_config
)

def main():
    print("🚀 PIPELINE OPTIMIZADO DE CACHEO TELEGRAM (Versión Multi-Proceso Real)")
    print("=" * 60)
    print_config()
    
    print(f"\n🔄 Iniciando {NUM_CICLOS} ciclos de procesamiento...")
    print(f"⏱️  Tiempo de espera entre ciclos: {Tiempo_entre_escaneos} segundos")
    print("=" * 60)
    
    for i in range(NUM_CICLOS):
        print(f"\n📋 === CICLO {i+1}/{NUM_CICLOS} ===")
        
        print("🔍 Paso 1: Indexación paralela de archivos...")
        start_time = time.time()
        run_etapa_indexar_optimizado()
        elapsed = time.time() - start_time
        print(f"⏱️  Indexación completada en {elapsed:.1f} segundos")
        
        print("🔗 Paso 2: Completando unique_ids...")
        start_time = time.time()
        completar_unique_ids()
        elapsed = time.time() - start_time
        print(f"⏱️  Unique_ids completados en {elapsed:.1f} segundos")
        
        print("📊 Paso 3: Reportando thumbs pendientes...")
        start_time = time.time()
        reportar_thumbs_pendientes()
        elapsed = time.time() - start_time
        print(f"⏱️  Reporte completado en {elapsed:.1f} segundos")
        
        if i < NUM_CICLOS - 1:
            print(f"😴 Durmiendo {Tiempo_entre_escaneos} segundos...")
            time.sleep(Tiempo_entre_escaneos)
    
    print("\n✅ Pipeline optimizado completado exitosamente")

if __name__ == "__main__":
    mp.freeze_support()
    main()