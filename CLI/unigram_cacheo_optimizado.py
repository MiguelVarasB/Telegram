import time
import multiprocessing as mp

from unigram_cacheo.etapa_indexar_concurrente import run_etapa_indexar_optimizado
from unigram_cacheo.etapa_completar_unique import completar_unique_ids
from unigram_cacheo.etapa_reportar_pendientes import reportar_thumbs_pendientes
from unigram_cacheo.config_optimizacion import (
    Tiempo_entre_escaneos, 
    NUM_CICLOS, 
    print_config
)

def main():
    """
    Pipeline optimizado auto-configurado según hardware disponible
    
    Características:
    - Detección automática de CPU y RAM
    - Ajuste dinámico de parámetros de rendimiento
    - Procesamiento concurrente con hilos (más estable que multiprocessing)
    - Balance óptimo entre throughput y uso de recursos
    """
    print("🚀 PIPELINE OPTIMIZADO DE CACHEO TELEGRAM (Versión Concurrente)")
    print("=" * 60)
    
    # Mostrar configuración detectada
    print_config()
    
    print(f"\n🔄 Iniciando {NUM_CICLOS} ciclos de procesamiento...")
    print(f"⏱️  Tiempo de espera entre ciclos: {Tiempo_entre_escaneos} segundos")
    print("=" * 60)
    
    for i in range(NUM_CICLOS):
        print(f"\n📋 === CICLO {i+1}/{NUM_CICLOS} ===")
        
        # 1) Indexar cache físico de Unigram (versión concurrente)
        print("🔍 Paso 1: Indexación concurrente de archivos...")
        start_time = time.time()
        run_etapa_indexar_optimizado()
        elapsed = time.time() - start_time
        print(f"⏱️  Indexación completada en {elapsed:.1f} segundos")
        
        # 2) Completar unique_id desde la base principal (chats.db)
        print("🔗 Paso 2: Completando unique_ids...")
        start_time = time.time()
        completar_unique_ids()
        elapsed = time.time() - start_time
        print(f"⏱️  Unique_ids completados en {elapsed:.1f} segundos")
        
        # 3) Reportar thumbs pendientes de subir al servidor
        print("📊 Paso 3: Reportando thumbs pendientes...")
        start_time = time.time()
        reportar_thumbs_pendientes()
        elapsed = time.time() - start_time
        print(f"⏱️  Reporte completado en {elapsed:.1f} segundos")
        
        # Pausa entre ciclos
        if i < NUM_CICLOS - 1:
            print(f"😴 Durmiendo {Tiempo_entre_escaneos} segundos...")
            time.sleep(Tiempo_entre_escaneos)
    
    print("\n✅ Pipeline optimizado completado exitosamente")
    print("🎯 Todos los ciclos de procesamiento finalizados")

if __name__ == "__main__":
    main()
