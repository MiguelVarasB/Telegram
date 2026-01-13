"""
COMPARACIÓN DE RENDIMIENTO - Indexador de Telegram
==================================================

Este script compara el rendimiento entre:
- Versión original (secuencial)
- Versión optimizada (concurrente)

Resultados esperados para hardware de alto rendimiento:
- Versión original: ~100-200 archivos/segundo
- Versión optimizada: ~500-1000+ archivos/segundo
"""

import time
import sys
import os

# Agregar el path del proyecto para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import log_timing

from unigram_cacheo.etapa_indexar import run_etapa_indexar as run_etapa_original
from unigram_cacheo.etapa_indexar_concurrente import run_etapa_indexar_optimizado
from unigram_cacheo.config_optimizacion import print_config

def medir_rendimiento(func_indexadora, nombre):
    """
    Mide el rendimiento de una función indexadora.
    
    Args:
        func_indexadora: Función a medir
        nombre: Nombre descriptivo de la función
    """
    print(f"\n🚀 Probando: {nombre}")
    print("=" * 50)
    
    start_time = time.time()
    try:
        func_indexadora()
        elapsed = time.time() - start_time
        
        print(f"\n✅ {nombre} completada en {elapsed:.1f} segundos")
        return elapsed
    except Exception as e:
        print(f"❌ Error en {nombre}: {e}")
        return None

def main():
    """
    Función principal que compara el rendimiento de ambas versiones.
    """
    print("🏁 COMPARACIÓN DE RENDIMIENTO - INDEXADOR TELEGRAM")
    print("=" * 60)
    
    # Mostrar configuración del sistema
    print_config()
    
    print("\n📊 INICIANDO PRUEBAS DE RENDIMIENTO")
    print("Nota: Se procesarán los mismos archivos en ambas pruebas")
    print("-" * 60)
    
    # Medir versión original
    tiempo_original = medir_rendimiento(run_etapa_original, "Versión Original (Secuencial)")
    
    if tiempo_original is None:
        print("❌ No se pudo completar la prueba de la versión original")
        return
    
    # Pequeña pausa entre pruebas
    print("\n⏱️ Pausa de 5 segundos entre pruebas...")
    time.sleep(5)
    
    # Medir versión optimizada
    tiempo_optimizado = medir_rendimiento(run_etapa_indexar_optimizado, "Versión Optimizada (Concurrente)")
    
    if tiempo_optimizado is None:
        print("❌ No se pudo completar la prueba de la versión optimizada")
        return
    
    # Calcular y mostrar mejoras
    print("\n" + "=" * 60)
    print("📈 RESULTADOS DE LA COMPARACIÓN")
    print("=" * 60)
    
    if tiempo_original > 0 and tiempo_optimizado > 0:
        mejora_velocidad = tiempo_original / tiempo_optimizado
        ahorro_tiempo = tiempo_original - tiempo_optimizado
        porcentaje_ahorro = (ahorro_tiempo / tiempo_original) * 100
        
        print(f"⏱️  Tiempo Versión Original:     {tiempo_original:.1f} segundos")
        print(f"⚡ Tiempo Versión Optimizada:   {tiempo_optimizado:.1f} segundos")
        print(f"🚀 Mejora de Velocidad:         {mejora_velocidad:.1f}x más rápido")
        print(f"💾 Ahorro de Tiempo:            {ahorro_tiempo:.1f} segundos ({porcentaje_ahorro:.1f}%)")
        
        # Recomendaciones
        if mejora_velocidad > 3:
            print("\n🎯 RECOMENDACIÓN:")
            print("   ✅ La versión optimizada muestra una mejora significativa")
            print("   ✅ Se recomienda usar la versión optimizada para producción")
            print("   ✅ Ejecutar: python unigram_cacheo_optimizado.py")
        elif mejora_velocidad > 1.5:
            print("\n🎯 RECOMENDACIÓN:")
            print("   ⚠️ La versión optimizada es más rápida")
            print("   ⚠️ Considere usar la versión optimizada")
            print("   ⚠️ Ejecutar: python unigram_cacheo_optimizado.py")
        else:
            print("\n🎯 RECOMENDACIÓN:")
            print("   ℹ️ Las diferencias son mínimas")
            print("   ℹ️ Puede continuar usando la versión original")
            print("   ℹ️ Ejecutar: python unigram_cacheo.py")
    else:
        print("❌ No se pudo calcular la comparación de rendimiento")

if __name__ == "__main__":
    main()
