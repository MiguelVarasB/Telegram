"""
Script de prueba para verificar la configuración optimizada
"""

from unigram_cacheo.config_optimizacion import print_config
from unigram_cacheo.etapa_indexar_optimizado import iter_archivos_nuevos_optimizado
from unigram_cacheo.common import preparar_base_local

def test_configuracion():
    """Prueba la configuración y rendimiento del sistema optimizado."""
    print("🧪 PRUEBA DE CONFIGURACIÓN OPTIMIZADA")
    print("=" * 50)
    
    # Mostrar configuración detectada
    print_config()
    
    print("\n🔍 Probando escaneo de archivos...")
    
    # Probar escaneo rápido
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    
    import time
    start_time = time.time()
    nuevos = iter_archivos_nuevos_optimizado(existentes)
    elapsed = time.time() - start_time
    
    print(f"📊 Resultados del escaneo:")
    print(f"   - Archivos ya indexados: {len(existentes)}")
    print(f"   - Archivos nuevos encontrados: {len(nuevos)}")
    print(f"   - Tiempo de escaneo: {elapsed:.2f} segundos")
    
    if len(nuevos) > 0:
        print(f"   - Velocidad de escaneo: {len(nuevos)/elapsed:.1f} archivos/segundo")
        print("\n📝 Primeros 5 archivos nuevos:")
        for i, archivo in enumerate(nuevos[:5]):
            print(f"   {i+1}. {archivo['nombre']} ({archivo['tipo']})")
    
    print("\n✅ Prueba completada exitosamente")
    
    # Recomendaciones
    if len(nuevos) > 1000:
        print("\n💡 RECOMENDACIÓN:")
        print("   Se detectaron muchos archivos nuevos.")
        print("   El procesamiento paralelo proporcionará una mejora significativa.")
        print("   Ejecuta: python unigram_cacheo_optimizado.py")
    elif len(nuevos) > 0:
        print("\n💡 RECOMENDACIÓN:")
        print("   Hay archivos nuevos para procesar.")
        print("   Puedes ejecutar el pipeline optimizado.")
        print("   Ejecuta: python unigram_cacheo_optimizado.py")
    else:
        print("\n💡 RECOMENDACIÓN:")
        print("   No hay archivos nuevos. El sistema está actualizado.")

if __name__ == "__main__":
    test_configuracion()
