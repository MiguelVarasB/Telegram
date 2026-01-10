import time
import sys
import os

# Agregar el path del proyecto para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import log_timing

from unigram_cacheo.etapa_indexar import run_etapa_indexar
from unigram_cacheo.etapa_completar_unique import completar_unique_ids
from unigram_cacheo.etapa_reportar_pendientes import reportar_thumbs_pendientes

Tiempo_entre_escaneos=30
def main():
    for i in range(5):
        # 1) Indexar cache físico de Unigram
        run_etapa_indexar()
        # 2) Completar unique_id desde la base principal (chats.db)
        completar_unique_ids()
        # 3) Reportar thumbs pendientes de subir al servidor
        reportar_thumbs_pendientes()
        # Pausa entre ciclos
        if i < 4:
            log_timing(f" Dormiendo {Tiempo_entre_escaneos} segundos...")
            time.sleep(Tiempo_entre_escaneos)


if __name__ == "__main__":
    main()
