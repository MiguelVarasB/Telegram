import ctypes
import json
import os
import sys
import time

# --- CONFIGURACIÓN DE SEGURIDAD ---
# Clave extraída de tu volcado anterior
CLAVE_DB = "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63"

# Rutas (Ajustar si tu carpeta se llama distinto)
CARPETA_BASE = os.path.dirname(__file__)
RUTA_DLL = os.path.join(CARPETA_BASE, "tdjson.dll")
RUTA_DB = os.path.join(CARPETA_BASE, "db_original")  # Carpeta con la base de datos cifrada

# IDs Oficiales de Unigram (Extraídos del código fuente)
API_ID = 17349
API_HASH = "344583e45741c457fe1862106095a5eb"

# --- CARGADOR DE LIBRERÍA (WRAPPER) ---
try:
    # En Windows, agregar el directorio de DLLs al path de búsqueda
    if sys.platform == 'win32' and sys.version_info >= (3, 8):
        os.add_dll_directory(CARPETA_BASE)
        print(f"📁 Directorio DLL agregado: {CARPETA_BASE}")
    
    # Cargar dependencias con ruta completa y flag especial
    dependencias = ["zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll"]
    LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008
    
    for dep in dependencias:
        ruta_dep = os.path.join(CARPETA_BASE, dep)
        if os.path.exists(ruta_dep):
            try:
                if sys.platform == 'win32':
                    ctypes.WinDLL(ruta_dep, winmode=LOAD_WITH_ALTERED_SEARCH_PATH)
                else:
                    ctypes.CDLL(ruta_dep)
                print(f"✅ Cargada: {dep}")
            except Exception as e:
                print(f"⚠️ No se pudo cargar {dep}: {str(e)[:60]}")
    
    # Ahora cargar tdjson.dll con el mismo método
    if sys.platform == 'win32':
        tdjson = ctypes.WinDLL(RUTA_DLL, winmode=LOAD_WITH_ALTERED_SEARCH_PATH)
    else:
        tdjson = ctypes.CDLL(RUTA_DLL)
    print(f"✅ Cargada: tdjson.dll")
except Exception as e:
    print(f"❌ Error fatal: No se pudo cargar {RUTA_DLL}")
    print(f"Detalle: {e}")
    print("\n💡 Posibles causas:")
    print("   1. Las DLLs son incompatibles con tu versión de Windows")
    print("   2. Las DLLs requieren dependencias adicionales del sistema")
    print("   3. Las DLLs están corruptas o incompletas")
    print("\n📋 Estado de archivos:")
    for dep in ["tdjson.dll", "zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll"]:
        ruta = os.path.join(CARPETA_BASE, dep)
        if os.path.exists(ruta):
            tamaño = os.path.getsize(ruta)
            print(f"      ✅ {dep} ({tamaño:,} bytes)")
        else:
            print(f"      ❌ {dep} (no existe)")
    sys.exit(1)

# Definir funciones nativas de la DLL
td_create = tdjson.td_json_client_create
td_create.restype = ctypes.c_void_p
td_create.argtypes = []

td_send = tdjson.td_json_client_send
td_send.restype = None
td_send.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

td_receive = tdjson.td_json_client_receive
td_receive.restype = ctypes.c_char_p
td_receive.argtypes = [ctypes.c_void_p, ctypes.c_double]

def enviar(cliente, metodo, parametros=None):
    if parametros is None: parametros = {}
    parametros['@type'] = metodo
    query = json.dumps(parametros).encode('utf-8')
    td_send(cliente, query)

def recibir(cliente, timeout=1.0):
    res = td_receive(cliente, timeout)
    if res:
        return json.loads(res.decode('utf-8'))
    return None

# --- PROGRAMA PRINCIPAL ---
def main():
    print("🚀 Iniciando cliente forense de Unigram...")
    client = td_create()

    # Desactivar logs ruidosos
    enviar(client, "setLogVerbosityLevel", {"new_verbosity_level": 1})

    autorizado = False
    
    while True:
        evento = recibir(client)
        if not evento: continue

        tipo = evento.get('@type')

        # 1. Manejo de Autorización
        if tipo == 'updateAuthorizationState':
            estado = evento['authorization_state']['@type']
            print(f"🔑 Estado: {estado}")

            if estado == 'authorizationStateWaitTdlibParameters':
                # Configuración exacta de Unigram
                params = {
                    "use_test_dc": False,
                    "database_directory": RUTA_DB,
                    "files_directory": RUTA_DB,
                    "use_file_database": True,
                    "use_chat_info_database": True,
                    "use_message_database": True,
                    "use_secret_chats": True,
                    "api_id": API_ID,
                    "api_hash": API_HASH,
                    "system_language_code": "es",
                    "device_model": "ForensePC",
                    "application_version": "1.0",
                    "enable_storage_optimizer": False
                }
                enviar(client, "setTdlibParameters", params)

            elif estado == 'authorizationStateWaitEncryptionKey':
                print("🔓 Inyectando clave de cifrado...")
                enviar(client, "checkDatabaseEncryptionKey", {"encryption_key": CLAVE_DB})

            elif estado == 'authorizationStateWaitPhoneNumber':
                print("\n✅ ¡ÉXITO! La base de datos se abrió correctamente.")
                print("⚠️ (Pide teléfono porque no conectamos a internet, pero ya podemos leer la DB local)")
                autorizado = True
                break

            elif estado == 'authorizationStateReady':
                print("✅ Autorizado y listo.")
                autorizado = True
                break
        
        # Manejo de errores
        if tipo == 'error':
            print(f"❌ Error de TDLib: {evento['message']}")

    if autorizado:
        print("\n📚 Extrayendo lista de chats...")
        enviar(client, "getChats", {"chat_list": {"@type": "chatListMain"}, "limit": 50})
        
        # Escuchar respuestas por unos segundos
        inicio = time.time()
        chats_encontrados = 0
        
        while time.time() - inicio < 5:
            e = recibir(client)
            if not e: continue
            
            # Al recibir un chat, mostramos su nombre
            if e.get('@type') == 'updateNewChat':
                chat = e['chat']
                titulo = chat.get('title', 'Sin título')
                print(f"📂 Chat encontrado: [{chat['id']}] {titulo}")
                chats_encontrados += 1
                
                # EJEMPLO: Leer los últimos 5 mensajes de este chat
                # enviar(client, "getChatHistory", {"chat_id": chat['id'], "limit": 5})

        print(f"\n✨ Proceso finalizado. Se encontraron {chats_encontrados} chats.")

if __name__ == "__main__":
    main()