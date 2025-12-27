"""
Configuración central.
Agregamos configuración para Smart Cache, Sprites y BOT.
"""
import os

# --- TELEGRAM API (USERBOT) ---
API_ID = 24228679
API_HASH = "7f3be49bb9d02f8e212bf7702992a936"
SESSION_NAME = "mi_sesion_premium"
# --- TELEGRAM API (USERBOT2) ---
API_ID2 = 23308309
API_HASH2 = "f2c7ede77671c3e6caf4a0d94958469c"
SESSION_NAME2 = "mi_session_free"
CANALES_CON_ACCESO_FREE = [
    -1001713639965,
    -1001560601095,
    -1002349297083,
    -1001098270440,
    -1002238663805,
    -1001947662901,
    -1001710503448,
    -1001354611597

]

# --- TELEGRAM BOT API (NUEVO) ---
# Necesario para agregar_bot_inteligente.py y auditorías
BOT_TOKEN = "8589372190:AAGUKsQQsuPxPmfWnzEBDFfajwcgH0RfznU"
BOT_USERNAME = "GrergBot"
# === TUS BOTS ===
BOT_POOL_TOKENS = [
   BOT_TOKEN,                                         # Bot Principal (GrergBot)
   "7695211398:AAGdLXYVVNd98f0lMSHBPNngVpUPoAtxoBY",  # Grerg2Bot
   "8368533412:AAGYMI2iyqbV-SLiHGXx7aq9VmRjn9L-lw8",  # Grerg3Bot
   "8426887240:AAGBf6FKUy5X71EyaCpPaA_n7eQk57MX4fM",  # Grerg4Bot
   "7875632672:AAGT_-ZrOW1_7EzwvDGokl9P91geDN-6rv4",  # Grerg5Bot
   "8516028474:AAFYfTFezRKYLlzl-w4Lmql8nn3z4PDAI8k",  # Grerg6Bot
   "8365683753:AAEYkqKrvtBBWsS01u9pShrpOIuR_-_vEfY",  # Grerg7Bot
   "8341521447:AAGnN5AJOgrUvsRgSzjLUUYyzQqlSmyqZ4I",  # Grerg8Bot
   "7724077021:AAGt2JV5agLPxiuSZEz6QTt_-uN7x1yQBak",  # Grerg9Bot
   "8532138853:AAFaaLEZFFWQB6WutJSquHW4-qZomk71CBo",  # Grerg10Bot
   "8400645303:AAGawU5Z-A9bR0CXcNjH6DPulVOjHe1NAlY",  # Grerg11Bot 
    "8390230078:AAFcwIPFsIqT04bSQUCq_nsZ4ZzLK8VB7EY",  # Grerg12Bot 
    "7972586116:AAGqjZOOpYomxoxtNLE0Qg1rEYj_5Us_-eg",  # Grerg13Bot 
"8513051368:AAFkJbxRDnZ5Pl3xK0uexEqh-bU9flam19M",  # Grerg14Bot 
"8496672639:AAGDj0KcYf5LvxAiq8iZK2z5Y9ZIMZ6NBzc",  # Grerg15Bot 

   "8244860201:AAG_K4_GyGKFvrM5CLZ2LRAFSpjBRuBPj_g",  # TheMiguel1Bot
   "8510786809:AAFG8B9Tp49x0F4oKjhZ_SXXhAqQIWDrc1s",  # TheMiguel2Bot
   "8392841964:AAERGYefWPIBUf1MycYtIR9QZ-F39YN4yvg",  # TheMiguel3Bot 
   "8588904581:AAF11FTOTGReTqXq-eHth-Z3WNrl2t9Z3BQ",  # TheMiguel4Bot 
   "8439231334:AAF_IlR_QPWEcEwoAcHK2huUl6PXb3SDnyQ",  # TheMiguel5Bot 
    "8552729898:AAGB_-ALMtJEKCkqp_ZfuS2Mch39QVNPt7c",  # TheMiguel6Bot 
    "8525414464:AAHLA930LLS-kN-Fd8UkZ9zX-BDcmMKYqTY",  # TheMiguel7Bot 
    "8525414464:AAHLA930LLS-kN-Fd8UkZ9zX-BDcmMKYqTY",  # TheMiguel8Bot 
     "8320807878:AAFsB3x-H0GiXBgwj8djhWc48J1P_A70NxQ",  # TheMiguel9Bot 
       "8230589872:AAFTxo7zx0RSX2prWap_ZU0DjYHD-g4tSQ0",  # TheMiguel10Bot 
      "8449820634:AAHXOn-nQwF4ln2zzFNfoBrhpDbfRQlwnKs",  # TheMiguel11Bot   
      "8274355772:AAE4lWjThXGNBMKoSGJEkursUpMd2ZCxRTE",  # TheMiguel12Bot
       "7956199045:AAG2WmowTq50lYN0gPMeV9lYwQ2i_mNo5_E",  # TheMiguel13Bot
       "8583323206:AAHZhcROKZ5ZAS1OzVIHfRjLfYnm2eTuJp0",  # TheMiguel14Bot  
 "8442404956:AAGVL1ypy97MepYP_1F0bmZObszrT8Rvqkk",  # TheMiguel15Bot  

]
# --- CONTROL DE VELOCIDAD DE BOTS ---
# 1. Ritmo "Sprint" (Tiempo entre cada foto individual)
BOT_WAIT_MIN = 3  # Mínimo de segundos a esperar
BOT_WAIT_MAX = 5  # Máximo de segundos a esperar

# 2. Descanso "Boxes" (Para evitar bans largos)
BOT_BATCH_LIMIT = 50    # ¿Cada cuántas descargas paramos?
BOT_BATCH_COOLDOWN = 20 # ¿Cuántos segundos descansamos?

# En Telegram/config.py
CACHE_DUMP_VIDEOS_CHANNEL_ID = -1003512635282
# --- STREAMING ---
CHUNK_SIZE = 1024 * 1024  # 1MB

# --- SMART CACHE ---
SMART_CACHE_ENABLED = os.getenv("SMART_CACHE_ENABLED", "1").lower() not in ("0", "false", "no", "off")
# Límite de 2 GB para la carpeta de previsualización/caché
MAX_DISK_CACHE_SIZE = 4 * 1024 * 1024 * 1024  
# Tamaño ideal por video en disco (5 MB es un buen balance)
TARGET_VIDEO_CACHE_SIZE = 5 * 1024 * 1024

# --- CARPETAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUMP_FOLDER = os.path.join(BASE_DIR, "dumps")
FOLDER_SESSIONS = os.path.join(BASE_DIR, "sessions")
THUMB_FOLDER = os.path.join(DUMP_FOLDER, "thumbs", "videos")
GRUPOS_THUMB_FOLDER = os.path.join(DUMP_FOLDER, "thumbs", "grupos_canales")
JSON_FOLDER = os.path.join(DUMP_FOLDER, "json")

# Carpeta gestionada inteligentemente
CACHE_DIR = os.path.join(DUMP_FOLDER, "smart_cache") 

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
MAIN_TEMPLATE = "layout.html"
DB_PATH = os.path.join(BASE_DIR, "database", "chats.db")

# --- SERVIDOR ---
HOST = "127.0.0.2"
PORT = 8000

# --- FFMPEG / SPRITES ---
# Dimensiones solicitadas
SPRITE_COLS = 5
SPRITE_ROWS = 15
SPRITE_THUMB_WIDTH = 400

# Configuraciones por defecto
THUMB_WIDTH = 320
THUMB_QUALITY = 80
SPRITE_QUALITY = 70  # Calidad un poco menor para que el sprite no pese demasiado
MIN_SPRITE_DURATION = 5
MIN_FILE_SIZE = 1024

# Timeouts (segundos)
FFPROBE_TIMEOUT = 15
FFMPEG_THUMB_TIMEOUT = 30
FFMPEG_SPRITE_TIMEOUT = 180

def ensure_directories():
    # Agregamos FOLDER_SESSIONS a la lista de creación automática
    for folder in [DUMP_FOLDER, JSON_FOLDER, THUMB_FOLDER, GRUPOS_THUMB_FOLDER, CACHE_DIR, FOLDER_SESSIONS]:
        os.makedirs(folder, exist_ok=True)