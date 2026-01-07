@echo off
REM Activa tu entorno virtual (ajusta la ruta si es distinto)
cd /d "C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram\CLI"

REM Ejecuta el pipeline con tope de 5 nuevos indexados.
python pipeline_cantidad_videos.py --max-nuevos 5

PAUSE
