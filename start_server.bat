@echo off
cd /d "C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram"

REM Intentar usar el lanzador "py" primero
where py >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_CMD=py
) else (
    set PYTHON_CMD=python
)

echo Iniciando servidor MegaTelegram Local...
echo Host: 127.0.0.2
echo Port: 8000
echo.
echo Presiona Ctrl+C para detener el servidor
echo.

%PYTHON_CMD% app.py
