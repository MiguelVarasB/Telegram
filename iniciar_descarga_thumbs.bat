@echo off
REM Secuencia: Reenviar videos -> Descargar dump

cd /d "C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram\CLI"

REM Intentar usar el lanzador "py" primero
where py >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo --- Ejecutando ciclo Reenvio/Descarga ---
    py flujo_reenviar_descargar.py
    echo.
   
) else (
    REM Fallback a "python" por si no existe "py"
    echo --- Ejecutando ciclo Reenvio/Descarga ---
    python flujo_reenviar_descargar.py
   
)

REM Mantener la ventana abierta para ver logs (opcional)
pause
