@echo off
cd /d "C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram"

REM Intentar usar el lanzador "py" primero
where py >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_CMD=py
) else (
    set PYTHON_CMD=python
)

:menu
%PYTHON_CMD% run_cli.py
echo.
goto menu
