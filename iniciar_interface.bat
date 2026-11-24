@echo off
cd /d "%~dp0"
title RODOMAX TRANSPORTES - HUB INTEGRADOR DE DADOS

echo ====================================================================
echo   RODOMAX TRANSPORTES - HUB INTEGRADOR DE DADOS
echo ====================================================================
echo.

REM 1. Tenta o Launcher py (padrao Windows)
where py >nul 2>nul
if %errorlevel% equ 0 (
    echo Iniciando aplicacao via Python Launcher [py]...
    start "" py app.py
    exit /b 0
)

REM 2. Tenta os caminhos diretos da instalacao Python em AppData
if exist "%LOCALAPPDATA%\Python\pythoncore-3.15-64\python.exe" (
    echo Iniciando via Python 3.15...
    start "" "%LOCALAPPDATA%\Python\pythoncore-3.15-64\python.exe" app.py
    exit /b 0
)

if exist "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" (
    echo Iniciando via Python 3.14...
    start "" "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" app.py
    exit /b 0
)

REM 3. Fallback padrao
start "" python app.py
exit /b 0
