@echo off
cd /d "%~dp0"
set "PATH=%PATH%;D:\Git\cmd"
title Enviando para o GitHub - SyncHub

echo ====================================================================
echo   RODOMAX TRANSPORTES - ENVIANDO PROJETO PARA O GITHUB
echo ====================================================================
echo.
echo Executando: git push -u origin main...
echo.

git push -u origin main

echo.
if %errorlevel% equ 0 (
    echo ====================================================================
    echo [SUCESSO] Projeto enviado para o GitHub com sucesso!
    echo ====================================================================
) else (
    echo ====================================================================
    echo [AVISO] Se solicitou login, faca o login na janela/navegador.
    echo ====================================================================
)
echo.
pause
