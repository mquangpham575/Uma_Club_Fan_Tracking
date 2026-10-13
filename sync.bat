@echo off
setlocal
title Trigger Uma Tracker Sync

echo ========================================================
echo   Uma Tracker - Manual Synchronization Trigger
echo ========================================================
echo.

echo [*] Sending sync trigger to http://localhost:8085/trigger...
curl -s -X POST http://localhost:8085/trigger
if errorlevel 1 (
    echo.
    echo [ERROR] Could not connect to uma-tracker on port 8085.
    echo Is the container running? Start it with start.bat
    echo.
    pause
    exit /b 1
)

echo.
echo.
echo ========================================================
echo [OK] Synchronization initiated in background!
echo To view live sync progress, run:
echo    docker logs -f --tail=50 uma-tracker
echo ========================================================
echo.
pause
