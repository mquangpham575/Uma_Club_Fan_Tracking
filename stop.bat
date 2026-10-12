@echo off
setlocal
title Stopping Uma Tracker

echo ========================================================
echo   Stopping Uma Tracker Container
echo ========================================================
echo.

docker compose down
echo.
echo [OK] Uma Tracker service stopped.
pause
