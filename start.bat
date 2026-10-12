@echo off
setlocal
title Starting Uma Tracker

echo ========================================================
echo   Uma Tracker - Local Docker Deployment
echo ========================================================
echo.

:: 1. Verify Docker Daemon
echo [*] Checking Docker Daemon status...
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running.
    echo Please start Docker Desktop and wait until it is ready.
    pause
    exit /b 1
)
echo [OK] Docker Daemon is operational.
echo.

:: 2. Verify Credentials
if not exist "config\credentials.json" (
    echo [ERROR] Missing Google credentials at config\credentials.json
    echo Please place your valid Google Service Account credentials.json file.
    pause
    exit /b 1
)
if not exist ".env" (
    echo [ERROR] Missing .env file.
    echo Please create .env with CHRONO_API_KEY and POSTGRES_PASSWORD.
    pause
    exit /b 1
)
echo [OK] Configuration and credentials verified.
echo.

:: 3. Build and launch container
echo [*] Building and launching uma-tracker container...
docker compose up -d --build
if errorlevel 1 (
    echo [ERROR] Failed to start uma-tracker container.
    pause
    exit /b 1
)

echo.
echo ========================================================
echo   Deployment Success!
echo   Service is listening on http://localhost:8085
echo   Health check: curl http://localhost:8085/health
echo ========================================================
echo.
pause
