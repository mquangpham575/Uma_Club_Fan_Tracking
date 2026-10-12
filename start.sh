#!/bin/bash
set -e

echo "========================================================"
echo "  Uma Tracker - Docker Deployment"
echo "========================================================"
echo ""

if ! docker info >/dev/null 2>&1; then
    echo "[ERROR] Docker is not running. Please start Docker."
    exit 1
fi

if [ ! -f "config/credentials.json" ]; then
    echo "[ERROR] Missing Google credentials at config/credentials.json"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "[ERROR] Missing .env file."
    exit 1
fi

echo "[*] Building and launching uma-tracker container..."
docker compose up -d --build

echo ""
echo "========================================================"
echo "  Uma Tracker listening on http://localhost:8085"
echo "========================================================"
