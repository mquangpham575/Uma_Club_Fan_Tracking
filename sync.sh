#!/bin/bash
set -e

echo "========================================================"
echo "  Uma Tracker - Manual Synchronization Trigger"
echo "========================================================"
echo ""

echo "[*] Sending sync trigger to http://localhost:8085/trigger..."
curl -s -X POST http://localhost:8085/trigger
echo ""
echo "[OK] Sync command sent. Stream logs with: docker logs -f uma-tracker"
