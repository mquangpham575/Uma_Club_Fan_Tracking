@echo off
title Uma Tracker Logs
docker compose logs -f --tail=100 uma-tracker
