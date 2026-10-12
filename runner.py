"""
Uma Tracker Scheduling & Webhook Runner Daemon

Intent:
    Runs a 24/7 background service that triggers daily Google Sheets sync
    at 10:15 UTC (after Chrono reset) and exposes an HTTP webhook endpoint
    for on-demand sync from UmaCore Discord bot.
"""

import asyncio
from datetime import datetime, time, timedelta, timezone
import json
import logging
import os
import sys
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("tracker-runner")

# Global lock to prevent overlapping runs and API rate limits
SYNC_LOCK = asyncio.Lock()

# Runner state
RUNNER_STATE = {
    "is_running": False,
    "last_run_started_utc": None,
    "last_run_finished_utc": None,
    "last_status": "initialized",
    "last_error": None,
    "next_scheduled_utc": None,
}


async def run_tracker_subprocesses(force: bool = False) -> bool:
    """
    Executes the tracker sync routines sequentially:
    1. src/main.py --cron --engine chrono [--force]
    2. OnlyRex/only_rex_tracker.py --cron [--force]
    """
    cmd1 = [sys.executable, "src/main.py", "--cron", "--engine", "chrono"]
    if force:
        cmd1.append("--force")

    cmd2 = [sys.executable, "OnlyRex/only_rex_tracker.py", "--cron"]
    if force:
        cmd2.append("--force")

    logger.info("Executing Step 1/2: %s", " ".join(cmd1))
    proc1 = await asyncio.create_subprocess_exec(
        *cmd1,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    stdout1, _ = await proc1.communicate()
    if stdout1:
        for line in stdout1.decode(errors="replace").splitlines():
            logger.info(" [main.py] %s", line)

    if proc1.returncode != 0:
        logger.error("Step 1 (main.py) failed with exit code %s", proc1.returncode)
        return False

    logger.info("Step 1 (main.py) succeeded. Executing Step 2/2: %s", " ".join(cmd2))
    proc2 = await asyncio.create_subprocess_exec(
        *cmd2,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    stdout2, _ = await proc2.communicate()
    if stdout2:
        for line in stdout2.decode(errors="replace").splitlines():
            logger.info(" [only_rex] %s", line)

    if proc2.returncode != 0:
        logger.error("Step 2 (only_rex_tracker.py) failed with exit code %s", proc2.returncode)
        return False

    logger.info("Step 2 (only_rex_tracker.py) succeeded. All sync tasks completed.")
    return True


async def perform_sync(trigger_source: str, force: bool = False):
    """
    Executes the sync pipeline protected by SYNC_LOCK.
    """
    if SYNC_LOCK.locked():
        logger.warning("Sync requested via [%s] but another sync is already in progress. Skipping.", trigger_source)
        return False

    async with SYNC_LOCK:
        RUNNER_STATE["is_running"] = True
        RUNNER_STATE["last_run_started_utc"] = datetime.now(timezone.utc).isoformat()
        RUNNER_STATE["last_status"] = f"running (source: {trigger_source})"
        RUNNER_STATE["last_error"] = None
        logger.info("Starting sync routine. Source: %s, Force: %s", trigger_source, force)

        try:
            success = await run_tracker_subprocesses(force=force)
            RUNNER_STATE["last_status"] = "success" if success else "failed"
            return success
        except Exception as exc:
            logger.exception("Unexpected error during sync execution: %s", exc)
            RUNNER_STATE["last_status"] = "error"
            RUNNER_STATE["last_error"] = str(exc)
            return False
        finally:
            RUNNER_STATE["is_running"] = False
            RUNNER_STATE["last_run_finished_utc"] = datetime.now(timezone.utc).isoformat()


async def handle_trigger(request: web.Request) -> web.Response:
    """
    HTTP POST /trigger endpoint.
    Triggered by UmaCore bot when club quotas or club configs are updated.
    """
    if SYNC_LOCK.locked():
        return web.json_response(
            {
                "status": "busy",
                "message": "A sheet synchronization is already currently in progress.",
                "state": RUNNER_STATE
            },
            status=202
        )

    # Launch sync in background task so HTTP response returns immediately
    asyncio.create_task(perform_sync(trigger_source="webhook", force=True))
    return web.json_response(
        {
            "status": "accepted",
            "message": "Tracker sync initiated successfully.",
            "source": "webhook"
        },
        status=202
    )


async def handle_health(request: web.Request) -> web.Response:
    """
    HTTP GET /health or GET / endpoint.
    Returns liveness status and execution metadata.
    """
    return web.json_response(
        {
            "status": "ok",
            "state": RUNNER_STATE
        }
    )


def compute_seconds_until_schedule(target_hour: int, target_minute: int) -> tuple[float, datetime]:
    """
    Calculates seconds remaining until the next occurrence of target UTC time.
    """
    now = datetime.now(timezone.utc)
    target_today = datetime.combine(now.date(), time(target_hour, target_minute), tzinfo=timezone.utc)
    if now >= target_today:
        target_next = target_today + timedelta(days=1)
    else:
        target_next = target_today

    sleep_seconds = (target_next - now).total_seconds()
    return max(0.0, sleep_seconds), target_next


async def daily_scheduler_loop(target_hour: int = 10, target_minute: int = 15):
    """
    Scheduler background loop running once daily at target UTC time.
    """
    logger.info("Daily scheduler loop active. Configured target time: %02d:%02d UTC", target_hour, target_minute)
    while True:
        try:
            sleep_sec, next_run = compute_seconds_until_schedule(target_hour, target_minute)
            RUNNER_STATE["next_scheduled_utc"] = next_run.isoformat()
            logger.info("Next scheduled run at %s UTC (in %.1f hours / %.0f seconds)",
                        next_run.strftime("%Y-%m-%d %H:%M:%S"), sleep_sec / 3600, sleep_sec)
            
            await asyncio.sleep(sleep_sec)
            logger.info("Daily scheduled trigger time reached (%02d:%02d UTC). Starting sync...", target_hour, target_minute)
            await perform_sync(trigger_source="scheduler", force=False)
            
            # Prevent re-trigger within the same minute
            await asyncio.sleep(65)
        except asyncio.CancelledError:
            logger.info("Daily scheduler loop cancelled.")
            break
        except Exception as exc:
            logger.error("Error in scheduler loop: %s. Retrying in 60 seconds...", exc)
            await asyncio.sleep(60)


async def start_background_tasks(app: web.Application):
    """
    Initializes daily scheduler task on web application startup.
    """
    sched_cfg = os.getenv("SCHEDULE_UTC_TIME", "10:15")
    try:
        parts = sched_cfg.strip().split(":")
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        hour, minute = 10, 15

    app["scheduler_task"] = asyncio.create_task(daily_scheduler_loop(hour, minute))


async def cleanup_background_tasks(app: web.Application):
    """
    Cleanly cancels scheduler background task on shutdown.
    """
    task = app.get("scheduler_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def init_app() -> web.Application:
    """
    Constructs the aiohttp application with routes and lifecycle hooks.
    """
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/trigger", handle_trigger)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    logger.info("Starting Uma Tracker Daemon on port %d...", port)
    app = init_app()
    web.run_app(app, host="0.0.0.0", port=port, print=None)
