import asyncio
import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict
import asyncpg

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UmaSync")

# Cache for circle_id -> club_uuid mapping to avoid redundant DB lookups
_uuid_cache: Dict[str, str] = {}

async def get_db_connection():
    """Establish connection to the Azure VM Postgres database"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set. Sync skipped.")
        return None
    
    try:
        # Connect using asyncpg
        conn = await asyncpg.connect(database_url)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

async def get_club_uuid(conn: asyncpg.Connection, circle_id: str) -> Optional[str]:
    """Lookup the internal UmaCore UUID for a given circle_id"""
    if circle_id in _uuid_cache:
        return _uuid_cache[circle_id]
    
    try:
        query = "SELECT club_id FROM clubs WHERE circle_id = $1"
        row = await conn.fetchrow(query, circle_id)
        if row:
            uuid = str(row['club_id'])
            _uuid_cache[circle_id] = uuid
            return uuid
        else:
            logger.warning(f"No club found in UmaCore database for circle_id: {circle_id}")
            return None
    except Exception as e:
        logger.error(f"Error looking up club UUID: {e}")
        return None

async def sync_raw_json_to_db(circle_id: str, raw_data: any):
    """
    Push raw scraped JSON to the UmaCore database so the bot can 'claim' it.
    This bypasses the bot's IP-banned scraper.
    """
    if not raw_data:
        logger.warning(f"No data to sync for club {circle_id}")
        return

    conn = await get_db_connection()
    if not conn:
        return

    try:
        # 1. Get the UUID
        club_uuid = await get_club_uuid(conn, circle_id)
        if not club_uuid:
            return

        # 2. Prepare data
        today = datetime.now().date()
        
        # Ensure raw_data is a dict (if it was a string from Chrono)
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode raw JSON string for {circle_id}")
                return

        # 3. Insert/Upsert into raw_scraped_data
        # Note: raw_scraped_data table was added to the bot project in the previous step
        query = """
            INSERT INTO raw_scraped_data (club_id, date, raw_json)
            VALUES ($1, $2, $3)
            ON CONFLICT (club_id, date) 
            DO UPDATE SET 
                raw_json = $3,
                created_at = NOW()
        """
        
        await conn.execute(query, club_uuid, today, json.dumps(raw_data))
        logger.info(f"✅ Successfully synced raw JSON for {circle_id} to UmaCore DB")

    except Exception as e:
        logger.error(f"Failed to sync data for {circle_id}: {e}")
    finally:
        await conn.close()
