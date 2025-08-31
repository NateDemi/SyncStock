# db.py
import os
import logging
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

# Configure logging
logger = logging.getLogger(__name__)

load_dotenv()
PG_DSN = os.getenv("PG_DSN")

def _require_dsn():
    if not PG_DSN:
        logger.error("PG_DSN environment variable not set")
        raise RuntimeError("PG_DSN not set")
    logger.debug(f"Database connection string configured: {PG_DSN[:20]}...")

@contextmanager
def conn_cursor():
    """Yields (conn, cur) with dict rows; caller controls BEGIN/COMMIT."""
    logger.debug("Creating database connection and cursor")
    _require_dsn()
    
    try:
        with psycopg.connect(PG_DSN, row_factory=dict_row) as conn:
            logger.debug("Database connection established")
            with conn.cursor() as cur:
                logger.debug("Database cursor created")
                yield conn, cur
                logger.debug("Database cursor and connection closed")
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def execute_values(cur, sql: str, rows, page_size: int = 1000):
    """Use psycopg's native execute_values with proper chunking"""
    if not rows:
        logger.debug("No rows to insert, skipping execute_values")
        return
    
    logger.debug(f"Executing bulk insert with {len(rows)} rows using page_size {page_size}")
    logger.debug(f"SQL template: {sql}")
    
    try:
        # Try to use psycopg's native execute_values if available
        from psycopg.extras import execute_values as _ev
        logger.debug("Using psycopg's native execute_values")
        _ev(cur, sql, rows, page_size=page_size)
        logger.info(f"Successfully inserted {len(rows)} rows using native execute_values")
        
    except ImportError:
        # Fallback to custom implementation if psycopg.extras not available
        logger.debug("psycopg.extras not available, using custom implementation")
        
        # Find the VALUES clause and replace only the %s there
        if 'VALUES %s' in sql:
            total_batches = (len(rows) + page_size - 1) // page_size
            logger.info(f"Processing {len(rows)} rows in {total_batches} batches of {page_size}")
            
            for i in range(0, len(rows), page_size):
                batch = rows[i:i + page_size]
                batch_num = (i // page_size) + 1
                
                logger.debug(f"Processing batch {batch_num}/{total_batches} with {len(batch)} rows")
                
                # Create proper placeholders for this batch
                placeholders = ','.join(['(' + ','.join(['%s'] * len(batch[0])) + ')'] * len(batch))
                batch_sql = sql.replace('VALUES %s', f'VALUES {placeholders}')
                
                logger.debug(f"Batch {batch_num} SQL: {batch_sql[:100]}...")
                
                # Flatten the batch rows for execution
                flat_rows = [item for row in batch for item in row]
                logger.debug(f"Batch {batch_num} flattened to {len(flat_rows)} parameters")
                
                cur.execute(batch_sql, flat_rows)
                logger.debug(f"Batch {batch_num} executed successfully")
            
            logger.info(f"Successfully inserted {len(rows)} rows in {total_batches} batches using custom implementation")
        else:
            logger.debug("No VALUES clause found, executing rows individually")
            # Fallback: execute each row individually
            for i, row in enumerate(rows):
                logger.debug(f"Executing row {i+1}/{len(rows)}: {row}")
                cur.execute(sql, row)
            
            logger.info(f"Successfully inserted {len(rows)} rows individually")