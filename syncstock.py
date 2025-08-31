from datetime import date, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import json
import sys
import logging
from db import conn_cursor, execute_values
import query as Q

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def pick_window(cur, user_lookback_start: Optional[date]) -> Tuple[date, date]:
    logger.debug("Starting pick_window function")
    logger.debug(f"User lookback start: {user_lookback_start}")
    
    cur.execute(Q.sql_now()); nowrow = cur.fetchone(); today = nowrow["today"]
    logger.debug(f"Current database date: {today}")
    
    cur.execute(Q.sql_get_last_sales_day()); r = cur.fetchone() or {}
    last_done: Optional[date] = r.get("d")
    logger.debug(f"Last sales day done: {last_done}")

    if user_lookback_start:
        start = user_lookback_start
        logger.debug(f"Using user-specified start date: {start}")
    else:
        # if we've never run, default to 30-day lookback; else resume from last_done+1
        if last_done:
            start = last_done + timedelta(days=1)
            logger.debug(f"Resuming from last done + 1 day: {start}")
        else:
            start = today - timedelta(days=30)
            logger.debug(f"No previous run, using 30-day lookback: {start}")

    # Include today in the window; end is exclusive
    end = today + timedelta(days=1)
    logger.info(f"Processing window: {start} to {end} ({(end - start).days} days)")
    return start, end

def fetch_daily(cur, start: date, end: date):
    logger.debug(f"Fetching daily data from {start} to {end}")
    
    logger.debug("Executing daily purchases query")
    cur.execute(Q.sql_daily_purchases(), (start, end)); p = list(cur.fetchall())
    logger.debug(f"Found {len(p)} purchase records")
    
    logger.debug("Executing daily sales query")
    cur.execute(Q.sql_daily_sales(), (start, end)); s = list(cur.fetchall())
    logger.debug(f"Found {len(s)} sales records")
    
    # Validate that all inventory IDs exist in inventory_items table
    logger.debug("Validating inventory IDs against inventory_items table")
    
    # Get all unique inventory IDs from both datasets
    all_ids = set()
    for row in p:
        if row["inventory_id"]:
            all_ids.add(row["inventory_id"])
    for row in s:
        if row["inventory_id"]:
            all_ids.add(row["inventory_id"])
    
    logger.debug(f"Found {len(all_ids)} unique inventory IDs to validate")
    
    # Check which IDs exist in inventory_items
    if all_ids:
        # Convert to list for SQL IN clause
        id_list = list(all_ids)
        # Split into chunks to avoid SQL parameter limits
        chunk_size = 1000
        valid_ids = set()
        
        for i in range(0, len(id_list), chunk_size):
            chunk = id_list[i:i + chunk_size]
            placeholders = ','.join(['%s'] * len(chunk))
            cur.execute(f"SELECT id FROM store_data.inventory_items WHERE id IN ({placeholders})", chunk)
            chunk_valid = {row['id'] for row in cur.fetchall()}
            valid_ids.update(chunk_valid)
        
        logger.info(f"Validation complete: {len(valid_ids)} valid IDs out of {len(all_ids)} total")
        
        # Filter out invalid records
        p_filtered = [row for row in p if row["inventory_id"] in valid_ids]
        s_filtered = [row for row in s if row["inventory_id"] in valid_ids]
        
        invalid_count = len(p) + len(s) - len(p_filtered) - len(s_filtered)
        if invalid_count > 0:
            logger.warning(f"Filtered out {invalid_count} records with invalid inventory IDs")
        
        return p_filtered, s_filtered
    
    return p, s

def merge_daily(p_rows: List[dict], s_rows: List[dict]):
    logger.debug("Merging daily purchase and sales data")
    
    by = defaultdict(lambda: {"p":0, "s":0})
    items = set()
    
    logger.debug(f"Processing {len(p_rows)} purchase rows")
    for r in p_rows:
        d, iid = r["day"], str(r["inventory_id"])
        # Handle NULL values safely
        raw_qty = r["purchased_qty"]
        if raw_qty is None:
            logger.warning(f"NULL purchased_qty found for {iid} on {d}, treating as 0")
            qty = 0
        else:
            qty = int(raw_qty)
        
        by[(d,iid)]["p"] += qty
        items.add(iid)
        if qty > 0:
            logger.debug(f"Purchase: {d} - {iid} = +{qty}")
    
    logger.debug(f"Processing {len(s_rows)} sales rows")
    for r in s_rows:
        d, iid = r["day"], str(r["inventory_id"])
        # Handle NULL values safely
        raw_qty = r["sold_qty"]
        if raw_qty is None:
            logger.warning(f"NULL sold_qty found for {iid} on {d}, treating as 0")
            qty = 0
        else:
            qty = int(raw_qty)
        
        by[(d,iid)]["s"] += qty
        items.add(iid)
        if qty > 0:
            logger.debug(f"Sale: {d} - {iid} = -{qty}")
    
    logger.info(f"Merged data: {len(by)} unique (day, item) combinations, {len(items)} unique items")
    return by, sorted(items)

def opening_balances(cur, start: date, items: List[str]) -> Dict[str,int]:
    logger.debug(f"Getting opening balances for {len(items)} items from {start}")
    
    if not items:
        logger.debug("No items to get opening balances for")
        return {}
    
    cur.execute(Q.sql_opening_on_hand_prev_day(), (start, items))
    balances = {str(r["inventory_id"]): int(r["on_hand_end"]) for r in cur.fetchall()}
    
    logger.debug(f"Found opening balances for {len(balances)} items")
    for item_id, balance in list(balances.items())[:5]:  # Log first 5 for debugging
        logger.debug(f"Opening balance: {item_id} = {balance}")
    
    return balances

def roll_forward(start: date, end: date, items: List[str], by: dict, opening: Dict[str,int]):
    logger.debug(f"Rolling forward balances from {start} to {end} for {len(items)} items")
    
    rows = []
    on_hand = {iid: opening.get(iid, 0) for iid in items}
    
    logger.debug(f"Initial on-hand balances: {sum(on_hand.values())} total across {len(on_hand)} items")
    
    day = start
    day_count = 0
    while day < end:
        day_count += 1
        logger.debug(f"Processing day {day_count}: {day}")
        
        daily_changes = 0
        for iid in items:
            p = by.get((day, iid), {}).get("p", 0)
            s = by.get((day, iid), {}).get("s", 0)
            old_balance = on_hand[iid]
            on_hand[iid] = old_balance + p - s
            
            if p > 0 or s > 0:
                logger.debug(f"  {iid}: {old_balance} + {p} - {s} = {on_hand[iid]}")
                daily_changes += 1
            
            rows.append((day, iid, p, s, on_hand[iid]))
        
        if daily_changes > 0:
            logger.debug(f"Day {day}: {daily_changes} items had activity")
        
        day += timedelta(days=1)
    
    logger.info(f"Rolled forward {day_count} days, created {len(rows)} ledger rows")
    logger.debug(f"Final on-hand balances: {sum(on_hand.values())} total across {len(on_hand)} items")
    
    return rows

def run_daily_rollup(user_lookback_start: Optional[date] = None, is_webhook: bool = False):
    """
    Run the daily rollup sync.
    
    Args:
        user_lookback_start: Optional date to start from (for webhook calls)
        is_webhook: Whether this is triggered by a webhook
    """
    start_time = date.today()
    logger.info("🚀 Starting SyncStock daily rollup...")
    logger.info(f"   Start time: {start_time}")
    logger.info(f"   Webhook triggered: {is_webhook}")
    
    if user_lookback_start:
        logger.info(f"   User requested start date: {user_lookback_start}")
    else:
        logger.info("   Using default behavior (resume from watermark or 30-day lookback)")
    
    with conn_cursor() as (conn, cur):
        logger.debug("Database connection established")
        
        # Prevent overlapping runs with advisory lock
        logger.debug("Acquiring advisory lock to prevent overlapping runs")
        cur.execute("SELECT pg_try_advisory_lock( hashtext('syncstock-runlock') ) AS got")
        lock_result = cur.fetchone() or {}
        if not lock_result.get("got"):
            logger.warning("Another SyncStock run is active; skipping.")
            return
        
        logger.debug("Advisory lock acquired successfully")
        
        try:
            start, end = pick_window(cur, user_lookback_start)
            logger.info(f"   Processing date range: {start} to {end}")
            
            if start >= end:
                logger.info("   No new data to process")
                return
            
            logger.debug("Fetching daily aggregates")
            p_rows, s_rows = fetch_daily(cur, start, end)
            logger.info(f"   Found {len(p_rows)} purchase records, {len(s_rows)} sales records")
            
            logger.debug("Merging daily data")
            by, items = merge_daily(p_rows, s_rows)
            
            logger.debug("Getting opening balances")
            opening = opening_balances(cur, start, items)
            
            logger.debug("Rolling forward balances")
            ledger_rows = roll_forward(start, end, items, by, opening)
            
            logger.info(f"   Creating {len(ledger_rows)} daily ledger entries")
            
            logger.debug("Starting database transaction")
            conn.execute("BEGIN")
            
            try:
                if ledger_rows:
                    logger.debug("Inserting ledger records")
                    execute_values(cur, Q.sql_upsert_ledger(), ledger_rows)
                    logger.info(f"   ✅ Inserted {len(ledger_rows)} ledger records")
                    
                    # mark watermark to last processed day
                    watermark_date = end - timedelta(days=1)
                    logger.debug(f"Advancing watermark to {watermark_date}")
                    cur.execute(Q.sql_advance_sales_day_watermark(), (watermark_date,))
                    logger.info(f"   ✅ Advanced watermark to {watermark_date}")
                    
                    # refresh current stock from latest day
                    logger.debug("Updating current stock from latest day")
                    cur.execute(Q.sql_upsert_stock_from_latest_day())
                    logger.info(f"   ✅ Updated current stock from latest day")
                else:
                    # nothing moved; still advance watermark so we don't reprocess idle days
                    watermark_date = end - timedelta(days=1)
                    logger.debug(f"No data to process, advancing watermark to {watermark_date}")
                    cur.execute(Q.sql_advance_sales_day_watermark(), (watermark_date,))
                    logger.info(f"   ✅ Advanced watermark (no data to process)")
                
                logger.debug("Committing transaction")
                conn.commit()
                logger.info("   ✅ Transaction committed successfully")
                
            except Exception as e:
                logger.error(f"   ❌ Error during transaction: {e}")
                logger.debug("Rolling back transaction")
                conn.rollback()
                logger.info("   🔄 Transaction rolled back")
                raise
        finally:
            # Always release the advisory lock
            logger.debug("Releasing advisory lock")
            cur.execute("SELECT pg_advisory_unlock( hashtext('syncstock-runlock') )")
    
    end_time = date.today()
    duration = end_time - start_time
    logger.info(f"🏁 Daily rollup completed in {duration}")

def parse_webhook_payload(payload: str) -> Optional[date]:
    """
    Parse webhook payload to extract optional start date.
    
    Expected payload format:
    - Empty string: use default behavior (resume from watermark or 30-day lookback)
    - JSON: {"start_date": "2025-08-01"} or {"start_date": null}
    - Date string: "2025-08-01"
    """
    logger.debug(f"Parsing webhook payload: {payload}")
    
    if not payload or payload.strip() == "":
        logger.debug("Empty payload, using default behavior")
        return None
    
    try:
        # Try to parse as JSON first
        data = json.loads(payload)
        logger.debug(f"Parsed as JSON: {data}")
        
        if isinstance(data, dict) and "start_date" in data:
            start_date_str = data["start_date"]
            if start_date_str:
                parsed_date = date.fromisoformat(start_date_str)
                logger.debug(f"Extracted start_date from JSON: {parsed_date}")
                return parsed_date
            else:
                logger.debug("start_date is null in JSON, using default behavior")
                return None
    except json.JSONDecodeError as e:
        logger.debug(f"Not valid JSON: {e}")
    
    try:
        # Try to parse as direct date string
        parsed_date = date.fromisoformat(payload.strip())
        logger.debug(f"Parsed as direct date string: {parsed_date}")
        return parsed_date
    except ValueError as e:
        logger.debug(f"Not valid date string: {e}")
    
    logger.warning(f"⚠️  Could not parse webhook payload: {payload}")
    logger.warning(f"   Expected formats: empty string, JSON with 'start_date', or ISO date string")
    return None

if __name__ == "__main__":
    # Check if this is a webhook call with payload
    if len(sys.argv) > 1:
        payload = sys.argv[1]
        logger.info(f"📅 Webhook payload received: {payload}")
        start_date = parse_webhook_payload(payload)
        if start_date is not None:
            logger.info(f"📅 Webhook requested start date: {start_date}")
        else:
            logger.info("📅 Using default behavior for webhook")
        run_daily_rollup(user_lookback_start=start_date, is_webhook=True)
    else:
        # Default behavior (for GitHub Actions or manual runs)
        logger.info("🔄 Running default sync (no webhook payload)")
        run_daily_rollup(user_lookback_start=None, is_webhook=False)