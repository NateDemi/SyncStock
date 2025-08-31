# query.py
import logging

# Configure logging
logger = logging.getLogger(__name__)

# --- meta / watermarks ---
def sql_get_last_sales_day():
    logger.debug("Generating SQL: get last sales day watermark")
    return "SELECT last_sales_day_done AS d FROM syncstock.meta WHERE id=TRUE"

def sql_set_status():
    logger.debug("Generating SQL: set status")
    return "UPDATE syncstock.meta SET run_status=%s, notes=COALESCE(%s, notes), updated_at=now() WHERE id=TRUE"

def sql_advance_sales_day_watermark():
    logger.debug("Generating SQL: advance sales day watermark")
    # set watermark to the last fully processed day
    return "UPDATE syncstock.meta SET last_sales_day_done=%s, run_status='SUCCESS', updated_at=now() WHERE id=TRUE"

def sql_now():
    logger.debug("Generating SQL: get current time and date")
    return "SELECT now() AS now, current_date AS today"

# --- daily aggregates in [start_day, end_day) ---
def sql_daily_purchases():
    logger.debug("Generating SQL: daily purchases aggregation")
    # uses vendor receipt/purchase date
    return """
    SELECT DATE(vp.purchase_date) AS day,
           ii.id::text           AS inventory_id,
           SUM(li.quantity)::int AS purchased_qty
    FROM store_data.vendor_purchases vp
    JOIN store_data.vendor_purchases_line_items li ON li.docupanda_id = vp.docupanda_id
    LEFT JOIN store_data.vendor_items vi ON li.upc = vi.receipt_upc
    LEFT JOIN store_data.item_mapping im ON vi.id = im.vendor_item_id
    LEFT JOIN store_data.inventory_items ii ON ii.id = im.inventory_item_id
    WHERE ii.id IS NOT NULL
      AND vp.purchase_date >= %s AND vp.purchase_date < %s
    GROUP BY 1,2
    ORDER BY 1,2
    """

def sql_daily_sales():
    logger.debug("Generating SQL: daily sales aggregation")
    # uses sales order created time
    return """
    SELECT DATE(so.clientcreatedtime) AS day,
           sol.item_id::text          AS inventory_id,
           SUM(COALESCE(NULLIF(sol.unitqty,0), sol.quantity)
               * CASE WHEN COALESCE(sol.refunded, FALSE) THEN -1 ELSE 1 END)::int AS sold_qty
    FROM store_data.sales_orders_line_items sol
    JOIN store_data.sales_orders so ON so.id = sol.order_id
    WHERE sol.item_id IS NOT NULL
      AND so.clientcreatedtime >= %s AND so.clientcreatedtime < %s
    GROUP BY 1,2
    ORDER BY 1,2
    """

# --- opening balance for the first day (yesterday's closing) ---
def sql_opening_on_hand_prev_day():
    logger.debug("Generating SQL: get opening balances from previous day")
    return """
    SELECT inventory_id, on_hand_end
    FROM syncstock.ledger
    WHERE order_created_date = %s::date - INTERVAL '1 day'
      AND inventory_id = ANY(%s::text[])
    """

# --- writers ---
def sql_upsert_ledger():
    logger.debug("Generating SQL: upsert daily ledger entries")
    return """
    INSERT INTO syncstock.ledger (order_created_date, inventory_id, purchased_qty, sold_qty, on_hand_end)
    VALUES %s
    ON CONFLICT (order_created_date, inventory_id) DO UPDATE
    SET purchased_qty = EXCLUDED.purchased_qty,
        sold_qty      = EXCLUDED.sold_qty,
        on_hand_end   = EXCLUDED.on_hand_end,
        computed_at   = now()
    """

def sql_upsert_stock_from_latest_day():
    logger.debug("Generating SQL: update current stock from latest ledger day")
    # take the latest processed day's on_hand_end as current stock
    return """
    INSERT INTO syncstock.stock (inventory_id, on_hand, updated_at, version)
    SELECT l.inventory_id,
           l.on_hand_end,
           now(),
           COALESCE(s.version,0)+1
    FROM syncstock.ledger l
    JOIN (SELECT MAX(order_created_date) AS d FROM syncstock.ledger) last ON true
    LEFT JOIN syncstock.stock s ON s.inventory_id = l.inventory_id
    WHERE l.order_created_date = last.d
    ON CONFLICT (inventory_id) DO UPDATE
    SET on_hand   = EXCLUDED.on_hand,
        updated_at= now(),
        version   = syncstock.stock.version + 1
    """