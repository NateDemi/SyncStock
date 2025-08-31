# SyncStock

A robust, set-based system for maintaining accurate current stock levels by combining operational deltas (purchases/sales) with manual POS stock counts as the final truth.

## Overview

SyncStock solves the common inventory synchronization problem by:

1. **Combining operational data** (purchases add, sales subtract) with **manual counts** (POS stock counts as truth)
2. **Using set-based operations** to avoid loops and minimize database round trips
3. **Maintaining watermarks** for incremental processing and idempotent operations
4. **Providing audit trails** through an optional ledger system
5. **Supporting daily snapshots** for reporting and reconciliation

## Design Principles

- **Manual counts override computed counts** - POS stock counts become the authoritative truth for each run
- **Idempotent operations** - Rerunning for the same time window yields identical results
- **Set-based processing** - All operations use bulk SQL operations, no per-item loops
- **Clean separation** - Stock data is separate from catalog data (inventory_items)
- **Scalable and debuggable** - Optional ledger provides full audit trail

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Purchases     │    │     Sales       │    │  POS Counts     │
│   (add stock)   │    │  (subtract      │    │ (manual truth)  │
│                 │    │   stock)        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   SyncStock     │
                    │   Engine        │
                    │                 │
                    │ 1. Apply deltas │
                    │ 2. Apply POS    │
                    │    overrides    │
                    │ 3. Update       │
                    │    watermarks   │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Outputs       │
                    │                 │
                    │ • item_stock    │
                    │ • ledger        │
                    │ • daily_snaps   │
                    └─────────────────┘
```

## Database Schema

### Core Tables

- **`syncstock.meta`** - Sync status and metadata
- **`syncstock.watermarks`** - Incremental processing watermarks
- **`syncstock.item_stock`** - Authoritative current stock levels
- **`syncstock.inventory_ledger`** - Audit trail of all quantity changes
- **`syncstock.item_stock_daily`** - Daily snapshots for reporting

### Key Features

- **Version tracking** - Each stock update increments a version counter
- **Audit trail** - Complete history of quantity changes with reasons
- **Watermark management** - Tracks last processed timestamp per data source
- **Temp table isolation** - Each run uses isolated temporary tables

## Installation

### 1. Database Setup

```sql
-- Run the schema file
\i schema.sql

-- Run the core implementation
\i syncstock_core.sql
```

### 2. Python Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Verify installation
python syncstock.py
```

## Usage

### Basic Sync Operation

```python
from syncstock import SyncStockDB, PurchaseRecord, SalesRecord, POSRecord, run_syncstock

# Connect to database
db = SyncStockDB("postgresql://user:pass@localhost:5432/dbname")

# Prepare data
purchases = [
    PurchaseRecord(inventory_id=1001, qty=50, day=date.today()),
    PurchaseRecord(inventory_id=1002, qty=25, day=date.today()),
]

sales = [
    SalesRecord(inventory_id=1001, qty=10, day=date.today()),
    SalesRecord(inventory_id=1002, qty=5, day=date.today()),
]

pos_counts = [
    POSRecord(inventory_id=1001, stock_count=45, counted_at=datetime.now()),
    POSRecord(inventory_id=1002, stock_count=20, counted_at=datetime.now()),
]

# Run sync
result = run_syncstock(db, purchases, sales, pos_counts, "Daily sync")
print(f"Sync completed: {result}")
```

### Direct Database Function Call

```python
# Alternative: Call the database function directly
result = db.run_sync(purchases, sales, pos_counts, "Manual sync")
```

### Monitoring and Status

```python
# Check current status
status = db.get_status()
print(f"Current status: {status}")

# Check watermarks
watermarks = db.get_watermarks()
for wm in watermarks:
    print(f"{wm['source_type']}: {wm['last_processed_at']}")
```

## Data Flow

### 1. Input Processing
- **Purchases**: Positive quantity deltas
- **Sales**: Negative quantity deltas  
- **POS Counts**: Target stock levels (manual truth)

### 2. Delta Application
```sql
-- Apply operational deltas (purchases - sales)
INSERT INTO item_stock (inventory_id, on_hand, version)
SELECT inventory_id, COALESCE(current_on_hand, 0) + delta, version + 1
FROM _delta_ops
ON CONFLICT DO UPDATE SET on_hand = on_hand + delta
```

### 3. POS Override
```sql
-- Apply manual targets as final truth
UPDATE item_stock 
SET on_hand = target, version = version + 1
FROM _targets 
WHERE item_stock.inventory_id = _targets.inventory_id
```

### 4. Watermark Update
- Update `last_processed_at` for each data source
- Mark sync as successful in meta table

## Scheduling

SyncStock is designed to be scheduled as a recurring job:

```bash
# Cron example - run every hour
0 * * * * /usr/bin/python3 /path/to/syncstock.py

# Or use a job scheduler like Airflow, Luigi, etc.
```

## Error Handling

- **Automatic rollback** - Failed operations rollback automatically
- **Status tracking** - Failed runs are marked with ERROR status
- **Error logging** - Full error details stored in meta.notes
- **Temp table cleanup** - Temporary tables are cleaned up even on failure

## Performance Considerations

- **Bulk operations** - All updates use set-based SQL operations
- **Minimal round trips** - Single transaction per sync operation
- **Indexed lookups** - Primary keys and foreign keys are properly indexed
- **Temp table isolation** - Each run uses fresh temporary tables

## Monitoring and Debugging

### Key Metrics
- **Sync frequency** - How often the sync runs
- **Processing time** - Duration of each sync operation
- **Data volumes** - Number of records processed per run
- **Error rates** - Frequency of failed syncs

### Debugging Tools
- **Ledger audit** - Complete history of quantity changes
- **Watermark tracking** - See exactly what data was processed when
- **Daily snapshots** - Historical stock levels for reconciliation
- **Status history** - Track sync success/failure over time

## Extensions and Customization

### Adding New Data Sources
1. Add new watermark type to `syncstock.watermarks`
2. Extend the data models in Python
3. Modify the sync logic to handle new source type

### Custom Business Logic
- Override the `_apply_operational_deltas()` function
- Add validation rules in the Python layer
- Implement custom reconciliation logic

### Reporting and Analytics
- Use `item_stock_daily` for trend analysis
- Query `inventory_ledger` for detailed change history
- Build dashboards on top of the sync results

## Troubleshooting

### Common Issues

1. **Watermark conflicts** - Use `syncstock.reset_watermarks()` for full resync
2. **Data type mismatches** - Ensure inventory_id is BIGINT, quantities are INTEGER
3. **Transaction timeouts** - Break large datasets into smaller batches
4. **Permission errors** - Ensure database user has CREATE TEMP TABLE privileges

### Debug Commands

```sql
-- Check current status
SELECT * FROM syncstock.get_status();

-- View watermarks
SELECT * FROM syncstock.get_watermarks();

-- Check recent ledger entries
SELECT * FROM syncstock.inventory_ledger ORDER BY occurred_at DESC LIMIT 10;

-- Verify stock levels
SELECT * FROM syncstock.item_stock ORDER BY updated_at DESC LIMIT 10;
```

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for any schema changes
4. Ensure all operations remain set-based and idempotent

## License

This project is provided as-is for educational and commercial use.
