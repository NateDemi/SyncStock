#!/usr/bin/env python3
"""
Simple webhook server for SyncStock sync operations.
Run with: python webhook_server.py
"""

from flask import Flask, request, jsonify
import subprocess
import json
import logging
from datetime import date
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

def validate_date(date_str: str) -> Optional[date]:
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        return None

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "SyncStock Webhook Server"})

@app.route('/sync', methods=['POST'])
def trigger_sync():
    try:
        # Get payload
        if request.is_json:
            payload = request.get_json()
            start_date = payload.get("start_date") if payload else None
            force_refresh = payload.get("force_refresh", False) if payload else False
        else:
            start_date = request.form.get("start_date") or request.data.decode('utf-8').strip()
            force_refresh = False
        
        logger.info(f"Webhook received - start_date: {start_date}, force_refresh: {force_refresh}")
        
        # Validate start_date if provided
        if start_date and start_date != "":
            parsed_date = validate_date(start_date)
            if not parsed_date:
                return jsonify({"error": "Invalid date format", "expected": "ISO date format (YYYY-MM-DD)"}), 400
            
            payload_arg = start_date if force_refresh else json.dumps({"start_date": start_date})
        else:
            payload_arg = ""
        
        # Run sync
        logger.info(f"Executing sync with payload: {payload_arg}")
        
        cmd = ["python", "syncstock.py"]
        if payload_arg:
            cmd.append(payload_arg)
            
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("Sync completed successfully")
            return jsonify({
                "status": "success",
                "message": "Sync completed successfully",
                "start_date": start_date if start_date else "default"
            })
        else:
            logger.error(f"Sync failed with return code {result.returncode}")
            return jsonify({
                "status": "error",
                "message": "Sync failed",
                "return_code": result.returncode,
                "stderr": result.stderr
            }), 500
            
    except subprocess.TimeoutExpired:
        logger.error("Sync operation timed out")
        return jsonify({"status": "error", "message": "Sync operation timed out"}), 408
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/sync/status', methods=['GET'])
def sync_status():
    try:
        from db import conn_cursor
        
        with conn_cursor() as (conn, cur):
            cur.execute("SELECT * FROM syncstock.meta WHERE id = TRUE")
            meta = cur.fetchone()
            
            if meta:
                return jsonify({
                    "status": "success",
                    "data": {
                        "last_sales_day_done": meta.get("last_sales_day_done"),
                        "run_status": meta.get("run_status"),
                        "updated_at": meta.get("updated_at")
                    }
                })
            else:
                return jsonify({"status": "error", "message": "No meta data found"}), 404
                
    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting SyncStock Webhook Server on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
