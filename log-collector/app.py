import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from datetime import datetime
 
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
 
 
class Config:
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "logsdb")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "logs_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "logs_pass")
 
    # Splunk
    SPLUNK_HEC_URL = os.getenv("SPLUNK_HEC_URL")
    SPLUNK_TOKEN = os.getenv("SPLUNK_TOKEN")
    SPLUNK_SOURCETYPE = os.getenv("SPLUNK_SOURCETYPE", "_json")
 
    ALLOWED_LEVELS = ("ERROR", "WARNING", "INFO", "DEBUG")
 
 
def get_conn():
    return psycopg2.connect(
        host=Config.POSTGRES_HOST,
        port=Config.POSTGRES_PORT,
        dbname=Config.POSTGRES_DB,
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASSWORD
    )
 
 
def init_db():
    sql = """
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        event_id TEXT UNIQUE,
        level TEXT,
        message TEXT,
        client_name TEXT,
        timestamp TIMESTAMP WITH TIME ZONE
    );
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
 
 
def normalize_level(level):
    if not level:
        return "INFO"
    level = level.strip().upper()
    return level if level in Config.ALLOWED_LEVELS else "INFO"
 
 
def send_to_splunk(event):
    if not Config.SPLUNK_HEC_URL or not Config.SPLUNK_TOKEN:
        logging.warning("Splunk HEC not configured, skipping...")
        return
 
    headers = {
        "Authorization": f"Splunk {Config.SPLUNK_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "time": event.get("timestamp", datetime.utcnow().isoformat()),
        "host": event.get("client_name", "unknown"),
        "source": "python_log_collector",
        "sourcetype": "_json",
        "event": event
    }
 
    try:
        resp = requests.post(
            Config.SPLUNK_HEC_URL,
            json=payload,
            headers=headers,
            verify=False,
            timeout=5
        )
        if resp.status_code not in (200, 201):
            logging.error("Splunk HEC error: %s", resp.text)
    except Exception as e:
        logging.error("Error sending to Splunk: %s", e)
 
 
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200
 
 
@app.route("/collect", methods=["POST"])
def collect():
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid payload"}), 400
 
    event_id = event.get("event_id") or str(int(time.time() * 1000))
    level = normalize_level(event.get("level"))
    message = event.get("message", "")
    client_name = event.get("client_name", "unknown")
    ts = event.get("timestamp")
 
    try:
        timestamp = datetime.fromisoformat(ts) if ts else datetime.utcnow()
    except Exception:
        timestamp = datetime.utcnow()
 
    # Save to Postgres
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO logs (event_id, level, message, client_name, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (event_id, level, message, client_name, timestamp))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("DB error: %s", e)
        return jsonify({"error": str(e)}), 500
 
    # Send to Splunk
    send_to_splunk({
        "event_id": event_id,
        "level": level,
        "message": message,
        "client_name": client_name,
        "timestamp": timestamp.isoformat()
    })
 
    return jsonify({"status": "ok"}), 200
 
 
@app.route("/logs", methods=["GET"])
def get_logs():
    limit = int(request.args.get("limit", "500"))
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT event_id, level, message, client_name, timestamp
            FROM logs
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("DB fetch error: %s", e)
        return jsonify({"error": str(e)}), 500
 
    for r in rows:
        r["timestamp"] = r["timestamp"].isoformat()
    return jsonify({"logs": rows}), 200
 
 
@app.route("/analyze", methods=["GET"])
def analyze():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT level, COUNT(*) FROM logs GROUP BY level;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("DB analyze error: %s", e)
        return jsonify({"error": str(e)}), 500
 
    stats = {r[0]: r[1] for r in rows}
    return jsonify({"counts": stats}), 200
 
 
if __name__ == "__main__":
    for i in range(10):
        try:
            init_db()
            logging.info("Database ready ✅")
            break
        except Exception as e:
            logging.warning("Waiting for Postgres... (%s)", e)
            time.sleep(2)
    app.run(host="0.0.0.0", port=5002)
