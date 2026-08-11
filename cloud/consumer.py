import redis
import sqlite3
import json
import time
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
DB_PATH = os.environ.get("DB_PATH", "events.db")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

conn = sqlite3.connect(DB_PATH)

conn.execute('''CREATE TABLE IF NOT EXISTS events
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             event_type TEXT NOT NULL,
             device_id TEXT NOT NULL,
             class TEXT NOT NULL,
             confidence REAL NOT NULL,
             frame_path TEXT,
             timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

conn.execute('''CREATE TABLE IF NOT EXISTS telemetry_alerts
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             device_id TEXT NOT NULL,
             metric TEXT NOT NULL,
             value REAL NOT NULL,
             breach_count INTEGER,
             window_size INTEGER,
             status TEXT NOT NULL,
             received_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

conn.commit()

print("Consumer is waiting for events and telemetry alerts...")
while True:
    # BLPOP with multiple keys blocks on whichever queue has data first,
    # so one consumer process can serve both without polling two queues.
    try:
        result = redis_client.blpop(['events_queue', 'telemetry_queue'], timeout=5)
    except redis.exceptions.TimeoutError:
        print("Redis connection timed out. Retrying...")
        time.sleep(1)
        continue

    if result is None:
        continue

    queue_name, raw_payload = result
    payload = json.loads(raw_payload)

    if queue_name == 'events_queue':
        conn.execute('''INSERT INTO events (event_type, device_id, class, confidence, frame_path)
                     VALUES (?, ?, ?, ?, ?)''',
                  (payload['event_type'], payload['device_id'], payload['class'],
                   payload['confidence'], payload.get('frame_path')))
        conn.commit()
        print(f"Event stored in database: {payload}")

    elif queue_name == 'telemetry_queue':
        conn.execute('''INSERT INTO telemetry_alerts
                     (device_id, metric, value, breach_count, window_size, status)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (payload['device_id'], payload['metric'], payload['value'],
                   payload.get('breach_count'), payload.get('window_size'), payload['status']))
        conn.commit()
        print(f"Telemetry alert stored: {payload}")