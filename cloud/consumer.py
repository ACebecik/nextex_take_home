import redis
import sqlite3
import json
import time
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
DB_PATH = os.environ.get("DB_PATH", "events.db")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

conn = sqlite3.connect(DB_PATH)

c = conn.execute('''CREATE TABLE IF NOT EXISTS events
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             event_type TEXT NOT NULL,
             device_id TEXT NOT NULL,
             class TEXT NOT NULL,
             confidence REAL NOT NULL,
             frame_path TEXT,
             timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

conn.commit()

print("Consumer is waiting for events...")
while True:

    # BRPOP blocks (waits) up to 5 seconds for something to appear
    # Returns None if nothing shows up in that time
    try:
        event_data = redis_client.brpop('events_queue', timeout=5)
    except redis.exceptions.TimeoutError:
        print("Redis connection timed out. Retrying...")
        time.sleep(1)
        continue

    if event_data is None:
        continue

    event_json = event_data[1]  # BRPOP returns (queue_name, value)
    event = json.loads(event_json)

    c.execute('''INSERT INTO events (event_type, device_id, class, confidence, frame_path)
                 VALUES (?, ?, ?, ?, ?)''',
              (event['event_type'], event['device_id'], event['class'],
               event['confidence'], event.get('frame_path')))
    conn.commit()

    print(f"Event stored in database: {event}")