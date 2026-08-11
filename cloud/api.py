from flask import Flask, jsonify, request
import redis
import json
import sqlite3

import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
DB_PATH = os.environ.get("DB_PATH", "events.db")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

app = Flask(__name__)

# connect the redis container
# testing locally, redis as the hostname, 6379 as the port number


@app.route('/events', methods=['POST'])

def receive_event():

    event = request.get_json()
    if not event or "event_type" not in event or "confidence" not in event:
        return jsonify({"error": "Invalid event data"}), 400

    # push the event data to the redis list as json string
    # LPUSH command adds the event data to the left of the list, so the most recent events are at the front of the list,        
    # Process the event data and store it in Redis
    redis_client.lpush("events_queue", json.dumps(event))
    return jsonify({"status": "queued"}), 202

def get_db():
    conn = sqlite3.connect(DB_PATH)
    return conn

@app.route('/metrics', methods=['GET'])
def metrics():
    conn = get_db()
    c = conn.cursor()

    total_events = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    by_type = c.execute("SELECT event_type, COUNT(*) FROM events GROUP BY event_type").fetchall()
    alarms_by_class = c.execute("SELECT class, COUNT(*) FROM events WHERE event_type = 'alarm' GROUP BY class").fetchall()
    backlog = redis_client.llen("events_queue")


    conn.close()
    return jsonify({
        "total_events": total_events,
        "backlog": backlog,
        "events_by_type": {event_type: count for event_type, count in by_type},
        "alarms_by_class": {class_name: count for class_name, count in alarms_by_class}
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
