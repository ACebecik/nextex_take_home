from flask import Flask, jsonify, request, send_from_directory
import redis
import json
import sqlite3
import os
import uuid

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
DB_PATH = os.environ.get("DB_PATH", "events.db")
FRAMES_DIR = os.environ.get("FRAMES_DIR", "frames")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

app = Flask(__name__)

# connect the redis container
# testing locally, redis as the hostname, 6379 as the port number


@app.route('/events', methods=['POST'])
def receive_event():
    frame_path = None

    # multipart/form-data (new_class events with an attached frame) vs plain JSON (alarms)
    if request.files:
        event = request.form.to_dict()
        frame = request.files.get('frame')
        if frame:
            os.makedirs(FRAMES_DIR, exist_ok=True)
            filename = f"{uuid.uuid4().hex}_{frame.filename}"
            frame_path = os.path.join(FRAMES_DIR, filename)
            frame.save(frame_path)
    else:
        event = request.get_json()

    if not event or "event_type" not in event or "confidence" not in event:
        return jsonify({"error": "Invalid event data"}), 400

    event["confidence"] = float(event["confidence"])  # form fields arrive as strings
    if frame_path:
        event["frame_path"] = frame_path

    # push the event data to the redis list as json string
    # LPUSH adds it to the left of the list; BRPOP on the consumer side reads from the right
    redis_client.lpush("events_queue", json.dumps(event))
    return jsonify({"status": "queued", "frame_saved": frame_path is not None}), 202


@app.route('/frames/<path:filename>', methods=['GET'])
def get_frame(filename):
    """Lets a retraining job (or a future dashboard) pull back a previously uploaded frame."""
    return send_from_directory(FRAMES_DIR, filename)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    return conn


@app.route('/metrics', methods=['GET'])
def metrics():
    conn = get_db()
    c = conn.cursor()

    total_events = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    by_type = c.execute("SELECT event_type, COUNT(*) FROM events GROUP BY event_type").fetchall()
    alarms_by_class = c.execute(
        "SELECT class, COUNT(*) FROM events WHERE event_type = 'alarm' GROUP BY class"
    ).fetchall()
    frames_captured = c.execute(
        "SELECT COUNT(*) FROM events WHERE frame_path IS NOT NULL"
    ).fetchone()[0]
    backlog = redis_client.llen("events_queue")

    conn.close()
    return jsonify({
        "total_events": total_events,
        "backlog": backlog,
        "events_by_type": {event_type: count for event_type, count in by_type},
        "alarms_by_class": {class_name: count for class_name, count in alarms_by_class},
        "frames_captured": frames_captured
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)