import requests
import time
import random
import json
import os
import uuid
from collections import deque
from PIL import Image, ImageDraw

OUTBOX_FILE = "outbox.json"
CAPTURED_DIR = "captured_frames"        # defected per-frame images, awaiting upload

BASE_URL = "http://localhost:8000"
EVENTS_ENDPOINT = "/events"
TELEMETRY_ENDPOINT = "/telemetry"

CONFIDENCE_THRESHOLD = 0.85
DEVICE_ID = "jetson-01"

POSSIBLE_EVENTS = ["broken_stitch", "needle_mark", "pinched_fabric", "oil_stain", "vertical_lines"]

seen_classes = set()

# --- Telemetry: windowed (K-of-N) alarm detection ------------------------
#
# Only temperature is monitored for now, kept intentionally simple to
# demonstrate the windowing approach clearly. Adding another signal (e.g.
# cpu_load, gpu_load, disk_free_pct) is just one more entry in this dict --
# api.py and consumer.py are already metric-agnostic (they group/store by
# whatever `metric` name arrives), so nothing downstream needs to change.

TELEMETRY_METRICS = {
    "temp_c": {"threshold": 80, "direction": "above", "unit": "C", "normal_range": (35, 55)},
}

WINDOW_SIZE = 10                # how many recent readings we look back over
BREACH_COUNT_THRESHOLD = 6      # K: need at least this many breaches in the window to alarm

# Deterministic demo: force temp_c to breach during these ticks, so the K-of-N
# alarm is guaranteed to fire at least once in a normal test run rather than
# depending on random luck.
STRESS_TEST_METRIC = "temp_c"
STRESS_TEST_TICKS = range(15, 25)

telemetry_windows = {metric: deque(maxlen=WINDOW_SIZE) for metric in TELEMETRY_METRICS}
telemetry_alarm_state = {metric: False for metric in TELEMETRY_METRICS}  # currently alarmed?


def sample_telemetry(tick_number):
    """Fakes one telemetry reading per metric. Mostly stays in a normal range,
    with an occasional isolated spike -- which should NOT alone trigger an
    alarm, since that's exactly what the K-of-N window is meant to filter out."""
    reading = {}
    for metric, cfg in TELEMETRY_METRICS.items():
        low, high = cfg["normal_range"]

        if metric == STRESS_TEST_METRIC and tick_number in STRESS_TEST_TICKS:
            value = round(random.uniform(cfg["threshold"] + 1, cfg["threshold"] + 15), 1)
        elif random.random() < 0.1:
            # isolated spike -- one bad reading, should be absorbed by the window
            if cfg["direction"] == "above":
                value = round(random.uniform(cfg["threshold"] + 1, cfg["threshold"] + 10), 1)
            else:
                value = round(random.uniform(0, max(cfg["threshold"] - 1, 0)), 1)
        else:
            value = round(random.uniform(low, high), 1)

        reading[metric] = value
    return reading


def is_breach(metric, value):
    cfg = TELEMETRY_METRICS[metric]
    return value > cfg["threshold"] if cfg["direction"] == "above" else value < cfg["threshold"]


def check_telemetry_window(metric):
    """K-of-N debounce: alarm only if at least BREACH_COUNT_THRESHOLD of the last
    WINDOW_SIZE readings individually breached this metric's threshold.
    Edge-triggered: only returns a result on state change (new alarm, or
    resolved), not on every tick while the state is unchanged -- otherwise a
    sustained problem would resend the same alarm every loop iteration."""
    window = telemetry_windows[metric]
    if len(window) < WINDOW_SIZE:
        return None  # not enough history yet to judge

    breach_count = sum(1 for v in window if is_breach(metric, v))
    should_alarm = breach_count >= BREACH_COUNT_THRESHOLD
    currently_alarmed = telemetry_alarm_state[metric]

    if should_alarm and not currently_alarmed:
        telemetry_alarm_state[metric] = True
        return ("alarm", breach_count)
    elif not should_alarm and currently_alarmed:
        telemetry_alarm_state[metric] = False
        return ("resolved", breach_count)
    return None


# --- Frame capture / mock inference ---------------------------------------

def capture_frame():
    """Simulates one camera capture -- happens every loop tick, independent of
    what the (mocked) model will later say about it."""
    os.makedirs(CAPTURED_DIR, exist_ok=True)
    capture_path = os.path.join(CAPTURED_DIR, f"frame_{uuid.uuid4().hex}.jpg")

    bg_color = tuple(random.randint(150, 220) for _ in range(3))
    img = Image.new("RGB", (224, 224), color=bg_color)
    draw = ImageDraw.Draw(img)
    for _ in range(random.randint(2, 5)):
        shape_color = tuple(random.randint(0, 150) for _ in range(3))
        x1, y1 = random.randint(0, 180), random.randint(0, 180)
        x2, y2 = x1 + random.randint(10, 40), y1 + random.randint(10, 40)
        if random.random() < 0.5:
            draw.ellipse([x1, y1, x2, y2], fill=shape_color)
        else:
            draw.line([x1, y1, x2, y2], fill=shape_color, width=random.randint(1, 4))

    img.save(capture_path)
    return capture_path


def mock_inference(frame_path):
    """Fakes what the real model would output for a given frame: a class + confidence.
    Takes the frame as input to make the dependency direction explicit, even though
    the mock doesn't actually look at the pixels."""
    error_class = random.choice(POSSIBLE_EVENTS)
    confidence = round(random.uniform(0.7, 1.0), 2)
    return {
        "device_id": DEVICE_ID,
        "class": error_class,
        "confidence": confidence
    }


def cleanup_frame(frame_path):
    """Deletes the local frame copy once the cloud has confirmed receipt, or once
    the frame turned out not to be needed, a real device shouldn't hold onto
    captured images indefinitely due to memory reasons."""
    if frame_path and os.path.exists(frame_path):
        try:
            os.remove(frame_path)
        except OSError as e:
            print(f"Could not clean up frame {frame_path}: {e}")


# --- Sending / buffering (shared by events AND telemetry alarms) ----------

def send_payload(item):
    """Sends one buffered item (event or telemetry alarm) to its endpoint.
    `_endpoint` and `_frame_path` are internal bookkeeping fields, stripped
    before the request is built."""
    endpoint = item.get("_endpoint", EVENTS_ENDPOINT)
    frame_path = item.get("_frame_path")
    payload = {k: v for k, v in item.items() if not k.startswith("_")}
    url = f"{BASE_URL}{endpoint}"

    try:
        if frame_path and os.path.exists(frame_path):
            with open(frame_path, "rb") as f:
                files = {"frame": (os.path.basename(frame_path), f, "image/jpeg")}
                data = {k: str(v) for k, v in payload.items()}
                response = requests.post(url, data=data, files=files, timeout=5)
        else:
            response = requests.post(url, json=payload, timeout=5)
        return response
    except requests.exceptions.RequestException:
        return None


def save_to_outbox(item):
    if not os.path.exists(OUTBOX_FILE):
        with open(OUTBOX_FILE, 'w') as f:
            json.dump([], f)

    with open(OUTBOX_FILE, 'r+') as f:
        outbox = json.load(f)
        outbox.append(item)
        f.seek(0)
        json.dump(outbox, f, indent=4)


def flush_outbox():
    if not os.path.exists(OUTBOX_FILE):
        return

    with open(OUTBOX_FILE, 'r+') as f:
        outbox = json.load(f)
        remaining = []
        resynced = 0

        for item in outbox:
            response = send_payload(item)
            if response is None or response.status_code != 202:
                remaining.append(item)
            else:
                resynced += 1
                cleanup_frame(item.get("_frame_path"))

        if resynced > 0:
            print(f"Resynced {resynced} item(s) from outbox, {len(remaining)} remain in outbox.")

        f.seek(0)
        f.truncate()
        json.dump(remaining, f, indent=4)


# --- Main loop --------------------------------------------------------------

def run(num_frames=100, delay=1):
    for tick in range(num_frames):
        flush_outbox()

        # 1. Defect detection (frame + mock inference)
        frame_path = capture_frame()
        event = mock_inference(frame_path)
        detected_class = event["class"]

        is_new_class = detected_class not in seen_classes
        if is_new_class:
            seen_classes.add(detected_class)

        should_send = is_new_class or event["confidence"] >= CONFIDENCE_THRESHOLD
        event = event | {"event_type": "alarm" if not is_new_class else "new_class"}
        event["_frame_path"] = frame_path if should_send else None
        event["_endpoint"] = EVENTS_ENDPOINT

        if should_send:
            response = send_payload(event)
            if response is None or response.status_code != 202:
                print(f"Failed to send event: {event}")
                save_to_outbox(event)
            else:
                print(f"Sent event: {event}, Response: {response.status_code}")
                cleanup_frame(event.get("_frame_path"))
        else:
            cleanup_frame(frame_path)

        # 2. Telemetry sampling + windowed alarm check
        reading = sample_telemetry(tick)
        for metric, value in reading.items():
            telemetry_windows[metric].append(value)
            result = check_telemetry_window(metric)
            if result is None:
                continue

            status, breach_count = result
            unit = TELEMETRY_METRICS[metric]["unit"]
            telemetry_item = {
                "device_id": DEVICE_ID,
                "metric": metric,
                "value": value,
                "breach_count": breach_count,
                "window_size": WINDOW_SIZE,
                "status": status,
                "_endpoint": TELEMETRY_ENDPOINT,
            }
            response = send_payload(telemetry_item)
            if response is None or response.status_code != 202:
                print(f"Failed to send telemetry {status}: {telemetry_item}")
                save_to_outbox(telemetry_item)
            else:
                print(f"Telemetry {status}: {metric}={value}{unit} "
                      f"({breach_count}/{WINDOW_SIZE} readings breached threshold)")

        time.sleep(delay)


if __name__ == "__main__":
    run(num_frames=50, delay=0.5)