import requests
import time
import random
import json
import os
import uuid
from PIL import Image, ImageDraw

OUTBOX_FILE = "outbox.json"
CAPTURED_DIR = "captured_frames"        # defected per-frame images, awaiting upload

API_URL = "http://localhost:8000/events"
CONFIDENCE_THRESHOLD = 0.85
DEVICE_ID = "jetson-01"

POSSIBLE_EVENTS = ["broken_stitch", "needle_mark", "pinched_fabric", "oil_stain", "vertical_lines"]

seen_classes = set()


def capture_frame():
    """Simulates one camera capture, happens every loop tick, independent of
    what the (mocked) model will later say about it. A real device captures
    continuously; it doesn't know in advance which frame will be interesting."""
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


def send_event(event):
    frame_path = event.get("_frame_path")
    payload = {k: v for k, v in event.items() if k != "_frame_path"}

    try:
        if frame_path and os.path.exists(frame_path):
            with open(frame_path, "rb") as f:
                files = {"frame": (os.path.basename(frame_path), f, "image/jpeg")}
                data = {k: str(v) for k, v in payload.items()}
                response = requests.post(API_URL, data=data, files=files, timeout=5)
        else:
            response = requests.post(API_URL, json=payload, timeout=5)
        return response

    except requests.exceptions.RequestException:
        return None


def run(num_frames=100, delay=1):
    for _ in range(num_frames):
        flush_outbox()  # Attempt to resend anything buffered before generating a new event

        frame_path = capture_frame()  # every tick captures a frame, regardless of outcome
        event = mock_inference(frame_path)
        detected_class = event["class"]

        is_new_class = detected_class not in seen_classes
        if is_new_class:
            seen_classes.add(detected_class)

        should_send = is_new_class or event["confidence"] >= CONFIDENCE_THRESHOLD

        event = event | {"event_type": "alarm" if not is_new_class else "new_class"}
        event["_frame_path"] = frame_path if should_send else None

        if should_send:
            response = send_event(event)
            if response is None or response.status_code != 202:
                print(f"Failed to send event: {event}")
                save_to_outbox(event)  # frame file (if any) stays on disk for the retry
            else:
                print(f"Sent event: {event}, Response: {response.status_code if response else 'No Response'}")
                cleanup_frame(event.get("_frame_path"))
        else:
            # not sent (below threshold, already-seen class): frame served no purpose, discard it
            cleanup_frame(frame_path)

        time.sleep(delay)  # Simulate time between frames


def save_to_outbox(event):
    if not os.path.exists(OUTBOX_FILE):
        with open(OUTBOX_FILE, 'w') as f:
            json.dump([], f)  # Initialize the file with an empty list

    with open(OUTBOX_FILE, 'r+') as f:
        outbox = json.load(f)
        outbox.append(event)
        f.seek(0)
        json.dump(outbox, f, indent=4)


def flush_outbox():
    if not os.path.exists(OUTBOX_FILE):
        return  # No outbox to flush

    with open(OUTBOX_FILE, 'r+') as f:
        outbox = json.load(f)
        remaining_events = []
        resynced_events = 0

        for event in outbox:
            response = send_event(event)
            if response is None or response.status_code != 202:
                remaining_events.append(event)  # frame file (if any) stays put for the next retry
            else:
                resynced_events += 1
                cleanup_frame(event.get("_frame_path"))

        if resynced_events > 0:
            print(f"Resynced {resynced_events} events from outbox, {len(remaining_events)} events remain in outbox.")

        f.seek(0)
        f.truncate()
        json.dump(remaining_events, f, indent=4)


if __name__ == "__main__":
    run(num_frames=50, delay=0.5)