import requests
import time
import random
import json 
import os



OUTBOX_FILE = "outbox.json"  # File to store events that couldn't be sent


API_URL = "http://localhost:8000/events"  
CONFIDENCE_THRESHOLD = 0.85
DEVICE_ID = "jetson-01"  # Example device ID

# sets of possible error events
POSSIBLE_EVENTS = ["broken_stitch", "needle_mark", "pinched_fabric", "oil_stain", "vertical_lines"]

seen_classes = set()  # To keep track of seen classes

def mock_inference():
    error_class = random.choice(POSSIBLE_EVENTS)
    confidence = round(random.uniform(0.7, 1.0), 2)  # Random confidence between 0.7 and 1.0
    return {
        "device_id": DEVICE_ID,
        "class": error_class,  
        "confidence": confidence
    }


def send_event(event):
    try:
        response = requests.post(API_URL, json=event, timeout=5)  # Set a timeout of 5 seconds
        return response
    
    except requests.exceptions.RequestException as e:
        return None     

def run(num_frames=100, delay=1):
    for _ in range(num_frames):
        flush_outbox()  # Attempt to send any events in the outbox before generating a new event
        event = mock_inference()
        detected_class = event["class"]

        is_new_class = detected_class not in seen_classes

        if is_new_class:
            seen_classes.add(event["class"])

        should_send = is_new_class or event["confidence"] >= CONFIDENCE_THRESHOLD

        event = event | {"event_type": "alarm" if not is_new_class else "new_class"}  # Add event_type to the event


        if should_send:
            response = send_event(event)
            if response is None or response.status_code != 202:
                print(f"Failed to send event: {event}")
                save_to_outbox(event)  # Save the event to the outbox for later retry
            else:
                print(f"Sent event: {event}, Response: {response.status_code if response else 'No Response'}")

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
                remaining_events.append(event)  # Keep the event for retry
            else:
                resynced_events += 1

        if resynced_events > 0:
            print(f"Resynced {resynced_events} events from outbox, {len(remaining_events)} events remain in outbox.")
        # elif remaining_events:
          #  print(f"Failed to resync {len(remaining_events)} events from outbox. They will remain in the outbox for retry.")


        # Update the outbox file with any remaining events
        f.seek(0)
        f.truncate()
        json.dump(remaining_events, f, indent=4)



if __name__ == "__main__":
    run(num_frames=50, delay=0.5)  # Run the simulation for 50 frames with a 0.5 second delay between frames
