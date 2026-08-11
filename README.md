# NexTex AI — Take-Home Assignment

Cloud ingestion service (Part 1) + edge simulator (Part 2) for the Jetson
anomaly-detection pipeline, plus design notes (Part 3).

## Repo structure

```
nextex_take_home/
├── docker-compose.yml
├── cloud/
│   ├── Dockerfile
│   ├── api.py          # Flask app: POST /events, GET /metrics
│   ├── consumer.py      # drains the Redis queue into SQLite
│   ├── requirements.txt
│   └── data/            # SQLite file lives here (Docker volume)
└── simulator/
    ├── simulate.py       # stands in for a Jetson device
    └── requirements.txt
```

## Running it

```
docker compose up --build
```

This starts three containers: Redis (queue), the ingestion API (`:8000`),
and the consumer that drains the queue into SQLite. No manual setup
beyond Docker is needed — `api.py` and `consumer.py` read `REDIS_HOST`
and `DB_PATH` from the environment (set in `docker-compose.yml`), so the
same code runs unmodified whether it's in a container or on your machine.

In a separate terminal, run the edge simulator:

```
pip install -r simulator/requirements.txt
python simulator/simulate.py
```

Check the pipeline is working:

```
curl http://localhost:8000/metrics
```

## What each part does

- **`cloud/api.py`** — `POST /events` validates that `event_type` and
  `confidence` are present, then pushes the event onto a Redis list
  (`events_queue`) and returns `202`. It never touches SQLite directly —
  ingestion and processing are two separate concerns.
- **`cloud/consumer.py`** — a separate process. Blocks on the Redis queue
  (`BRPOP`, 5s timeout) and on each event writes a row into a SQLite
  `events` table (`event_type`, `device_id`, `class`, `confidence`,
  `timestamp`). Runs independently of the API so a slow/stuck consumer
  can never block event ingestion.
- **`cloud/api.py` → `GET /metrics`** — reports `total_events`,
  `backlog` (current Redis queue length), `events_by_type` (`new_class`
  vs `alarm` — the two categories the brief defines), and
  `alarms_by_class` (which defects are triggering alarms most often).
- **`simulator/simulate.py`** — loops over frames, fakes an inference
  result (`mock_inference()`: random class + confidence), and applies
  the brief's two trigger rules: first time seeing a class → `new_class`
  event; confidence ≥ `CONFIDENCE_THRESHOLD` (0.85) → `alarm` event. If
  the API is unreachable, the event is appended to a local `outbox.json`
  file instead of being dropped; every loop iteration calls
  `flush_outbox()` first, retrying anything still buffered before
  sending the current frame's event.

## Verified: zero-loss outage recovery

This is the behavior the brief says it cares about most, and it's been
tested against the actual dockerized service, not just locally:

```
docker compose stop api        # kill the cloud mid-run
# simulator keeps running, buffers every event into outbox.json
docker compose start api       # bring it back
# next loop iterations resync the full outbox automatically
curl http://localhost:8000/metrics   # totals include everything, buffered or not
```

** Simulator Logs on Catchup/Resync Missing Events

Captured run — 10 events sent normally, `api` container stopped mid-run
(19 events buffered locally), container restarted, full backlog resynced
with zero loss:

​```
Sent event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.94, 'event_type': 'new_class'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 1.0, 'event_type': 'alarm'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.89, 'event_type': 'new_class'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.92, 'event_type': 'new_class'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'broken_stitch', 'confidence': 0.94, 'event_type': 'new_class'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'vertical_lines', 'confidence': 0.9, 'event_type': 'new_class'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'broken_stitch', 'confidence': 0.94, 'event_type': 'alarm'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.91, 'event_type': 'alarm'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.96, 'event_type': 'alarm'}, Response: 202
Sent event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.92, 'event_type': 'alarm'}, Response: 202
Failed to send event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.89, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.9, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.85, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.99, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.87, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'vertical_lines', 'confidence': 0.85, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'vertical_lines', 'confidence': 0.85, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.96, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.91, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.89, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'broken_stitch', 'confidence': 0.86, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.92, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'broken_stitch', 'confidence': 0.92, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'oil_stain', 'confidence': 0.92, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'broken_stitch', 'confidence': 0.96, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'vertical_lines', 'confidence': 0.95, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'vertical_lines', 'confidence': 0.95, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'pinched_fabric', 'confidence': 0.97, 'event_type': 'alarm'}
Failed to send event: {'device_id': 'jetson-01', 'class': 'needle_mark', 'confidence': 0.95, 'event_type': 'alarm'}
Resynced 19 events from outbox, 0 events remain in outbox.
​```



## Why these choices

**Redis for the queue.** Single binary, no config, `LPUSH`/`BRPOP` is
enough for the producer/consumer split the brief asks for. Kafka or
RabbitMQ would make more sense at real fleet scale but add setup cost
with no benefit at this size.

**SQLite for storage.** A file-based DB removes any need to provision a
database service for a take-home. `api.py` and `consumer.py` share the
same file via the `./cloud/data` Docker volume.

**One generic `events` table, not one per defect type.** The brief's
appendix makes this explicit: alarms and new-class detections reach the
backend as the same shape (a class label + confidence), and the defect
vocabulary is expected to keep growing — special-casing types in the
schema would mean a migration every time a new defect is added.

**`event_type` is strictly `new_class` / `alarm`; `class` is the defect
name.** These answer two different questions — *why* did this event
fire vs. *what* did the model see — and conflating them into one field
would make it impossible to answer either cleanly. `/metrics` reports
both breakdowns separately.

## Local disk vs. MQTT vs. hybrid for telemetry

Not implemented as working code — the brief lists this as a design
decision to reason about, not a required component — but the reasoning:
telemetry (CPU/GPU/temp/sensor readings) differs from alarms and frames
in two ways that argue for different handling. Value density is much
lower — one confidence alarm is actionable on its own, one CPU-temperature
reading generally isn't. And volume is much higher and constant, not
event-triggered. Given that, I'd go **hybrid**: full-resolution telemetry
stays on local disk (a rolling window, overwritten as it fills), and
only a downsampled summary ships to the cloud on a slow interval, over
MQTT rather than a synchronous HTTP POST per reading — built for exactly
this "many small messages, unreliable network" case. Deep debugging on
a specific device can pull the full local log directly from it.

## What breaks going from 1 factory / 1 device to 50 factories / 20 devices each

That's 1 → 1,000 devices. A few things stop being fine:

- **SQLite stops being viable** — a single file with limited concurrent-write
  support, fine for one API + one consumer, not fine for 1,000 devices'
  continuous write volume. First thing I'd swap, for Postgres.
- **Redis as a single list is a bottleneck and a single point of failure**
  — I'd move to something with partitioning and consumer groups (Kafka)
  so throughput scales with the number of consumers.
- **One consumer process can't keep up** — needs to become horizontally
  scalable, or backlog grows unbounded during any traffic spike.
- **Device identity becomes a real problem** — right now `device_id` is
  a hardcoded string anyone could send; at 1,000 devices you need real
  per-device credentials so one compromised device can't impersonate
  another.
- **`/metrics` as written won't hold up** — it's a full-table-scan count
  query on every request; at that volume you'd want counters updated
  incrementally as events land instead.

## What I'd address before production

- **Device identity and security** — first priority. Anything can
  currently `POST` to `/events` claiming any `device_id`; production
  needs per-device certificates (mutual TLS) so the ingestion service
  can verify a device before trusting its data.
- **Observability** — `/metrics` here is pull-based and manual.
  Production needs structured logging/metrics from both the API and
  consumer, plus alerting on backlog growth specifically, since a
  growing queue backlog is exactly the signal that something's unhealthy.
- **OTA updates** — out of scope to build, but I'd design toward:
  versioned model artifacts in a registry, staged/canary rollout before
  fleet-wide push, and a rollback path.
- **Data lifecycle** — uploaded frames need a retention policy; keeping
  every frame forever is a cost and privacy problem at fleet scale.

## Assumptions

- Events are accepted as JSON over HTTP rather than real multipart image
  upload — the brief explicitly says to mock inference, so this keeps
  Part 1 focused on ingestion/queue/consumer/buffering behavior.
- A "new anomaly class" always fires its own event even if its
  confidence also crosses the alarm threshold (new-class-or-alarm, not
  both at once) — an explicit interpretation call, since the brief
  doesn't specify what happens when both conditions are true.
- `seen_classes` in `simulate.py` is in-memory only, so a simulator
  restart re-reports already-seen classes as new. A real device would
  persist this locally so a reboot doesn't retrigger new-class events —
  noted as a known simplification given the time budget, not fixed.
- Telemetry ingestion itself isn't implemented (see the hybrid section
  above) — only alarm and new-class events are wired end-to-end.
