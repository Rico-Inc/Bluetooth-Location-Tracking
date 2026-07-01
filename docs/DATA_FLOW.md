# Data Flow — BLE Employee Location Tracking

This document traces how a signal from a physical BLE tag becomes a location
record in the database and ends up on the dashboard (and eventually NetSuite).

## System at a glance

```
┌──────────┐   BLE adv    ┌──────────────┐   MQTT/WiFi   ┌───────────────────┐
│ BLE Tag  │ ───────────▶ │ ESP32        │ ────────────▶ │ Mosquitto broker  │
│ (beacon  │   (RSSI)     │ Receiver     │  ble/readings │ (localhost:1883)  │
│  on      │              │ (firmware)   │   JSON        └─────────┬─────────┘
│  person) │              └──────────────┘                        │ subscribe
└──────────┘                                                       ▼
                                                        ┌───────────────────────┐
                                                        │ FastAPI server        │
                                                        │  • on_mqtt_message()   │
                                                        │  • LocationEngine      │
                                                        │  • processing_loop()   │
                                                        └──────────┬────────────┘
                                                                   │ SQL
                                                                   ▼
                                                        ┌───────────────────────┐
                                                        │ SQLite (ble_tracking.db)│
                                                        │  raw_readings          │
                                                        │  employees / locations │
                                                        │  location_log          │
                                                        └──────────┬────────────┘
                                                                   │ read
                                                    ┌──────────────┴──────────────┐
                                                    ▼                             ▼
                                          ┌──────────────────┐        ┌────────────────────┐
                                          │ Web dashboard    │        │ NetSuite sync       │
                                          │ /admin/* HTML    │        │ /api/netsuite/*     │
                                          │ /api/* JSON      │        │ (pending → synced)  │
                                          └──────────────────┘        └────────────────────┘
```

Two codebases:
- **`firmware/`** — ESP32 receiver firmware (C++/Arduino via PlatformIO).
- **`server/`** — Python FastAPI app that ingests readings, computes location, and serves the dashboard.

---

## Stage 1 — Tag → Receiver (physical layer)

- Employees carry BLE beacon tags that continuously broadcast advertisements.
- Each **ESP32 receiver** is bolted to a fixed spot (a workstation or a zone).
- The firmware (`firmware/src/main.cpp`) runs a loop:
  1. **Scan** for BLE advertisements for `SCAN_DURATION_SEC` (5s).
  2. **Filter** by MAC prefix — only tags whose MAC starts with `TAG_MAC_PREFIX` (`DC0D30`) are kept (`matchesPrefix`). This screens out phones, laptops, and other BLE noise.
  3. **Aggregate** each seen tag in `tagReadings[]`, tracking a running `rssiSum` and `count` (`addReading`).
- **RSSI** (signal strength, e.g. `-58`) is the core signal — closer to `0` means the tag is physically nearer that receiver.

## Stage 2 — Receiver → MQTT (transport)

- Every `REPORT_INTERVAL_MS` (15s), the receiver:
  1. Stops the scan to free the radio, ensures WiFi + MQTT are connected.
  2. Averages RSSI per tag (`rssiSum / count`) in `publishReadings()`.
  3. Publishes one JSON message to MQTT topic **`ble/readings`**.
- The receiver identifies itself by its own WiFi MAC (`receiverMac`), captured at boot via `getReceiverMac()`. This MAC is the join key to a location on the server.
- Payload shape:

```json
{
  "receiver_mac": "AA:BB:CC:DD:EE:01",
  "readings": [
    {"tag_id": "DC:0D:30:48:30:2D", "rssi": -58},
    {"tag_id": "DC:0D:30:11:22:33", "rssi": -74}
  ],
  "timestamp": "2026-03-03T10:00:00Z",
  "wifi_rssi": -61
}
```

- Even when no tags are seen, the periodic publish acts as a **heartbeat** (empty `readings`) so the server knows the receiver is alive.
- Broker: **Mosquitto** on `localhost:1883`. The firmware points at `MQTT_BROKER` (`192.168.2.40` in the field); the server subscribes at `MQTT_BROKER` (`localhost`).

## Stage 3 — MQTT → Server ingest

`server/server.py`, `on_mqtt_message()`:

1. Parse JSON. Extract `receiver_mac`, `timestamp`, optional `wifi_rssi`.
2. **Health**: stamp `engine.receiver_health[receiver_mac] = timestamp` (even for heartbeats), and store `engine.receiver_wifi_rssi[receiver_mac]`.
3. **Persist raw**: insert every reading into the **`raw_readings`** table (audit trail / debugging).
4. **Feed the engine**: `engine.add_reading()` buffers each reading in memory as `readings_buffer[tag_id][receiver_mac] = [rssi, ...]`.

At this point nothing has been decided about *where* anyone is — readings are just being collected.

## Stage 4 — Location engine (the decision)

`LocationEngine` uses a **"loudest signal wins"** algorithm with hysteresis so people don't flip-flop between adjacent receivers. A background thread (`processing_loop`) calls `process_window()` every `WINDOW_SECONDS` (60s):

1. **Snapshot & clear** the in-memory buffer (under a lock).
2. Load lookups from DB: `receiver_mac → location_id`, `tag_id → employee_id`.
3. For each tag seen in the window:
   - **Average** RSSI per receiver.
   - Pick the **strongest** receiver (max RSSI).
   - **Ignore** if even the strongest is below `RSSI_THRESHOLD_WEAK` (`-80`) — treated as passing-through / gone.
   - Map that receiver to a **candidate location**.
   - If the candidate equals the current confirmed location → no change.
   - Otherwise track it in `candidates[tag_id]`, incrementing a `count` each consecutive window it persists.
   - Once `count >= HOLD_PERIODS` (2 windows ≈ 2 min), **confirm** the move via `_log_transition()`.

Key tunables (top of `server.py`):

| Constant | Value | Meaning |
|---|---|---|
| `WINDOW_SECONDS` | 60 | Averaging window length |
| `HOLD_PERIODS` | 2 | Consecutive windows a candidate must hold before it's confirmed |
| `RSSI_THRESHOLD_STRONG` | -65 | "Definitely here" (reference) |
| `RSSI_THRESHOLD_WEAK` | -80 | Below this = ignore |
| `RECEIVER_TIMEOUT_SECONDS` | 600 | No data for 10 min = receiver offline |

## Stage 5 — Writing a location change (`_log_transition`)

The **`location_log`** table is the source of truth for presence. It's an open/close interval log:

1. Find the employee's most recent **open** entry (`timestamp_out IS NULL`) and set its `timestamp_out` = now (closing the previous location).
2. Insert a **new** row with `timestamp_in` = now and `timestamp_out` = NULL (the new open location).
3. Update `engine.current_locations[tag_id]` in memory.

So an employee always has at most one open (`timestamp_out IS NULL`) row = where they are right now. Closed rows form their history with computable durations.

**Restart safety**: on startup, the server re-reads all open `location_log` entries back into `engine.current_locations` so a server restart doesn't lose or duplicate presence state.

## Stage 6 — Reading the data out

All consumers read from SQLite; nothing recomputes location on read.

**Web UI (HTML, server-rendered):**
- `/` — Dashboard: everyone currently located (open `location_log` rows). Auto-refresh 30s.
- `/admin/employees` — CRUD employees + tag assignment.
- `/admin/locations` — CRUD locations + receiver-MAC mapping.
- `/admin/history` + `/admin/history/{id}` — per-employee interval history with durations.
- `/admin/health` — receiver online/offline + WiFi signal. Auto-refresh 15s.
- `/admin/flash` — flash firmware to a connected ESP32 and assign its MAC to a location (see below).

**JSON API:**
- `/api/employees`, `/api/employees/{id}/history`
- `/api/locations`, `/api/locations/{id}/occupants`
- `/api/tags/register` (POST), `/api/tags/{tag_id}` (DELETE)
- `/api/health`

## Stage 7 — NetSuite sync (downstream, not yet automated)

The pipeline is staged but the actual NetSuite push is external:
- `/api/netsuite/pending` returns **closed** `location_log` rows (`timestamp_out IS NOT NULL`) where `synced_to_netsuite = 0`, enriched with employee `netsuite_employee_id`, location, and department.
- An external job would push those, then call `/api/netsuite/mark-synced` (POST list of log ids) to flip `synced_to_netsuite = 1`.

Only *completed* intervals sync — an in-progress location isn't pushed until the person leaves and it closes.

---

## Side flow — Provisioning a receiver (`/admin/flash`)

Not part of the runtime data path, but how a receiver gets onto the map:

1. `/api/flash/ports` — list USB serial ports.
2. `/api/flash/stream` (SSE) — locate PlatformIO (`find_pio()`), run `pio run --target upload` against `firmware/`, stream build/upload output.
3. `/api/flash/capture-mac` (SSE) — read the device's serial boot output and extract the line `[Info] Receiver MAC:` → the receiver's MAC.
4. `/api/flash/assign` (POST) — write that MAC into `locations.receiver_mac`, binding the physical device to a location.

Once assigned, that receiver's future MQTT messages map to a location in Stage 4.

---

## Data stores summary

| Table | Written by | Read by | Purpose |
|---|---|---|---|
| `raw_readings` | `on_mqtt_message` | (debug/audit) | Every individual RSSI reading |
| `employees` | admin UI / API | engine, UI | Person ↔ `tag_id` ↔ `netsuite_employee_id` |
| `locations` | admin UI / flash | engine, UI | Place ↔ `receiver_mac` ↔ department |
| `location_log` | `_log_transition`, tag deactivation | dashboard, history, NetSuite | Open/close presence intervals (source of truth) |

**In-memory engine state** (lost on restart except `current_locations`, which is rehydrated):
`readings_buffer`, `current_locations`, `candidates`, `receiver_health`, `receiver_wifi_rssi`.
