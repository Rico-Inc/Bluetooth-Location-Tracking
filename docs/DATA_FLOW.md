# Data Flow — BLE Employee Location Tracking

This document traces how a signal from a physical BLE tag becomes a location
record and ends up on the dashboard and the live station displays.

> **Target architecture.** This reflects the design *after* the SQLite →
> Supabase migration (see [SUPABASE_MIGRATION.md](SUPABASE_MIGRATION.md)).
> The ESP32 firmware and the MQTT ingest path (Stages 1–3) are unchanged by
> the migration. What changed: SQLite is replaced by Supabase (cloud
> Postgres); employee identity is unified on `platform_users`; station
> identity comes from a **read-only mirror of NetSuite's production-station
> records** (`netsuite_production_stations`) rather than a local `locations`
> table; and the historical NetSuite push is replaced by Supabase Realtime.

## System at a glance

```
   NetSuite                          rico-platform                    Supabase (cloud Postgres)
┌──────────────────┐   twice-daily  ┌────────────────────┐  truncate ┌────────────────────────────┐
│ customrecord_    │──── sweep ────▶│ sweep-production-  │── +load ─▶│ netsuite_production_stations │
│ production_      │  (NS → mirror) │ stations.ts        │  (in a tx)│  (mirror; BLE app READS only)│
│ station          │                └────────────────────┘           │                              │
└──────────────────┘                ┌────────────────────┐  admin    │ platform_users (+ble_tag_id) │
                                     │ user editor        │── edit ──▶│                              │
                                     │ ("BLE Tag ID")     │           │ location_log (+ snapshots)   │
                                     └────────────────────┘           └───────▲───────────┬──────────┘
                                                                  writes(tx)  │           │ Realtime
┌──────────┐   BLE adv    ┌──────────────┐   MQTT/WiFi   ┌───────────────────┴──┐        │ + broadcast
│ BLE Tag  │ ───────────▶ │ ESP32        │ ────────────▶ │ FastAPI server (LAN)  │        ▼
│ (beacon  │   (RSSI)     │ Receiver     │  ble/readings │  • on_mqtt_message()  │  ┌───────────────┐
│  on      │              │ (firmware)   │   JSON        │  • tag→employee cache │  │ Station display│
│  person) │              └──────────────┘  ┌─────────┐  │  • rx→station cache   │  │ SPA (future)   │
└──────────┘                               │ Mosquitto│◀─┤  • LocationEngine     │  │ arrival events │
                                           │ broker   │  │  • processing_loop()  │  └───────────────┘
                                           │ (LAN)    │  └───────────┬───────────┘
                                           └──────────┘              │ reads (dashboard/API)
                                                                     ▼
                                                          ┌──────────────────┐
                                                          │ Web dashboard    │
                                                          │ /admin/* HTML     │
                                                          │ /api/* JSON       │
                                                          └──────────────────┘
```

Three repos touch this system:
- **`Rico-Inc/Bluetooth-Location-Tracking`** (this repo) — `firmware/` (ESP32, **unchanged by the migration**) and `server/` (the FastAPI ingest app).
- **`Rico-Inc/rico-platform`** — owns the Supabase schema, the `platform_users` admin UI (where tags are assigned), and the twice-daily job that mirrors NetSuite production stations into Supabase.
- **NetSuite** — system of record for production stations (`customrecord_production_station`, incl. `custrecord_bt_mac_address`). The BLE app never talks to NetSuite directly; it only reads the Supabase mirror.

---

## Stage 1 — Tag → Receiver (physical layer)

- Employees carry BLE beacon tags that continuously broadcast advertisements.
- Each **ESP32 receiver** is bolted to a fixed spot (a production station).
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
- The receiver identifies itself by its own WiFi MAC (`receiverMac`), captured at boot via `getReceiverMac()`. **This MAC is the join key to a production station.**
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
- Broker: **Mosquitto** on the factory LAN (`:1883`). The firmware points at `MQTT_BROKER` (`192.168.2.40`); the server subscribes to the same broker.

## Stage 3 — MQTT → Server ingest

`server/server.py`, `on_mqtt_message()`:

1. Parse JSON. Extract `receiver_mac`, `timestamp`, optional `wifi_rssi`.
2. **Health**: stamp `engine.receiver_health[receiver_mac] = timestamp` (even for heartbeats), and store `engine.receiver_wifi_rssi[receiver_mac]`.
3. **Feed the engine**: `engine.add_reading()` buffers each reading in memory as `readings_buffer[tag_id][receiver_mac] = [rssi, ...]`.

Raw per-reading persistence (`raw_readings`) is **not** written to Supabase —
~500K rows/day is too much for the cloud DB. It is dropped, or optionally kept
in a local-only SQLite file for debugging. Ingest is otherwise unchanged from
the SQLite design; nothing has yet been decided about *where* anyone is —
readings are just being collected.

## Stage 3.5 — Identity caches (two in-memory maps)

The FastAPI server runs on the factory LAN; Supabase is in the cloud. We can't
afford a WAN round-trip per BLE reading, so both identity lookups are served
from in-memory maps that are refreshed in the background.

**Tag → employee** (`tag_to_employee`), from `platform_users`:

```
{ "DC:0D:30:48:30:2D": { id: <uuid>, name: "Alice Johnson", department: "Print" } }
```

- **Bulk-load at boot** — one `select` of every `platform_users` row with a `ble_tag_id`.
- **Realtime subscription** — subscribe to `platform_users` changes; when an admin assigns/reassigns a tag in the rico-platform UI, the cache updates within ~1s (old tag evicted, new tag added).
- **Periodic full reload** — every 5 minutes, re-bulk-load to recover from any missed Realtime events.

**Receiver → station** (`receiver_to_station`), from the NetSuite mirror
`netsuite_production_stations`:

```
{ "AA:BB:CC:DD:EE:01": { id: <ns_internal_id>, name: "UV Printer 1" } }
```

- Keyed by `bt_mac_address` (the receiver MAC set on the NS station record).
- **Reloaded at boot and hourly.** The mirror itself only changes twice a day (the rico-platform sweep), so hourly is more than fresh enough.
- A receiver MAC that isn't in this map is an **orphan** — powered on but not yet bound to a station in NetSuite. Readings are still processed and stored (see Stages 4–5); they just carry no station until the NS record catches up.

Both lookups are then pure dict reads. A tag not in `tag_to_employee` is
skipped (unassigned). A receiver not in `receiver_to_station` resolves to
"no station" but is **not** skipped.

## Stage 4 — Location engine (the decision)

`LocationEngine` uses a **"loudest signal wins"** algorithm with hysteresis so
people don't flip-flop between adjacent receivers. A background thread
(`processing_loop`) calls `process_window()` every `WINDOW_SECONDS` (60s):

1. **Snapshot & clear** the in-memory reading buffer (under a lock).
2. For each **tag** seen in the window:
   - Resolve `tag → employee` via the cache. **Skip if unassigned.**
   - **Average** RSSI per receiver.
   - Pick the **strongest** receiver (max RSSI).
   - **Ignore** if even the strongest is below `RSSI_THRESHOLD_WEAK` (`-80`) — treated as passing-through / gone.
   - Resolve the winning `receiver_mac → station` via the cache. A station may or may not exist (orphan receivers resolve to none).
   - Compare against the employee's current confirmed location (see key rule below). If unchanged → no change.
   - Otherwise track it in `candidates`, incrementing a `count` each consecutive window it persists.
   - Once `count >= HOLD_PERIODS` (2 windows ≈ 2 min), **confirm** the move via `_log_transition()`.

> **Orphan-safe engine key.** `candidates` and `current_locations` are keyed by
> `platform_users.id` (a UUID). The **value** they track is the resolved
> **station id when one exists, otherwise the `receiver_mac`**. Keying the
> value on `location_id` alone would make two not-yet-provisioned antennas both
> read as "no station," so a person moving between them would produce no new
> row — losing that movement and leaving a stale `receiver_mac` on the open
> row. Using `location_id or receiver_mac` keeps orphan-to-orphan moves
> distinct and correctly attributed for later backfill. (Employee identity is
> keyed on the UUID, not the tag, because a tag can be reassigned but the UUID
> is stable.)

Key tunables (top of `server.py`, unchanged from the SQLite design):

| Constant | Value | Meaning |
|---|---|---|
| `WINDOW_SECONDS` | 60 | Averaging window length |
| `HOLD_PERIODS` | 2 | Consecutive windows a candidate must hold before it's confirmed |
| `RSSI_THRESHOLD_STRONG` | -65 | "Definitely here" (reference) |
| `RSSI_THRESHOLD_WEAK` | -80 | Below this = ignore |
| `RECEIVER_TIMEOUT_SECONDS` | 600 | No data for 10 min = receiver offline |

Latency budget: a confirmed transition lands ~2 min (p95) after the person
actually moves. That's acceptable for this use case.

## Stage 5 — Writing a location change (`_log_transition`)

The **`location_log`** table is the source of truth for presence. It's an
open/close interval log. The close-old + open-new pair **must run inside one
Postgres transaction** (via an `asyncpg` connection) so concurrent workers or
Realtime retries can't leave two open rows for the same person:

1. `UPDATE location_log SET timestamp_out = now() WHERE employee_id = $1 AND timestamp_out IS NULL` — close the previous open location.
2. `INSERT` a new row with `timestamp_in = now()`, `timestamp_out = NULL`, and **snapshot columns** filled: `employee_name`, `employee_department`, `location_id`, `location_name`, `receiver_mac`.
3. Update `engine.current_locations[employee_id]` in memory (with the orphan-safe key from Stage 4).

So an employee always has at most one open (`timestamp_out IS NULL`) row = where
they are right now. Closed rows form their history with computable durations.

**Snapshot columns are the durability strategy.** Every insert copies, onto the
row itself:

- `employee_name` + `employee_department` — from the tag cache. History stays
  readable even if the person is later renamed, their `platform_users` record
  is removed, or the tag is reassigned.
- `location_name` + `location_id` — from the station cache. `location_id` is a
  **plain snapshot with no foreign key** (the mirror is truncate-reloaded twice
  daily, so an FK would break the reload — see the data model). The row stays
  self-describing even after the mirror changes.
- `receiver_mac` — **always** populated, even when the station lookup misses.

**Orphan receivers never block a write.** If the winning receiver isn't bound
to a station yet, the row is still written with `location_id = NULL`,
`location_name = NULL`, and `receiver_mac` populated. Ops can find orphaned
receivers with `SELECT ... WHERE location_id IS NULL`, set the MAC on the NS
station record, and the name/id can be backfilled — no history is lost during
the gap.

**Restart safety**: on startup, the server re-reads all open `location_log`
rows (`timestamp_out IS NULL`) back into `engine.current_locations`, keyed by
`employee_id`, using the same orphan-safe value (`location_id` when set, else
`receiver_mac`), so a restart doesn't lose or duplicate presence state.

## Stage 6 — Realtime broadcast (live displays)

Immediately after the transaction commits, **and only if a station was
resolved**, the server publishes a Supabase Realtime **broadcast** on the
destination station's channel (`station:{location_id}`, where `location_id` is
the NS internal id) with an `arrival` event carrying the user id, name,
department, and timestamp.

- This drives the future station-display / gamification SPA without polling.
- **Orphan-receiver transitions don't broadcast** — there's no station channel to broadcast to yet. Their movement is still durable in `location_log`; a display lights up once the NS record (and the mirror) catches up.
- Broadcast is **fire-and-forget**: a failed broadcast is logged but never blocks or rolls back the transition. `location_log` is the durable record; Realtime is a best-effort notification layer on top.

## Stage 7 — Reading the data out

All consumers read from Supabase; nothing recomputes location on read.

**Web UI (HTML, server-rendered inline in `server.py`):**
- `/` — Dashboard: everyone currently located (open `location_log` rows). Auto-refresh 30s.
- `/admin/history` + `/admin/history/{id}` — per-employee interval history with durations.
- `/admin/health` — receiver online/offline + WiFi signal. Auto-refresh 15s.
- `/admin/flash` — flash firmware to a connected ESP32 and read its MAC (see the side flow).

> **Moved out of this app:** employee CRUD + tag assignment now live in the
> rico-platform admin UI (editing `platform_users.ble_tag_id`). Station + MAC
> binding lives in NetSuite. There is no local `employees` table, no local
> `locations` table, and no `netsuite_employee_id` field here anymore.

**JSON API:**
- `/api/health`
- (location/occupant read endpoints now query `location_log` + the station cache rather than a local `locations` table.)

> **Removed by the migration:** the `/api/netsuite/pending` and
> `/api/netsuite/mark-synced` routes, the `synced_to_netsuite` column, and the
> "Synced" column in the history view. All history now lives in Supabase; there
> is no historical NetSuite push path.

---

## Side flow — Provisioning a receiver (`/admin/flash`)

Not part of the runtime data path, but how a receiver gets deployed. The flash
+ MAC-capture steps are unchanged; the **binding** step changed because the
`locations` table is gone:

1. `/api/flash/ports` — list USB serial ports.
2. `/api/flash/stream` (SSE) — locate PlatformIO (`find_pio()`), run `pio run --target upload` against `firmware/`, stream build/upload output.
3. `/api/flash/capture-mac` (SSE) — read the device's serial boot output and extract the line `[Info] Receiver MAC:` → the receiver's MAC.
4. **Binding** — the receiver MAC is recorded on the NetSuite production-station record (`custrecord_bt_mac_address`); the twice-daily sweep propagates it into the `netsuite_production_stations` mirror, and the server's hourly station-cache reload picks it up. (The old `/api/flash/assign` → local `locations` write is being retired — see open decision #3 in the migration doc for the exact `/admin/flash` rework.)

Until that binding exists, the receiver is an **orphan**: its readings are
stored with `receiver_mac` but no station, and backfilled once bound.

---

# Data Model (ERD)

The cloud store is Supabase Postgres. Three tables matter to this app. Only
`location_log` is written by this app. `platform_users` and
`netsuite_production_stations` are read-only here (owned/filled by
rico-platform).

```
   ┌───────────────────────────────┐     ┌────────────────────────────────────┐
   │ platform_users                │     │ netsuite_production_stations         │
   │  (owned by rico-platform)     │     │  (mirror of NetSuite; truncate+reload│
   ├───────────────────────────────┤     │   twice daily — READ-ONLY here)      │
   │ PK id                uuid      │     ├──────────────────────────────────────┤
   │    first_name        text      │     │ PK ns_internal_id   bigint           │
   │    last_name         text      │     │    name             text             │
   │    department        text      │     │ U  bt_mac_address   text             │
   │ U  ble_tag_id        text      │     │    is_inactive      boolean          │
   │    ...                         │     │    raw              jsonb            │
   └───────────────┬───────────────┘     │    synced_at        timestamptz      │
                   │ 1                    └──────────────────┬───────────────────┘
                   │                                         ┊
                   │ employee_id                             ┊ location_id  (SOFT ref,
                   │ (FK, ON DELETE RESTRICT)                ┊  NO foreign key — snapshot
                   │                                         ┊  of ns_internal_id)
                   │ *                                       ┊ *
   ┌───────────────┴─────────────────────────────────────────┴────────────┐
   │ location_log            (source of truth for presence; written here)  │
   ├───────────────────────────────────────────────────────────────────────┤
   │ PK id                     bigint                                        │
   │ FK employee_id            uuid       → platform_users.id (RESTRICT)     │
   │    employee_name          text       snapshot                          │
   │    employee_department    text       snapshot                          │
   │    location_id            bigint     snapshot of ns_internal_id, NO FK, │
   │                                       NULL when receiver is orphaned    │
   │    location_name          text       snapshot, NULL when orphaned       │
   │    receiver_mac           text       physical BLE receiver MAC; always  │
   │                                       filled (soft-matches bt_mac_address)│
   │    timestamp_in           timestamptz                                   │
   │    timestamp_out          timestamptz  NULL = still here (one open row) │
   └───────────────────────────────────────────────────────────────────────┘
```

Legend: `──` solid = enforced foreign key. `┈┈` dashed = **soft reference**
(value is snapshotted; no FK constraint).

## Relationships

- **`platform_users` 1 —— * `location_log`** via `location_log.employee_id → platform_users.id`. FK is **`ON DELETE RESTRICT`** — a person's history can't be CASCADE-deleted out from under them.
- **`netsuite_production_stations` 1 ┈┈ * `location_log`** via `location_log.location_id → ns_internal_id`. **Soft reference, no FK** — the mirror is truncate-and-reloaded twice daily, so a real FK would either block the `TRUNCATE` (RESTRICT) or wipe historical ids (SET NULL). `location_id` + `location_name` are snapshots; the row is self-describing.
- **`receiver_mac` ↔ `bt_mac_address`** — how a reading is matched to a station at write time (via the in-memory station cache). Also a soft match; `receiver_mac` is stored on every row regardless.
- **`platform_users.ble_tag_id` ↔ MQTT `tag_id`** — how a reading is matched to a person. Unique, nullable.

## Table: `platform_users` (read-only here; owned by rico-platform)

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` | PK. Stable identity — never changes. Stored as `location_log.employee_id`. |
| `first_name`, `last_name` | `text` | Combined into the `employee_name` snapshot at write time. |
| `department` | `text` | Copied into the `employee_department` snapshot. |
| `ble_tag_id` | `text` | **Unique**, nullable. The tag MAC assigned to this person. Partial index where not null. Assigned via the rico-platform admin UI. |

Realtime is enabled on this table so the ingest server's tag→employee cache
refreshes within ~1s of an admin edit.

## Table: `netsuite_production_stations` (read-only here; mirror of NetSuite)

| Column | Type | Notes |
|---|---|---|
| `ns_internal_id` | `bigint` | PK. The NetSuite record internal id. Used as `location_log.location_id` and as the Realtime channel id (`station:{id}`). |
| `name` | `text` | Station name; snapshotted into `location_log.location_name`. |
| `bt_mac_address` | `text` | **Unique**. The bound receiver MAC (`custrecord_bt_mac_address`). Matched against MQTT `receiver_mac`. Indexed. |
| `is_inactive` | `boolean` | Inactive stations are filtered out of the cache. |
| `raw` | `jsonb` | Full NS row, for future fields. |
| `synced_at` | `timestamptz` | When the mirror last refreshed. |

Filled by rico-platform's `sweep-production-stations.ts` twice daily. The sweep
**truncates and reloads inside one transaction** so the BLE app's cache reload
never sees an empty mid-sync table. Because it truncates, **no table may hold a
foreign key to it.**

## Table: `location_log` (source of truth for presence; written by this app)

| Column | Type | Notes |
|---|---|---|
| `id` | `bigint` (identity) | PK. |
| `employee_id` | `uuid` | FK → `platform_users.id`, **`ON DELETE RESTRICT`**. The person, at write time. |
| `employee_name` | `text` | **Snapshot** of `first_name + last_name`. Survives renames/deletes. |
| `employee_department` | `text` | **Snapshot** of `department`. |
| `location_id` | `bigint` | **Snapshot** of `ns_internal_id`. **No FK.** `NULL` when the receiver is orphaned (no station bound yet). |
| `location_name` | `text` | **Snapshot** of the station name. `NULL` when orphaned. |
| `receiver_mac` | `text` | Physical receiver MAC. **Always filled**, even when orphaned. Indexed for orphan debugging. |
| `timestamp_in` | `timestamptz` | When they arrived. Defaults to `now()`. |
| `timestamp_out` | `timestamptz` | When they left. **NULL = still here** (the one open row per person). |

**Indexes (hot paths):**

| Index | Definition | Serves |
|---|---|---|
| `location_log_open_by_employee_idx` | `(employee_id) WHERE timestamp_out IS NULL` | "where is this person now" + restart rehydrate |
| `location_log_open_by_location_idx` | `(location_id) WHERE timestamp_out IS NULL` | "who's at this station now" (dashboard, occupants) |
| `location_log_employee_time_idx` | `(employee_id, timestamp_in DESC)` | per-employee history |
| `location_log_receiver_mac_idx` | `(receiver_mac)` | orphaned-receiver debugging |

**Realtime:** `location_log` is in the `supabase_realtime` publication. (Live
station displays primarily use the explicit broadcast in Stage 6, but the
table-level publication is available too.)

## Not in the cloud schema

| Table | Status |
|---|---|
| `employees` | **Removed.** Identity is unified on `platform_users`. |
| `locations` | **Removed.** Station identity comes from the `netsuite_production_stations` mirror; `location_log` snapshots `location_id` / `location_name` / `receiver_mac`. |
| `raw_readings` | **Not in Supabase.** Dropped, or optionally retained in a local-only SQLite file for debugging (~500K rows/day is too much for the cloud DB). |
| NetSuite time-record sync / `synced_to_netsuite` | **Removed.** No historical NetSuite push. |

## In-memory engine state (not persisted; rebuilt on restart)

| State | Keyed by | Value | Rebuilt from |
|---|---|---|---|
| `tag_to_employee` | `tag_id` | `{id, name, department}` | Bulk-load of `platform_users` at boot + Realtime + 5-min reload |
| `receiver_to_station` | `receiver_mac` | `{id, name}` | Load of `netsuite_production_stations` at boot + hourly reload |
| `readings_buffer` | `tag_id` → `receiver_mac` | `[rssi, ...]` | Live MQTT (transient, per-window) |
| `current_locations` | `employee_id` (uuid) | station id **or** `receiver_mac` (orphan-safe) | Open `location_log` rows on startup |
| `candidates` | `employee_id` (uuid) | pending station id / `receiver_mac` + count | Rebuilds naturally over a few windows |
| `receiver_health`, `receiver_wifi_rssi` | `receiver_mac` | last-seen / RSSI | Live MQTT |
