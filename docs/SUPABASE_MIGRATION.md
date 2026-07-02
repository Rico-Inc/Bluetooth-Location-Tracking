# BLE Location Tracker — SQLite → Supabase Migration

Playbook for the agent doing the rewrite. Two repos touch this project:

- **Rico-Inc/Bluetooth-Location-Tracking** (this repo) — FastAPI ingest
  server + ESP32 firmware.
- **Rico-Inc/rico-platform** — home of `platform_users` and the
  migration runner. Schema changes already applied on 2026-07-01 (see
  Status below); no further platform-side migration work required for
  this project.

Read this end to end before starting.

---

## Status (through 2026-07-02)

Rico-platform has already been prepped so the agent can start on the
FastAPI rewrite immediately, no Supabase admin work needed:

- [x] Supabase schema landed in all 4 environments (`rico-dev`,
      `rico-nonprod-dev`, `rico-nonprod-prod`, `rico-prod`).
- [x] `platform_users.ble_tag_id text unique` column added.
- [x] `location_log` table created + partial indexes + Realtime
      publication enabled.
- [x] `netsuite_production_stations` table (mirror of NetSuite's
      `customrecord_production_station`) created. Twice-daily
      NS→Supabase sync job wired in `apps/api` (`sweep-production-stations.ts`).
      Local BLE app reads station name + MAC from this mirror — never
      touches NetSuite directly.
- [x] History-durability follow-up applied: `location_log.employee_id`
      FK is now `ON DELETE RESTRICT`, and `employee_name` +
      `employee_department` snapshot columns exist on `location_log`.
- [x] Station-side durability applied: `location_log.location_id` is
      nullable **and carries no foreign key** (writes never fail on a
      missing station, and the twice-daily truncate-reload of the mirror
      can't touch it). `location_name` and `receiver_mac` snapshot
      columns exist on `location_log`, and `receiver_mac` is indexed for
      orphaned-receiver debugging.
- [x] Realtime enabled on `platform_users` so the FastAPI ingest server
      can subscribe to admin-side tag reassignments and refresh its
      in-memory cache within ~1s (see Step 3).
- [x] Admin UI in rico-platform now has a "BLE Tag ID" field on the
      user editor so ops can assign / reassign tags.
- [ ] FastAPI rewrite from SQLite → Supabase (this playbook).
- [ ] Firmware — no changes.
- [ ] Station display SPA — out of scope for this project; separate
      module inside rico-platform.

---

## Goals

1. Move all persistence from local SQLite to Supabase.
2. Unify employee identity on `platform_users` — no separate `employees`
   table.
3. **BLE app only depends on Supabase.** No direct NetSuite calls from
   the local app. Station names + MAC assignments live in NetSuite
   (`customrecord_production_station`) but are mirrored to Supabase
   twice daily by rico-platform's `sweep-production-stations.ts` job.
4. Drop the historical NetSuite time-record push path. All movement
   history lives in Supabase.
5. Emit Supabase Realtime broadcasts on confirmed transitions for a
   future station-display (gamification screen) to react without polling.
6. Keep existing 60s window / 2 hold-periods tuning. p95 latency
   ~2 minutes is acceptable. No ESP32 firmware changes.
7. **Preserve history integrity across employee lifecycle changes** —
   lost tags, tag reassignments, employee deletions must not corrupt
   or erase historical location data.

---

## The employee-identity rule (important, easy to get wrong)

Employees will lose tags. Tags will be reassigned. Occasionally an
employee record may be removed. The schema is designed so historical
`location_log` rows survive all of this:

- `location_log.employee_id` stores the platform_users UUID at INSERT
  time. That UUID never changes even if the person's `ble_tag_id`
  changes later.
- When a tag is reassigned to a different person, the ingest server
  writes new `location_log` rows with the NEW employee's UUID. Old rows
  are untouched.
- `location_log.employee_id` FK is `ON DELETE RESTRICT` — you can't
  accidentally CASCADE-delete a person's history when their record is
  removed.
- **`location_log.employee_name` + `location_log.employee_department`
  are snapshot columns.** The ingest server MUST fill them at INSERT
  time by reading the current `platform_users.first_name + last_name`
  + `department`. This guarantees reports show a name even if:
  - The employee record is later force-removed
  - The employee is renamed
  - Data is exported to a report that doesn't join `platform_users`

Every INSERT into `location_log` must snapshot the name. Not optional.

---

## Azure Key Vault — keys the agent will need

Vault: `ricoincbikeyvault`. Read via:
```bash
az keyvault secret show --vault-name ricoincbikeyvault --name <secret-name> --query value -o tsv
```

**The local BLE FastAPI app points at `rico-prod` — the real production
Supabase.** Rationale: this is a production floor tool that reads NS
station data (which lives in rico-prod, filled by the twice-daily sync)
and writes employee location history that ops will actually query. The
`rico-dev` project exists for sandbox testing only.

The BLE tracker uses the **prod webapp Supabase pair**, not the
staging/devsite pair. Sandbox testing uses `rico-dev` first; cutover
switches to `rico-prod`.

### Production runtime (this is the default)

| Vault secret name | Env var in the BLE app | Purpose |
|---|---|---|
| `supabase-url-prod` | `SUPABASE_URL` | REST + Realtime endpoint. Points at `rico-prod`. |
| `supabase-anon-key-prod` | `SUPABASE_ANON_KEY` | For the future station-display SPA. Optional for the FastAPI backend. |
| `supabase-service-role-key-prod` | `SUPABASE_SERVICE_ROLE_KEY` | Server-side writes, bypasses RLS. FastAPI uses this. |
| `supabase-db-password-prod` | `SUPABASE_DB_PASSWORD` | Postgres password. For the asyncpg raw connection (Step 4 transaction). |

### Sandbox / dev testing (opt-in, temporary)

Use these ONLY during development. Once the app is validated, switch to
the production entries above.

| Vault secret name | Env var in the BLE app | Purpose |
|---|---|---|
| `supabase-url-dev` | `SUPABASE_URL` | Points at `rico-dev`. |
| `supabase-anon-key-dev` | `SUPABASE_ANON_KEY` | |
| `supabase-service-role-key-dev` | `SUPABASE_SERVICE_ROLE_KEY` | |
| `supabase-db-password-dev` | `SUPABASE_DB_PASSWORD` | |

`<project-ref>` = the subdomain of `SUPABASE_URL` (e.g.
`dldnnphcdztojkmdpkdl` for `rico-prod`).

**Use the transaction pooler on port 6543, not the direct 5432
connection.** Note the pooler host and username differ from the direct
connection — the username is `postgres.<project-ref>`, not `postgres`:

`postgresql://postgres.<ref>:<pw>@aws-0-<region>.pooler.supabase.com:6543/postgres`

(Copy the exact string from the Supabase dashboard → Project Settings →
Database → Connection string → "Transaction pooler"; the region segment
varies per project.)

**asyncpg + transaction pooler gotcha:** pgBouncer in transaction mode
does not support prepared statements, which asyncpg caches by default.
You MUST disable the cache or every query errors:

```python
pool = await asyncpg.create_pool(dsn, statement_cache_size=0)
```

### Not from Key Vault (local config)

- `MQTT_BROKER` — already `192.168.2.40`
- `TAG_MAC_PREFIX` — already `DC0D30`

### What is NOT needed

- NetSuite creds — that path is being removed.
- MSAL / Graph creds — no Microsoft-auth flow in this app.
- The `supabase-nonprod-*` secrets — the staging/devsite Supabase pair.
  BLE tracker doesn't touch it.

---

## Prerequisites

1. Agent has read access to `ricoincbikeyvault`. (Cliff to verify.)
2. The FastAPI host machine (on the factory LAN) can reach
   `*.supabase.co` on TCP 443. Test against the prod project ref:
   `curl -v https://dldnnphcdztojkmdpkdl.supabase.co`
3. Current SQLite app is fully working. Snapshot `ble_tracking.db` —
   that's the rollback point.

---

## The applied schema (reference)

Nothing here for the agent to run — this is the schema they'll be coding
against.

```sql
-- platform_users (already existed; only ble_tag_id is new)
alter table platform_users
  add column ble_tag_id text unique;
create index platform_users_ble_tag_id_idx
  on platform_users(ble_tag_id) where ble_tag_id is not null;

-- NetSuite Production Station mirror (refreshed twice a day by
-- apps/api's sweep-production-stations.ts). BLE app READS this — never
-- writes to it. The sync job TRUNCATEs and reloads (small table); it
-- MUST run truncate + insert inside one transaction so readers never
-- observe the empty mid-sync state. Because it truncates, NO other
-- table may hold a foreign key to it (see location_id below).
create table netsuite_production_stations (
  ns_internal_id  bigint primary key,          -- NS record internal id
  name            text,                         -- NS record name
  bt_mac_address  text unique,                  -- custrecord_bt_mac_address
  is_inactive     boolean not null default false,
  raw             jsonb not null,               -- full NS row (for future fields)
  synced_at       timestamptz not null default now()
);
create index netsuite_production_stations_mac_idx
  on netsuite_production_stations(bt_mac_address)
  where bt_mac_address is not null;

-- location_log
create table location_log (
  id                    bigint generated always as identity primary key,
  employee_id           uuid not null references platform_users(id) on delete restrict,
  employee_name         text,        -- snapshot at insert; the app must fill
  employee_department   text,        -- snapshot at insert; the app must fill
  location_id           bigint,      -- NS station internal id, SNAPSHOT ONLY (no FK)
  location_name         text,        -- snapshot at insert; NULL if station lookup missed
  receiver_mac          text,        -- physical BLE receiver MAC; always available
  timestamp_in          timestamptz not null default now(),
  timestamp_out         timestamptz
);
create index location_log_receiver_mac_idx on location_log(receiver_mac);

-- NOTE: location_id intentionally has NO foreign key to
-- netsuite_production_stations. That mirror is truncate-and-reloaded
-- twice daily; an inbound FK would either block the TRUNCATE (RESTRICT)
-- or null out historical location_ids on every reload (SET NULL). We
-- treat location_id as a snapshot of the NS internal id — self-describing
-- alongside location_name + receiver_mac, no referential integrity needed.

-- Hot paths
create index location_log_open_by_employee_idx
  on location_log(employee_id) where timestamp_out is null;
create index location_log_open_by_location_idx
  on location_log(location_id) where timestamp_out is null;
create index location_log_employee_time_idx
  on location_log(employee_id, timestamp_in desc);

-- Realtime
alter publication supabase_realtime add table location_log;
alter publication supabase_realtime add table platform_users;
```

Migrations are in rico-platform:
- `supabase/migrations/20260701140000_ble_tracker_tables.sql` (initial)
- `supabase/migrations/20260701150000_ble_tracker_stable_history.sql` (RESTRICT + employee snapshot)
- `supabase/migrations/20260701160000_platform_users_realtime.sql` (Realtime on platform_users)
- `supabase/migrations/20260702120000_ns_production_stations.sql` (drop `locations`, add NS mirror)
- `supabase/migrations/20260702130000_location_log_station_snapshot.sql` (make `location_id` nullable; add `location_name` + `receiver_mac` snapshot columns + `receiver_mac` index)
- `supabase/migrations/20260702140000_location_log_drop_station_fk.sql` (drop the station FK entirely — mirror is truncate-reloaded twice daily, so the FK's `ON DELETE RESTRICT` would break every sweep the moment any `location_log` row referenced a station. Verified reproducible on 2026-07-02. `location_id` stays as a plain bigint snapshot next to `location_name` + `receiver_mac`.)

---

## FastAPI rewrite — step by step

### Step 1 — Dependencies

Add to `server/requirements.txt`:

```
supabase>=2.7
asyncpg>=0.29
```

Keep `paho-mqtt`. Drop `sqlite3` (stdlib, but no longer used).

### Step 2 — Env plumbing

Read from env at startup:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_PASSWORD` (only if using asyncpg for the transaction)
- `MQTT_BROKER` (unchanged)

Two clients:
- `supabase-py` client for high-level table ops (upsert, select, broadcast)
- `asyncpg` connection pool for the `_log_transition` transaction
  (`supabase-py` doesn't expose transactions)

### Step 3 — Employee lookup (in-memory cache, Realtime-refreshed)

The FastAPI server runs on the factory LAN; Supabase is cloud. We can't
afford a WAN round-trip per BLE reading — instead, cache the
`tag_id → {id, name, department}` map in memory and refresh it via
Supabase Realtime whenever an admin edits `platform_users` in the
rico-platform UI.

**3a. Bulk-load on startup:**

```python
tag_to_employee: dict[str, dict] = {}

def rebuild_tag_map():
    rows = supabase.table("platform_users") \
        .select("id, first_name, last_name, department, ble_tag_id") \
        .not_.is_("ble_tag_id", "null") \
        .execute().data
    tag_to_employee.clear()
    for r in rows:
        tag = (r["ble_tag_id"] or "").upper()
        if not tag: continue
        tag_to_employee[tag] = {
            "id": r["id"],
            "name": f"{r['first_name']} {r['last_name']}".strip(),
            "department": r["department"],
        }
```

Call `rebuild_tag_map()` at app boot. One Supabase call, ~200ms for
500 employees. Every subsequent reading is a dict lookup.

**3b. Realtime subscription — reacts to admin edits within ~1s:**

Enable Realtime on `platform_users` in the Supabase dashboard (Database
→ Replication → check `platform_users` for `supabase_realtime`). Then:

```python
def on_platform_users_change(payload):
    old = payload.get("old") or {}
    new = payload.get("new") or {}
    old_tag = (old.get("ble_tag_id") or "").upper()
    new_tag = (new.get("ble_tag_id") or "").upper()
    if old_tag and old_tag != new_tag:
        tag_to_employee.pop(old_tag, None)
    if new_tag:
        tag_to_employee[new_tag] = {
            "id": new["id"],
            "name": f"{new['first_name']} {new['last_name']}".strip(),
            "department": new["department"],
        }

supabase.channel("platform_users_changes") \
  .on_postgres_changes(
      event="*", schema="public", table="platform_users",
      callback=on_platform_users_change,
  ) \
  .subscribe()
```

**3c. Periodic full reload (belt & suspenders):**

Every 5 minutes, call `rebuild_tag_map()` again. Cheap. Recovers from
any missed Realtime events (channel reconnects, WAN blips).

**3d. Reading path — pure in-memory lookup:**

```python
def resolve_employee(tag_mac: str):
    entry = tag_to_employee.get(tag_mac.upper())
    if not entry:
        # Tag not assigned to anyone — skip this reading
        return None
    return entry  # {id, name, department}
```

No network call per reading. The name snapshot written into
`location_log` comes straight from this cache, so historical rows have
the correct name even if the tag is later reassigned.

### Step 4a — Station lookup (in-memory, refreshed hourly)

Just like tags, station data is cached in memory. The mirror table
`netsuite_production_stations` is refreshed twice a day by
rico-platform's sync job — so the local BLE app only needs to reload
its cache periodically (hourly is fine — mirror refreshes are slower
than that anyway).

```python
receiver_to_station: dict[str, dict] = {}   # bt_mac_address -> {id, name}

def rebuild_station_map():
    rows = supabase.table("netsuite_production_stations") \
        .select("ns_internal_id, name, bt_mac_address, is_inactive, raw") \
        .eq("is_inactive", False) \
        .not_.is_("bt_mac_address", "null") \
        .execute().data
    receiver_to_station.clear()
    for r in rows:
        mac = (r["bt_mac_address"] or "").upper()
        if not mac: continue
        receiver_to_station[mac] = {
            "id": r["ns_internal_id"],
            "name": r["name"],
        }
```

Call `rebuild_station_map()` at boot and every hour.

### Step 4b — `_log_transition` (must be one transaction, tolerates missing station)

Prior schema wrote close-old then insert-new sequentially. With multiple
workers or Realtime retries, that can produce duplicate open rows. Wrap
in a Postgres transaction via asyncpg. Snapshots employee name +
department + station name + receiver MAC; uses the NS station internal
id (from the map above) as `location_id` when present.

**A missing station must NEVER cause the write to fail.** If a receiver
is powered on but its MAC hasn't been assigned to a NS station yet, the
transition still writes — with `location_id = NULL`, `location_name =
NULL`, and `receiver_mac` still populated. Ops can spot orphaned
receivers by querying `where location_id is null` and go set the MAC in
NS; the next sync brings the station in. History from that gap is not
lost.

```python
def resolve_station(receiver_mac: str):
    return receiver_to_station.get(receiver_mac.upper())

# In the confirmed-transition handler:
station = resolve_station(reading.receiver_mac)     # may be None
location_id = station["id"] if station else None
location_name = station["name"] if station else None

async with pool.acquire() as conn:
    async with conn.transaction():
        await conn.execute(
            "UPDATE location_log SET timestamp_out = now() "
            "WHERE employee_id = $1 AND timestamp_out IS NULL",
            employee_id,
        )
        await conn.execute(
            "INSERT INTO location_log "
            "(employee_id, employee_name, employee_department, "
            " location_id, location_name, receiver_mac, timestamp_in) "
            "VALUES ($1, $2, $3, $4, $5, $6, now())",
            employee_id, display_name, department,
            location_id, location_name, reading.receiver_mac.upper(),
        )
```

Skipping the employee snapshot (`employee_name` NULL) still leaves the
FK to resolve the name via join, but a future employee delete would
leave that row nameless. Same for `location_name`: NULL is only correct
when the station itself doesn't exist yet — otherwise always snapshot.

**Engine transition key (orphan-safe).** The location engine keys its
`current_locations` / `candidates` on the **resolved station id when one
exists, otherwise the `receiver_mac`**. If it keyed on `location_id`
alone, two not-yet-provisioned antennas would both read as `NULL`, and a
person walking from one to the other would produce no new row — leaving a
stale `receiver_mac` on their open row and losing that movement for later
backfill. Keying on `location_id or receiver_mac` keeps orphan-to-orphan
moves distinct and correctly attributed.

### Step 5 — Realtime broadcast on confirmed transition

After the transaction commits, publish a broadcast payload on the
station's channel — but only if we resolved a station. Orphan-receiver
readings (no station in NS yet) still get durable rows in `location_log`
via Step 4b; they just don't broadcast to a station display until the
NS record catches up.

```python
if location_id is not None:
    supabase.channel(f"station:{location_id}").send({
        "type": "broadcast",
        "event": "arrival",
        "payload": {
            "user_id": str(employee_id),
            "user_name": display_name,
            "avatar_url": avatar_url,   # from platform_users if you fetched it
            "department": department,
            "at": iso_timestamp,
        },
    })
```

Broadcast is fire-and-forget — failures should log but not block the
transition. The transition is durable in `location_log`.

Verify with a small Python subscriber:
```python
supabase.channel("station:1") \
    .on_broadcast("arrival", lambda p: print(p)) \
    .subscribe()
```

### Step 6 — Drop the removed surfaces

There is no `templates/` directory — all HTML is inline f-strings in
`server/server.py`. Remove, in that file:

- `/api/netsuite/pending` and `/api/netsuite/mark-synced` routes, and the
  `synced_to_netsuite` column / "Synced" column in the history view.
- The local `employees` table and all its admin surface: the
  `/admin/employees*` routes and the employee add/edit forms (including
  the `netsuite_employee_id` field). Identity now comes from
  `platform_users` via the cache (Step 3).
- The `locations` table is gone. Rework or remove `/admin/locations*`
  (it edited the local table) and `/api/flash/assign` (it wrote
  `locations.receiver_mac`). Station↔MAC binding now lives in NetSuite —
  see open decision #3 for the recommended `/admin/flash` changes.
- `init_db()` / `seed_demo_data()` and the `NAV_HTML` links for any
  removed pages.

### Step 7 — Drop or localize `raw_readings`

500K rows/day is too much to push to Supabase. Options:

- (Preferred) Drop entirely.
- Keep local: retain SQLite JUST for `raw_readings`. Nothing else uses
  that DB anymore.

### Step 8 — Restart safety

On startup, rehydrate `engine.current_locations` from Supabase, keyed by
employee UUID and using the same orphan-safe key as the engine
(`location_id` when set, else `receiver_mac`):

```python
rows = supabase.table("location_log") \
    .select("employee_id, location_id, receiver_mac") \
    .is_("timestamp_out", "null") \
    .execute().data
for r in rows:
    key = r["location_id"] if r["location_id"] is not None else r["receiver_mac"]
    engine.current_locations[r["employee_id"]] = key
```

The tag → employee map is NOT rebuilt lazily — it is bulk-loaded at boot
and kept warm via Realtime + a 5-minute reload (see Step 3). The station
map is likewise loaded at boot and hourly (Step 4a).

### Step 9 — Backfill (production cutover only — sandbox skips)

For production cutover, one-time backfill script:

1. Read `employees.tag_id` from the SQLite snapshot, match to
   `platform_users` by `netsuite_employee_id`, write `ble_tag_id` into
   `platform_users` via the admin API or a direct SQL update.
2. Read the last N days of `location_log` from SQLite, translate
   `employee_id` (int) → `platform_users.id` (uuid), fetch the
   corresponding first/last name + department from `platform_users`,
   insert into Supabase `location_log` with the same timestamps AND
   the name snapshot columns populated.
3. Employees without a `netsuite_employee_id` match get flagged; Jay or
   Ricardo decides whether to create a `platform_users` row for them or
   skip.

Run against `rico-dev` first with a copy of the SQLite DB, to prove the
mapping. Do not run against prod until the mapping report is reviewed.

### Step 10 — Runtime testing (sandbox / rico-dev)

For sandbox testing point the FastAPI server at rico-dev Supabase
(the *-dev vault entries above). Walk a tag through three receivers,
verify:

- `location_log` opens/closes correctly (one open row per employee)
- Every new row has `employee_name` filled in
- Every new row has `receiver_mac` filled in (even when the station
  lookup misses, the physical MAC is still recorded)
- A receiver whose MAC isn't yet assigned in NS still produces a row
  with `location_id = NULL` and `location_name = NULL` — writes never
  fail on a missing station
- Walking a tag between two receivers that are BOTH unassigned in NS
  produces two separate rows with different `receiver_mac` values — the
  orphan-safe engine key doesn't collapse orphan-to-orphan moves
- Realtime broadcast fires within ~1s of confirmed transition
- Server restart preserves `current_locations` (from open rows)
- Two rapid transitions on the same employee do NOT leave two open rows
- Assigning a tag to a different employee in the admin UI, then walking
  that tag, correctly records new rows under the new employee — old
  rows still have the old name snapshot

### Step 11 — Docs

Rewrite `docs/DATA_FLOW.md` (done alongside this doc):
- Replace the SQLite stage block with Supabase (Postgres, cloud).
- Employee identity from `platform_users` via the in-memory cache; no
  local `employees` table.
- Station identity from the `netsuite_production_stations` mirror via the
  station cache; no local `locations` table.
- Add the orphan-receiver path (nullable `location_id`, always-filled
  `receiver_mac`, orphan-safe engine key).
- Add the Realtime broadcast section (broadcast only when a station
  resolves).
- Drop the historical NetSuite push section.
- Update the data-model / ERD — `platform_users`,
  `netsuite_production_stations`, `location_log` (with snapshot columns);
  no `raw_readings`, no `employees`, no `locations`.

---

## Rollback

Sandbox rollback is free — point FastAPI back at the SQLite file.

Prod rollback (once cut over):
1. Point FastAPI back at SQLite snapshot
2. Note that any Supabase-only history written after cutover is lost

Keep the SQLite snapshot for 30 days post-cutover.

---

## Open decisions to raise before starting

1. **Realtime broadcast channel naming** — `station:{ns_internal_id}`
   per this doc (uses the NS station id, since the local `locations`
   table is gone). Confirm displays will be one-per-station.
2. **Existing employees without `netsuite_employee_id`** — do we
   create `platform_users` rows for them (production floor workers who
   PIN-auth), or leave them out of the tracker entirely?
3. **Receiver ↔ station binding.** Today's `/admin/flash/assign`
   endpoint used to write `receiver_mac` into the local SQLite
   `locations` table. Options going forward:
   - (Recommended) Drop `/admin/flash/assign`. Ops updates
     `custrecord_bt_mac_address` on the NS station record directly;
     the twice-daily sync propagates. Keeps single-source-of-truth
     integrity, but there's up to a 12h delay from NS edit to BLE app
     seeing it.
   - Keep `/admin/flash` for the FLASH step (upload firmware, capture
     MAC) but drop the assign step. Print the captured MAC on-screen
     so ops can paste into NS.
   - Add a "sync now" button somewhere in rico-platform that force-
     runs the mirror job on demand (skips waiting 12h).
4. **Realtime authorization** — station display SPA will need to
   subscribe. Anon key with a permissive Realtime policy vs a
   per-station JWT. Deferred until the display is built.
5. **`raw_readings` retention** — recommendation: drop entirely.
   Confirm before deleting the table.

---

## Timeline estimate

Assuming one agent, no LAN/permission surprises:

- Steps 1-3 (deps + env + lookups): half a day
- Steps 4-6 (server rewrite): 1-2 days
- Step 10 (sandbox testing): half a day
- Docs + prod cutover coordination: half a day

**2.5-3.5 days to sandbox-proven.** Prod cutover is a separate
coordination task once sandbox has been running a few days without
incident.
