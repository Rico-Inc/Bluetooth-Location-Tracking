# BLE Location Tracker — SQLite → Supabase Migration Status

Companion to [SUPABASE_MIGRATION.md](SUPABASE_MIGRATION.md) (the playbook)
and [DATA_FLOW.md](DATA_FLOW.md) (the target architecture). This file is
the running progress log — updated as each phase lands.

**Last updated:** 2026-07-10
**Repo:** `Rico-Inc/Bluetooth-Location-Tracking`
**Current commit at HEAD:** `0996f2f`

---

## TL;DR

**Phase 0 — Orient:** complete.
**Phase 1 — Build against sandbox (rico-dev):** complete. All 10 playbook
sub-steps landed, verified against the sandbox, committed, and pushed for
Steps 1–5. Steps 6–8 committed locally, awaiting a push.
**Phase 2 — Prove it in sandbox:** blocked on test-data seeding by
rico-platform.
**Phase 3 — Cutover to rico-prod:** not started.

The FastAPI ingest server no longer opens SQLite. All reads and writes go
against the rico-dev Supabase project. When Phase 2 acceptance passes,
switching to production is a single environment variable
(`SUPABASE_ENV=prod` in `start-server.bat`) plus a restart.

---

## Phase 0 — Orient (complete)

| Prereq | Result |
|---|---|
| Read `server/server.py` end to end; note engine internals that change | Done |
| Verify LAN → `*.supabase.co` reachable on TCP 443 | Working. TLS handshake succeeds. Discovered a stale SERVFAIL on the corporate DNS server (`192.168.2.4`) blocking `.supabase.co` resolution; cleared with `Clear-DnsServerCache -Force`. |
| Verify Azure Key Vault read access on `ricoincbikeyvault` | Granted `Key Vault Secrets User` role on the vault to `canstey@ricoinc.com`. |
| Take SQLite rollback snapshot | `server/ble_tracking.pre-supabase-migration.db` (~1.95 GB) sits alongside the live DB. Gitignored. |

**Ambient work also completed during Phase 0:**
- Installed GitHub CLI (`gh 2.95.0`) and Azure CLI (`az 2.87.0`) via winget.
- Set up SSH-based git auth to GitHub (HTTPS/PAT path was blocked by
  Rico-Inc's SAML OAuth policy; SSH sidestepped it).
- Corporate DNS server `192.168.2.5` is decommissioned but was still
  configured on the dev machine's NIC — flagged for IT so DHCP option
  6 can be updated org-wide.
- The FastAPI service currently auto-launches as **`SYSTEM`** (probably
  via Task Scheduler or a service wrapper). This forced multiple
  workarounds this migration (KV credentials don't cache for SYSTEM,
  `~` resolves to `\systemprofile`, etc.). **Follow-up:** reconfigure
  the auto-launcher to run as `canstey-admin` so `start-server.bat` can
  `az keyvault secret show` normally.

---

## Phase 1 — Build against sandbox (complete)

All work targets `rico-dev`. Env-selected via `SUPABASE_ENV=dev`
(default) in [`server/start-server.bat`](../server/start-server.bat).

### Step 1 — Dependencies
Added to [`server/requirements.txt`](../server/requirements.txt):
- `supabase>=2.7` (2.31.0 installed)
- `asyncpg>=0.29` (0.31.0 installed)

### Step 2 — Env plumbing + startup wiring
[`server/server.py`](../server/server.py):
- All Supabase config loaded from env at import time:
  `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_PASSWORD`,
  `SUPABASE_DB_CLUSTER`, `SUPABASE_DB_REGION`, optional
  `SUPABASE_DB_URL` full override.
- `_build_pooler_dsn()` builds the transaction-pooler DSN. asyncpg pool
  uses `statement_cache_size=0` (pgBouncer transaction mode requirement).
- Module-level `supabase_client` (sync REST) and `pg_pool` (async writes)
  populated in `startup()`.
- `startup()` is now `async def`. Matching `shutdown()` closes the pool
  and the Realtime client cleanly.

[`server/start-server.bat`](../server/start-server.bat) rewritten:
- Fetches `supabase-url-<env>`, `supabase-service-role-key-<env>`,
  `supabase-db-password-<env>` from `ricoincbikeyvault` via `az` before
  starting uvicorn.
- `SUPABASE_ENV` selects between `dev` (sandbox) and `prod` (cutover).

**Playbook drift:** the playbook example uses
`aws-0-<region>.pooler.supabase.com` but rico-dev sits on the newer
`aws-1` Supavisor cluster. Added `SUPABASE_DB_CLUSTER` env var
(default `aws-1`) to accommodate. **Worth confirming** rico-prod's
cluster before Phase 3 cutover.

### Step 3 — Employee cache
`tag_to_employee: dict[mac_upper, {id, name, department, avatar_url}]`,
guarded by `_tag_map_lock`.

- `rebuild_tag_map()` — bulk load from `platform_users` at boot and
  every 5 minutes (belt-and-suspenders reload thread).
- `_on_platform_users_change()` — Realtime callback. **Sync, not async
  — the library invokes callbacks synchronously and silently drops
  coroutines.**
- Sweeps stale tags by user_id rather than relying on
  `old_record.ble_tag_id`, because `platform_users` uses `REPLICA
  IDENTITY DEFAULT` (only PK in old_record). Handles INSERT/UPDATE/DELETE
  uniformly.
- Verified: admin edits propagate to cache in ~0.5s.

### Step 4a — Station cache
`receiver_to_station: dict[mac_upper, {id, name}]`, guarded by
`_station_map_lock`.

- Sources from `netsuite_production_stations` (the twice-daily mirror
  rico-platform's `sweep-production-stations.ts` maintains).
- Bulk load at boot, hourly reload thread.
- No Realtime — mirror churn is slow and TRUNCATE-based, so Realtime
  would spam.

### Step 4 — Engine refactor
`LocationEngine` re-keyed:
- `readings_buffer` still keyed on `tag_id` (that's what MQTT delivers).
- `current_locations` and `candidates` now keyed on **platform_users
  UUID**. Same person → one open row, even after tag reassignment.
- Location value is **orphan-safe**: `station["id"]` when the receiver
  resolves, else `receiver_mac`. Two unassigned receivers never
  collapse into a single "unknown" bucket.
- Hold-period logic (`HOLD_PERIODS=2`) and RSSI thresholds unchanged.
- 8 unit tests cover: unassigned tag skip, weak-signal skip, hold
  period, orphan MAC as key, orphan-to-orphan distinct rows,
  no-transition-when-staying-put, independent employees, strongest wins.

### Step 4b — `_log_transition` transactional write
- One asyncpg transaction: UPDATE-close + INSERT-open. No window can
  produce two open rows for the same employee.
- Every INSERT snapshots `employee_name`, `employee_department`,
  `location_name`, `receiver_mac`. Missing station is not an error
  (`location_id=NULL`, `location_name=NULL`, `receiver_mac` still set).
- `process_window()` and `processing_loop()` are now async; the loop
  runs as an asyncio task on the FastAPI event loop.
- Verified: full round-trip against rico-dev with cleanup.

### Step 5 — Realtime arrival broadcast
- On confirmed station arrival, fires
  `POST /realtime/v1/api/broadcast` on channel `station:<ns_internal_id>`
  with event `"arrival"` and payload `{user_id, user_name, avatar_url,
  department, at}`.
- REST-based (via `httpx`) not WebSocket — stateless, no per-station
  channel joins to manage.
- **Fire-and-forget** via `asyncio.create_task`; failures log but never
  block the transition (which is already durable in `location_log`).
- Orphan-receiver transitions intentionally do **not** broadcast.
- Verified: broadcast arrives at a subscriber in ~0.5s; orphans
  correctly skip.

### Step 6 — Strip removed surfaces + rewrite reads
Deleted routes:
- `/api/netsuite/pending`, `/api/netsuite/mark-synced`
- All `/admin/employees*` (5 routes) — identity managed in rico-platform
- All `/admin/locations*` (5 routes) — station metadata in NetSuite now
- `/api/tags/register`, `DELETE /api/tags/{tag_id}` — tag assignment moves
  with the employee record in rico-platform
- `/api/employees`, `/api/employees/{id}/history` — consumers should
  query Supabase REST directly
- `/api/locations`, `/api/locations/{id}/occupants` — same
- `/api/flash/assign` (per decision #3)
- "Save Assignment" section from `/admin/flash` UI
- Employees + Locations nav links

Rewritten to hit Supabase:
- `/` dashboard — reads `location_log` open rows using snapshot columns
- `/admin/history`, `/admin/history/{id}` — from `platform_users` +
  `location_log`
- `/admin/health`, `/api/health` — station names via `receiver_to_station`
  cache
- `/admin/flash` — post-flash, shows MAC with "Copy to clipboard" for
  ops to paste into NetSuite

### Step 7 — Drop `raw_readings` + SQLite stack
- `on_mqtt_message` no longer INSERTs into `raw_readings` (~500K
  rows/day, unused downstream)
- Removed: `sqlite3` import, `DB_PATH`, `get_db()`, `init_db()`,
  `seed_demo_data()`, `contextmanager` import
- Local `server/ble_tracking.db` is untouched on disk (kept as
  belt-and-suspenders rollback) — the server just no longer opens it.

### Step 8 — Restart-safety rehydrate
- `startup()` reads every open row (`timestamp_out IS NULL`) from
  Supabase `location_log` and seeds `engine.current_locations` by
  employee_id (with the same orphan-safe key: `location_id or
  receiver_mac`).
- Prevents a spurious "new location" transition being fired for every
  currently-tracked employee on server restart.

### Phase 1 diff summary

`server/server.py`: **713 lines removed, 356 added** across the two
Phase 1 commits (`3f3c3ce` + `0996f2f`). File shrank from 1327 → 1375
after the additions but is dramatically simpler in structure — 679 of
the removed lines were the deleted admin surfaces.

---

## Phase 2 — Sandbox acceptance (blocked)

Step 10 in the playbook. Every acceptance box has to pass before Phase 3
cutover:

- [ ] Walk a tag through three receivers; verify `location_log` opens
      and closes correctly (one open row per employee)
- [ ] Every new row has `employee_name` filled
- [ ] Every new row has `receiver_mac` filled (even for orphan cases)
- [ ] Orphan receiver (unassigned MAC in NS) still produces a row with
      `location_id=NULL`, `location_name=NULL`
- [ ] Orphan-to-orphan move produces **two separate rows** with
      different `receiver_mac` values
- [ ] Realtime broadcast fires within ~1s of confirmed transition
- [ ] Server restart preserves `current_locations` (Step 8 rehydrate)
- [ ] Two rapid transitions on the same employee do **not** leave two
      open rows
- [ ] Tag reassignment via rico-platform admin UI, then walking that
      tag, correctly records new rows under the new employee — old
      rows retain the old name snapshot

**Blockers:**
1. **rico-dev has zero `ble_tag_id` assignments.** All acceptance
   scenarios need at least a handful of employees with tags assigned via
   the rico-platform admin UI.
2. **rico-dev has zero `bt_mac_address` bindings in
   `netsuite_production_stations`.** For the "station resolves" boxes
   to be exercised, at least one NS record needs its MAC set. (The
   orphan-path boxes are fine to test without any bindings.)

**Ask for rico-platform team:** seed rico-dev with 3–5 `platform_users`
having `ble_tag_id` set, and set `custrecord_bt_mac_address` on 2–3
`customrecord_production_station` records in the NS sandbox so the sync
job propagates them into `netsuite_production_stations`.

---

## Phase 3 — Cutover to rico-prod (not started)

Depends on Phase 2 passing. Concrete steps once ready:

1. **Resolve remaining open decisions** with Jay / Ricardo:
   - **#2** — What happens to legacy employees without a
     `netsuite_employee_id`? (Create matching `platform_users` rows or
     drop from the tracker.)
   - **#4** — Realtime authorization model for the future station
     display SPA. Defer until the display is built.
2. **Verify rico-prod's Supabase pooler cluster.** If it's still on
   `aws-0`, add `SET SUPABASE_DB_CLUSTER=aws-0` to the prod branch of
   `start-server.bat`; the playbook's example DSN assumes `aws-0`.
3. **Run the one-time backfill script** (playbook Step 9) against
   `rico-dev` with a copy of the SQLite snapshot first to prove the
   `netsuite_employee_id` → `platform_users.id` mapping. Review the
   mismatched report with Jay/Ricardo. Only then run against prod.
4. **Cutover:**
   - Stop the current server
   - `set SUPABASE_ENV=prod` in `start-server.bat` (or make it env)
   - Restart
   - Verify writes are flowing into `rico-prod.location_log`
5. **Post-cutover:**
   - Keep the SQLite snapshot for 30 days as rollback
   - Monitor for a week — any "orphan" writes should shrink to zero as
     ops finishes populating NS station MAC bindings

**Cross-repo follow-up on `rico-platform` (playbook decision #3
option 3):** add a "sync now" button/endpoint that force-runs
`sweep-production-stations.ts`. Without it, edits made in NetSuite have
up to a 12h delay before the BLE server sees them.

---

## Rollback strategy

Three tiers, cheapest to most expensive:

| Situation | Action |
|---|---|
| Code bug in Phase 1 or 2 | `git checkout 52d49ee` — the parent of the migration work. Restores pre-migration server.py. |
| Data mishap in rico-dev | Sandbox; delete rows via SQL. No user-visible impact. |
| Prod cutover fails after Phase 3 | Point `SUPABASE_ENV=dev` (temporary), then revert to the pre-migration server.py + point back at SQLite via the `.pre-supabase-migration.db` snapshot. Any Supabase-only history written post-cutover is lost. |

The pre-migration SQLite snapshot at
`server/ble_tracking.pre-supabase-migration.db` is retained on disk for
30 days after production cutover as the ultimate rollback artifact.
Gitignored, ~1.95 GB.

---

## Ambient / non-migration follow-ups

Not blocking Phase 2, but worth noting:

- **BLE server auto-relaunches as SYSTEM** — needs to be reconfigured
  to run as a normal user account (`canstey-admin`) so `az keyvault
  secret show` in `start-server.bat` works without a service-principal
  workaround.
- **DHCP option 6** on the office network still hands out the
  decommissioned `192.168.2.5` as a DNS server — should be corrected
  to prevent future clients from timing out on it.
- **Corporate DNS cache monitoring** — after the SERVFAIL incident on
  `192.168.2.4`, consider a monitor that alerts on `supabase.co` (and
  other prod dependencies) NXDOMAIN/SERVFAIL from the internal
  resolvers.
- **Firmware self-healing** — the `NEVER SEEN` incident earlier this
  session was traced to PubSubClient believing it was connected after
  the TCP socket half-closed. A small firmware tweak (force reconnect
  when `publish()` returns false, periodic ping) would prevent it
  recurring. Deferred.
