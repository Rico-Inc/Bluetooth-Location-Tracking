"""
BLE Employee Location Tracking Server
======================================
FastAPI + MQTT + Supabase (Postgres via transaction pooler)

Env vars required at boot (mid-migration; SQLite path is still present):
  SUPABASE_URL                — https://<ref>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY   — server-side, bypasses RLS
  SUPABASE_DB_PASSWORD        — postgres password (used for asyncpg pooler DSN)
  SUPABASE_DB_CLUSTER         — pooler cluster prefix, default "aws-1"
  SUPABASE_DB_REGION          — pooler region segment, default "us-east-1"
  SUPABASE_DB_URL             — full pooler DSN override; if set, used verbatim

  MQTT_BROKER                 — default "localhost"

Populate these from Azure Key Vault (`ricoincbikeyvault`) via start-server.bat
or an equivalent shell wrapper. Never commit values to disk.

Run:
  1. Start Mosquitto:  mosquitto -v
  2. Start server:     uvicorn server:app --reload --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import os
from urllib.parse import urlparse
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import asyncpg
import httpx
import serial.tools.list_ports
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from supabase import create_client, Client
from realtime import AsyncRealtimeClient
import paho.mqtt.client as mqtt

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = 1883
MQTT_TOPIC = "ble/readings"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_DB_PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "").strip()
SUPABASE_DB_CLUSTER = os.environ.get("SUPABASE_DB_CLUSTER", "aws-1").strip()
SUPABASE_DB_REGION = os.environ.get("SUPABASE_DB_REGION", "us-east-1").strip()
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "").strip()

FIRMWARE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "firmware"))

# Location engine settings
WINDOW_SECONDS = 60           # 60-second averaging window
HOLD_PERIODS = 2              # Must hold for 2 consecutive periods (2 min)
RSSI_THRESHOLD_STRONG = -65   # Definitely here
RSSI_THRESHOLD_WEAK = -80     # Passing through / gone

# Receiver health
RECEIVER_TIMEOUT_SECONDS = 600  # 10 min with no data = offline

# Module-level Supabase/Postgres handles; populated in startup()
supabase_client: Client | None = None
pg_pool: asyncpg.Pool | None = None


def _build_pooler_dsn() -> str:
    """Assemble the transaction-pooler DSN Supabase expects for asyncpg.

    Prefers SUPABASE_DB_URL if set (copied from Supabase dashboard).
    Otherwise constructs from SUPABASE_URL + SUPABASE_DB_PASSWORD + region.
    Uses `statement_cache_size=0` at pool-create time — pgBouncer transaction
    mode does not support prepared statements.
    """
    if SUPABASE_DB_URL:
        return SUPABASE_DB_URL
    host = urlparse(SUPABASE_URL).hostname or ""
    project_ref = host.split(".")[0]
    if not project_ref or not SUPABASE_DB_PASSWORD:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_DB_PASSWORD must be set "
            "(or set SUPABASE_DB_URL to the full pooler connection string)"
        )
    return (
        f"postgresql://postgres.{project_ref}:{SUPABASE_DB_PASSWORD}"
        f"@{SUPABASE_DB_CLUSTER}-{SUPABASE_DB_REGION}.pooler.supabase.com:6543/postgres"
    )


# ─────────────────────────────────────────────
# LOCATION ENGINE
# ─────────────────────────────────────────────
class LocationEngine:
    """
    Determines employee location using 'loudest signal wins' logic.

    Flow:
    1. MQTT thread buffers RSSI readings via add_reading(), keyed on tag_id.
    2. Every WINDOW_SECONDS, process_window() averages per receiver, picks
       the strongest, resolves tag→employee (Step 3 cache) and receiver→
       station (Step 4a cache), and tracks candidates.
    3. Candidate must hold for HOLD_PERIODS windows to become a confirmed
       transition, at which point _log_transition() records it.

    Key change vs. pre-migration:
      - current_locations / candidates keyed on EMPLOYEE UUID, not tag_id.
        Same person moving = one open row, even after tag reassignment.
      - Location value is orphan-safe: station_id if resolved, else the
        receiver_mac itself. Two unassigned receivers do not collapse.
    """

    def __init__(self):
        # Raw RSSI buffer stays keyed on tag_id — that's what MQTT delivers.
        # {tag_id: {receiver_mac: [rssi, rssi, ...]}}
        self.readings_buffer = defaultdict(lambda: defaultdict(list))

        # {employee_id (uuid str): location_key}
        # location_key = station["id"] (int) if the receiver is bound to a
        # station in netsuite_production_stations, else receiver_mac (str).
        self.current_locations: dict[str, object] = {}

        # {employee_id: {"location_key", "receiver_mac", "station", "employee", "count"}}
        self.candidates: dict[str, dict] = {}

        # Receiver health: {receiver_mac: last_seen_timestamp}
        self.receiver_health = {}
        # WiFi signal strength: {receiver_mac: rssi}
        self.receiver_wifi_rssi = {}
        self.lock = threading.Lock()

    def add_reading(self, receiver_mac, tag_id, rssi, timestamp):
        """Buffer an incoming RSSI reading (called from the MQTT thread)."""
        with self.lock:
            self.readings_buffer[tag_id][receiver_mac].append(rssi)
            self.receiver_health[receiver_mac] = timestamp

    async def process_window(self):
        """Called every WINDOW_SECONDS. Runs on the FastAPI asyncio loop."""
        with self.lock:
            buffer = dict(self.readings_buffer)
            self.readings_buffer = defaultdict(lambda: defaultdict(list))

        if not buffer:
            return

        for tag_id, receivers in buffer.items():
            emp = resolve_employee(tag_id)
            if emp is None:
                continue  # unassigned tag — skip silently
            employee_id = emp["id"]

            # Average RSSI per receiver, pick strongest (closest to 0)
            avg_rssi = {mac: sum(rl) / len(rl) for mac, rl in receivers.items()}
            strongest_mac = max(avg_rssi, key=avg_rssi.get)
            strongest_rssi = avg_rssi[strongest_mac]

            if strongest_rssi < RSSI_THRESHOLD_WEAK:
                continue  # signal too weak — probably passing through

            station = resolve_station(strongest_mac)
            # Orphan-safe key: station_id if we can resolve one, else the
            # MAC itself. Prevents two unassigned receivers from collapsing
            # into a single "unknown" location.
            candidate_key = station["id"] if station else strongest_mac.upper()

            current_key = self.current_locations.get(employee_id)
            if candidate_key == current_key:
                # Signal confirms current location — clear any prior candidate
                self.candidates.pop(employee_id, None)
                continue

            # Track candidate for hold period
            cand = self.candidates.get(employee_id)
            if cand and cand["location_key"] == candidate_key:
                cand["count"] += 1
                # keep the latest employee snapshot in case name/dept changed
                cand["employee"] = emp
            else:
                self.candidates[employee_id] = {
                    "location_key": candidate_key,
                    "receiver_mac": strongest_mac.upper(),
                    "station": station,      # None when orphan
                    "employee": emp,
                    "count": 1,
                }

            if self.candidates[employee_id]["count"] >= HOLD_PERIODS:
                confirmed = self.candidates.pop(employee_id)
                try:
                    await self._log_transition(confirmed, current_key)
                except Exception as exc:
                    # Log but don't crash the window; keep other employees' work.
                    # Re-adding to candidates would allow a retry next window, but
                    # for now we drop — the receiver will re-nominate on the next
                    # HOLD_PERIODS worth of readings.
                    print(f"[Engine] transition write failed for {confirmed['employee']['name']}: {exc}")

        print(f"[Engine] Processed window — {len(buffer)} tag(s) seen")

    async def _log_transition(self, cand, old_key):
        """Persist a confirmed location transition to Supabase.

        One transaction:
          1. Close any open row for this employee (timestamp_out = ts)
          2. Insert new row with timestamp_in = ts and all snapshot columns

        location_id / location_name are NULL when the receiver isn't bound
        to a NetSuite station (orphan). receiver_mac is always set — that's
        the debug trail for ops to reconcile once the NS record catches up.
        """
        if pg_pool is None:
            raise RuntimeError("_log_transition called before pg_pool initialized")

        emp = cand["employee"]
        station = cand["station"]
        receiver_mac = cand["receiver_mac"]
        location_id = station["id"] if station else None
        location_name = station["name"] if station else None
        ts = datetime.now(timezone.utc)

        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE location_log SET timestamp_out = $1 "
                    "WHERE employee_id = $2 AND timestamp_out IS NULL",
                    ts, emp["id"],
                )
                await conn.execute(
                    """
                    INSERT INTO location_log
                        (employee_id, employee_name, employee_department,
                         location_id, location_name, receiver_mac, timestamp_in)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    emp["id"], emp["name"], emp.get("department"),
                    location_id, location_name, receiver_mac, ts,
                )

        self.current_locations[emp["id"]] = cand["location_key"]
        where = (
            f"station #{station['id']} ({station['name']!r})"
            if station else f"orphan receiver {receiver_mac}"
        )
        print(
            f"[Engine] {emp['name']} ({emp['id'][:8]}...) transition: "
            f"{old_key or '<none>'} -> {cand['location_key']}  [{where}]"
        )

        # --- Step 5: Realtime arrival broadcast (fire-and-forget) --------
        # Only broadcast when a station resolves. Orphan-receiver rows are
        # durable in location_log, but there's no display to notify yet.
        if station is not None:
            asyncio.create_task(
                _broadcast_arrival(station["id"], emp, ts)
            )

    def get_receiver_status(self):
        """Return health status of all receivers."""
        now = datetime.now(timezone.utc)
        status = {}
        for mac, last_seen in self.receiver_health.items():
            try:
                ls = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                age = (now - ls).total_seconds()
                status[mac] = {
                    "last_seen": last_seen,
                    "status": "online" if age < RECEIVER_TIMEOUT_SECONDS else "offline",
                    "seconds_ago": int(age),
                }
            except Exception:
                status[mac] = {"last_seen": last_seen, "status": "unknown"}
        return status


# ─────────────────────────────────────────────
# EMPLOYEE CACHE (Step 3)
# ─────────────────────────────────────────────
# In-memory {tag_mac_upper: {id, name, department}} kept warm by:
#   - rebuild_tag_map()               bulk load at boot + every 5 min
#   - _on_platform_users_change()     Realtime callback (~1s on admin edits)
# The reading path (location engine) uses resolve_employee() — dict lookup,
# no network. Every INSERT into location_log snapshots name+department from
# this cache so history survives later tag reassignments or user deletes.

tag_to_employee: dict[str, dict] = {}
_tag_map_lock = threading.Lock()
_realtime_client: AsyncRealtimeClient | None = None


def _display_name(row: dict) -> str:
    first = (row.get("first_name") or "").strip()
    last = (row.get("last_name") or "").strip()
    return " ".join(p for p in (first, last) if p)


def rebuild_tag_map():
    """Full reload from platform_users. Called at boot and every 5 minutes."""
    if supabase_client is None:
        return
    resp = (
        supabase_client.table("platform_users")
        .select("id, first_name, last_name, department, avatar_url, ble_tag_id")
        .not_.is_("ble_tag_id", "null")
        .execute()
    )
    new_map: dict[str, dict] = {}
    for r in resp.data or []:
        tag = (r.get("ble_tag_id") or "").upper()
        if not tag:
            continue
        new_map[tag] = {
            "id": r["id"],
            "name": _display_name(r),
            "department": r.get("department"),
            "avatar_url": r.get("avatar_url"),
        }
    with _tag_map_lock:
        tag_to_employee.clear()
        tag_to_employee.update(new_map)
    print(f"[Cache] tag_to_employee reloaded — {len(new_map)} tag(s)")


def resolve_employee(tag_mac: str):
    """Return {id, name, department} or None if this tag is unassigned."""
    with _tag_map_lock:
        return tag_to_employee.get(tag_mac.upper())


def _on_platform_users_change(payload):
    """Realtime callback for INSERT/UPDATE/DELETE on platform_users.

    The realtime library invokes this synchronously — do NOT make it async
    (it silently swallows the coroutine). Only dict mutation under a lock
    happens here; no I/O, so sync is correct anyway.

    We can't rely on old_record.ble_tag_id: `platform_users` uses
    REPLICA IDENTITY DEFAULT (PK only), so old_record has just `id` on
    UPDATE/DELETE. Instead: sweep any stale tag pointing at this user_id
    and re-add the current mapping if a tag is set.
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    new = data.get("record") or {}
    old = data.get("old_record") or {}
    user_id = new.get("id") or old.get("id")
    if not user_id:
        return
    new_tag = (new.get("ble_tag_id") or "").upper()

    with _tag_map_lock:
        # Drop any existing tag that maps to this user (handles tag change,
        # tag clear, and user delete uniformly).
        stale = [t for t, emp in tag_to_employee.items() if emp["id"] == user_id and t != new_tag]
        for t in stale:
            tag_to_employee.pop(t, None)
        # Add / refresh the current mapping if the user still has a tag.
        if new_tag:
            tag_to_employee[new_tag] = {
                "id": user_id,
                "name": _display_name(new),
                "department": new.get("department"),
                "avatar_url": new.get("avatar_url"),
            }


async def _start_platform_users_realtime():
    """Open the Realtime WebSocket and subscribe to platform_users changes.

    Runs the client's listen/heartbeat loops as background tasks on the
    FastAPI event loop; auto-reconnect is on by default. If a WS blip drops
    events, the 5-minute reload loop catches them.
    """
    global _realtime_client
    # AsyncRealtimeClient tacks "/websocket" onto whatever URL we pass — Supabase's
    # Realtime endpoint lives under /realtime/v1, so pre-join that here.
    realtime_url = SUPABASE_URL.rstrip("/") + "/realtime/v1"
    _realtime_client = AsyncRealtimeClient(realtime_url, token=SUPABASE_SERVICE_ROLE_KEY)
    await _realtime_client.connect()
    channel = _realtime_client.channel("platform_users_changes")
    await (
        channel
        .on_postgres_changes(
            event="*",
            schema="public",
            table="platform_users",
            callback=_on_platform_users_change,
        )
        .subscribe()
    )
    print("[Realtime] Subscribed to platform_users changes")


def _tag_map_reload_loop():
    """Every 5 minutes, full reload. Belt-and-suspenders for missed Realtime events."""
    while True:
        time.sleep(300)
        try:
            rebuild_tag_map()
        except Exception as e:
            print(f"[Cache] tag_to_employee reload failed: {e}")


# ─────────────────────────────────────────────
# STATION CACHE (Step 4a)
# ─────────────────────────────────────────────
# In-memory {receiver_mac_upper: {id, name}} sourced from the
# netsuite_production_stations mirror in Supabase. The rico-platform
# `sweep-production-stations.ts` job refreshes that mirror twice a day
# from NetSuite; hourly reload here is more than fast enough to notice.
# No Realtime — mirror churn is slow, and TRUNCATE+reload during the
# sync could produce a burst of events we don't care about.

receiver_to_station: dict[str, dict] = {}
_station_map_lock = threading.Lock()


def rebuild_station_map():
    """Full reload from netsuite_production_stations. Called at boot + hourly."""
    if supabase_client is None:
        return
    resp = (
        supabase_client.table("netsuite_production_stations")
        .select("ns_internal_id, name, bt_mac_address, is_inactive")
        .eq("is_inactive", False)
        .not_.is_("bt_mac_address", "null")
        .execute()
    )
    new_map: dict[str, dict] = {}
    for r in resp.data or []:
        mac = (r.get("bt_mac_address") or "").upper()
        if not mac:
            continue
        new_map[mac] = {
            "id": r["ns_internal_id"],
            "name": r.get("name"),
        }
    with _station_map_lock:
        receiver_to_station.clear()
        receiver_to_station.update(new_map)
    print(f"[Cache] receiver_to_station reloaded — {len(new_map)} station(s)")


def resolve_station(receiver_mac: str):
    """Return {id, name} for the station this receiver is bound to, or None.

    None means: the receiver's MAC is not in the NS mirror. Callers must
    still record the receiver_mac on location_log (orphan-safe write)."""
    with _station_map_lock:
        return receiver_to_station.get(receiver_mac.upper())


def _station_map_reload_loop():
    """Every hour, full reload from the mirror."""
    while True:
        time.sleep(3600)
        try:
            rebuild_station_map()
        except Exception as e:
            print(f"[Cache] receiver_to_station reload failed: {e}")


# ─────────────────────────────────────────────
# REALTIME BROADCAST (Step 5)
# ─────────────────────────────────────────────
# When the engine confirms an arrival at a resolved station, publish a
# broadcast on channel "station:<ns_internal_id>". A future station-display
# SPA subscribes to that channel and reacts (~1s) with animations, avatars,
# etc. Fire-and-forget: if broadcast fails (network, Supabase blip), we log
# and move on — the transition is already durable in location_log.

_broadcast_client: httpx.AsyncClient | None = None


def _broadcast_endpoint() -> str:
    return SUPABASE_URL.rstrip("/") + "/realtime/v1/api/broadcast"


async def _broadcast_arrival(station_id: int, emp: dict, ts) -> None:
    """POST an 'arrival' broadcast to station:<id>. Log on failure, never raise."""
    global _broadcast_client
    if _broadcast_client is None:
        _broadcast_client = httpx.AsyncClient(timeout=5.0)
    payload = {
        "messages": [{
            "topic": f"station:{station_id}",
            "event": "arrival",
            "payload": {
                "user_id": str(emp["id"]),
                "user_name": emp.get("name"),
                "avatar_url": emp.get("avatar_url"),
                "department": emp.get("department"),
                "at": ts.isoformat(),
            },
        }],
    }
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    try:
        r = await _broadcast_client.post(_broadcast_endpoint(), json=payload, headers=headers)
        if r.status_code >= 400:
            print(f"[Broadcast] station:{station_id} HTTP {r.status_code}: {r.text[:200]}")
    except Exception as exc:
        print(f"[Broadcast] station:{station_id} send failed: {exc}")


# ─────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────
engine = LocationEngine()


def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Connected and subscribed to {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Connection failed with code {rc}")


def on_mqtt_message(client, userdata, msg):
    """
    Expected payload:
    {
      "receiver_mac": "AA:BB:CC:DD:EE:FF",
      "readings": [
        {"tag_id": "11:22:33:44:55:66", "rssi": -62}
      ],
      "timestamp": "2026-03-02T14:30:00Z"
    }
    """
    try:
        data = json.loads(msg.payload.decode())
        receiver_mac = data["receiver_mac"]
        timestamp = data.get("timestamp", datetime.now(timezone.utc).isoformat())

        # Mark receiver as alive on every message, even an empty-readings heartbeat
        engine.receiver_health[receiver_mac] = timestamp

        # Store WiFi RSSI for health monitoring
        wifi_rssi = data.get("wifi_rssi")
        if wifi_rssi is not None:
            engine.receiver_wifi_rssi[receiver_mac] = wifi_rssi

        # Feed to location engine. raw_readings persistence was dropped
        # (~500K rows/day, unused downstream) — the engine's in-memory buffer
        # is authoritative for the current window; confirmed transitions land
        # in Supabase location_log.
        for r in data["readings"]:
            engine.add_reading(receiver_mac, r["tag_id"], r["rssi"], timestamp)

    except Exception as e:
        print(f"[MQTT] Error processing message: {e}")


def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    return client


# ─────────────────────────────────────────────
# PROCESSING LOOP
# ─────────────────────────────────────────────
async def processing_loop():
    """Run location engine every WINDOW_SECONDS on the FastAPI event loop."""
    while True:
        await asyncio.sleep(WINDOW_SECONDS)
        try:
            await engine.process_window()
        except Exception as e:
            print(f"[Engine] Error in processing loop: {e}")


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="BLE Employee Tracking", version="0.1.0")


@app.on_event("startup")
async def startup():
    global supabase_client, pg_pool

    # --- Supabase REST/Realtime client -----------------------------------
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set before boot"
        )
    supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    print(f"[Supabase] REST client ready — {SUPABASE_URL}")

    # --- asyncpg pool via transaction pooler -----------------------------
    dsn = _build_pooler_dsn()
    pg_pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=5,
        statement_cache_size=0,  # required for pgBouncer transaction mode
    )
    async with pg_pool.acquire() as conn:
        pg_version = await conn.fetchval("SELECT version()")
    print(f"[Postgres] Pool ready — {pg_version.split(',')[0]}")

    # --- Employee cache: bulk-load + Realtime + 5-min reload -------------
    rebuild_tag_map()
    await _start_platform_users_realtime()
    threading.Thread(target=_tag_map_reload_loop, daemon=True).start()

    # --- Station cache: bulk-load + hourly reload ------------------------
    rebuild_station_map()
    threading.Thread(target=_station_map_reload_loop, daemon=True).start()

    # --- Restart-safety rehydrate (Step 8) --------------------------------
    # Read every open row (timestamp_out IS NULL) from Supabase location_log
    # and seed engine.current_locations so an employee mid-shift doesn't
    # trigger a spurious "new location" transition on the very next window.
    async with pg_pool.acquire() as conn:
        open_rows = await conn.fetch(
            "SELECT employee_id, location_id, receiver_mac "
            "FROM location_log WHERE timestamp_out IS NULL"
        )
    for row in open_rows:
        key = row["location_id"] if row["location_id"] is not None else row["receiver_mac"]
        engine.current_locations[str(row["employee_id"])] = key
    if open_rows:
        print(f"[Engine] Rehydrated {len(open_rows)} open location(s) from Supabase")

    start_mqtt()
    asyncio.create_task(processing_loop())
    print("[Server] Started — MQTT listener and processing loop running")


@app.on_event("shutdown")
async def shutdown():
    global pg_pool, _realtime_client, _broadcast_client
    if _realtime_client is not None:
        await _realtime_client.close()
        print("[Realtime] Client closed")
    if _broadcast_client is not None:
        await _broadcast_client.aclose()
    if pg_pool is not None:
        await pg_pool.close()
        print("[Postgres] Pool closed")


# --- Employee, location, and tag JSON endpoints removed. --------------
# Employee identity is owned by rico-platform (`platform_users`); tag
# assignments happen in that repo's admin UI. Consumers that need employee
# lists or history should query Supabase directly (REST or SQL).
#
# Location metadata now lives in the NetSuite `customrecord_production_station`
# record, mirrored to `netsuite_production_stations`; the BLE app reads that
# via the receiver_to_station cache (Step 4a) but doesn't publish JSON for it.


# --- Health / Admin ---

@app.get("/api/health")
def health_check():
    """Receiver status and system health."""
    receiver_status = engine.get_receiver_status()

    # Station names sourced from the netsuite_production_stations cache.
    with _station_map_lock:
        mac_to_name = {mac: entry["name"] for mac, entry in receiver_to_station.items()}

    enriched = {}
    for mac, status in receiver_status.items():
        status["location_name"] = mac_to_name.get(mac, "UNKNOWN")
        enriched[mac] = status

    # Check for registered receivers we haven't heard from
    for mac, name in mac_to_name.items():
        if mac not in enriched:
            enriched[mac] = {"status": "never_seen", "location_name": name}

    return {
        "server_time": datetime.now(timezone.utc).isoformat(),
        "tracked_tags": len(engine.current_locations),
        "receivers": enriched,
    }


# --- NetSuite push endpoints removed. All movement history lives in
# Supabase location_log. Any downstream reporting can consume from there
# directly; there's no push-to-NS path anymore.


# --- Simple Dashboard ---

COMMON_STYLES = """
    body { font-family: -apple-system, sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; margin-bottom: 4px; }
    nav { margin-bottom: 24px; padding: 10px 0; border-bottom: 1px solid #ddd; }
    nav a { margin-right: 18px; color: #0066cc; text-decoration: none; font-weight: 500; }
    nav a:hover { text-decoration: underline; }
    table { width: 100%; border-collapse: collapse; margin-top: 16px; }
    th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #f5f5f5; font-weight: 600; }
    .status { font-size: 13px; color: #888; margin-top: 4px; }
    .btn { padding: 6px 14px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; text-decoration: none; display: inline-block; }
    .btn-primary { background: #0066cc; color: #fff; }
    .btn-primary:hover { background: #0052a3; }
    .btn-danger { background: #cc3333; color: #fff; }
    .btn-danger:hover { background: #a32929; }
    .btn-sm { padding: 4px 10px; font-size: 12px; }
    form { margin-top: 20px; }
    label { display: block; margin-top: 12px; font-weight: 500; font-size: 14px; }
    input, select { padding: 8px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; width: 300px; margin-top: 4px; }
    .msg-ok { background: #e6f9e6; border: 1px solid #4caf50; padding: 10px 14px; border-radius: 4px; margin-top: 16px; color: #2e7d32; }
    .msg-err { background: #fdecea; border: 1px solid #f44336; padding: 10px 14px; border-radius: 4px; margin-top: 16px; color: #c62828; }
    .tag-badge { background: #eef; padding: 2px 8px; border-radius: 3px; font-family: monospace; font-size: 13px; }
    .online { color: #2e7d32; font-weight: 600; }
    .offline { color: #c62828; font-weight: 600; }
    .never { color: #888; }
"""

NAV_HTML = """
    <nav>
        <a href="/">Dashboard</a>
        <a href="/admin/history">History</a>
        <a href="/admin/health">Receivers</a>
        <a href="/admin/flash">Flash Receiver</a>
        <a href="/docs">API Docs</a>
    </nav>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Dashboard — who's where right now (from Supabase location_log snapshots)."""
    resp = (
        supabase_client.table("location_log")
        .select("employee_name, employee_department, location_name, receiver_mac, timestamp_in")
        .is_("timestamp_out", "null")
        .order("location_name")
        .order("employee_name")
        .execute()
    )

    table_rows = ""
    for r in resp.data or []:
        # For orphan rows (no station bound), fall back to receiver_mac as the visible location
        location_display = r["location_name"] or f'<span style="color:#888;">unmapped receiver {r["receiver_mac"]}</span>'
        table_rows += (
            f"<tr>"
            f"<td>{r['employee_name'] or ''}</td>"
            f"<td>{location_display}</td>"
            f"<td>{r['employee_department'] or ''}</td>"
            f"<td>{r['timestamp_in'] or ''}</td>"
            f"</tr>\n"
        )

    if not table_rows:
        table_rows = '<tr><td colspan="4" style="text-align:center;color:#888;">No active locations — send some MQTT readings to get started</td></tr>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>BLE Tracking — Dashboard</title>
        <meta http-equiv="refresh" content="30">
        <style>{COMMON_STYLES}</style>
    </head>
    <body>
        {NAV_HTML}
        <h1>Dashboard</h1>
        <p class="status">Auto-refreshes every 30 seconds</p>
        <table>
            <thead><tr><th>Employee</th><th>Location</th><th>Department</th><th>Since</th></tr></thead>
            <tbody>{table_rows}</tbody>
        </table>
    </body>
    </html>
    """


# --- Employee & Location admin removed. -------------------------------
# Employee identity now lives in rico-platform's `platform_users` (edited
# via that app's admin UI). Station names + receiver MAC bindings live in
# NetSuite (`customrecord_production_station.custrecord_bt_mac_address`)
# and are mirrored twice-daily into `netsuite_production_stations`.


from starlette.requests import Request as StarletteRequest
from starlette.responses import RedirectResponse


# --- History ---

@app.get("/admin/history", response_class=HTMLResponse)
def admin_history():
    """List all employees (from platform_users) with their location-log entry counts."""
    users = (
        supabase_client.table("platform_users")
        .select("id, first_name, last_name, ble_tag_id")
        .order("last_name")
        .order("first_name")
        .execute()
        .data or []
    )

    rows = ""
    for u in users:
        name = f"{(u.get('first_name') or '').strip()} {(u.get('last_name') or '').strip()}".strip() or "(unnamed)"
        tag = (
            f'<span class="tag-badge">{u["ble_tag_id"]}</span>'
            if u.get("ble_tag_id") else '<span style="color:#888;">No tag</span>'
        )
        entries = (
            supabase_client.table("location_log")
            .select("id", count="exact")
            .eq("employee_id", u["id"])
            .limit(1)
            .execute()
            .count or 0
        )
        rows += f"""<tr>
            <td><a href="/admin/history/{u['id']}" style="color:#0066cc;font-weight:500;">{name}</a></td>
            <td>{tag}</td>
            <td>{entries}</td>
        </tr>\n"""

    if not rows:
        rows = '<tr><td colspan="3" style="text-align:center;color:#888;">No employees in platform_users</td></tr>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>BLE Tracking — History</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Employee History</h1>
        <p class="status">Employees managed in rico-platform. Click a name for their location history.</p>
        <table>
            <thead><tr><th>Employee</th><th>Tag</th><th>Total Entries</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </body>
    </html>
    """


@app.get("/admin/history/{emp_id}", response_class=HTMLResponse)
def admin_employee_history(emp_id: str, days: int = 7):
    """Per-employee history from Supabase location_log (snapshot columns)."""
    user = (
        supabase_client.table("platform_users")
        .select("id, first_name, last_name, ble_tag_id, department")
        .eq("id", emp_id)
        .limit(1)
        .execute()
        .data
    )
    if not user:
        return RedirectResponse(url="/admin/history?err=Employee+not+found", status_code=303)
    u = user[0]
    name = f"{(u.get('first_name') or '').strip()} {(u.get('last_name') or '').strip()}".strip() or "(unnamed)"

    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    history = (
        supabase_client.table("location_log")
        .select("location_name, receiver_mac, employee_department, timestamp_in, timestamp_out")
        .eq("employee_id", emp_id)
        .gte("timestamp_in", since)
        .order("timestamp_in", desc=True)
        .execute()
        .data or []
    )

    rows = ""
    for h in history:
        time_in = h.get("timestamp_in") or ""
        time_out = h.get("timestamp_out") or ""

        duration = ""
        if time_in and time_out:
            try:
                t_in = datetime.fromisoformat(time_in.replace("Z", "+00:00"))
                t_out = datetime.fromisoformat(time_out.replace("Z", "+00:00"))
                total_min = int((t_out - t_in).total_seconds() / 60)
                hours, mins = divmod(total_min, 60)
                duration = f"{hours}h {mins}m" if hours else f"{mins}m"
            except Exception:
                duration = "—"
        elif time_in and not time_out:
            duration = '<span style="color:#2e7d32;font-weight:600;">Currently here</span>'

        location_display = h.get("location_name") or (
            f'<span style="color:#888;">unmapped receiver {h.get("receiver_mac") or "?"}</span>'
        )
        dept = h.get("employee_department") or ""
        display_in = time_in.replace("T", " ").replace("+00:00", "") if time_in else ""
        display_out = time_out.replace("T", " ").replace("+00:00", "") if time_out else ""

        rows += f"""<tr>
            <td>{location_display}</td>
            <td>{dept}</td>
            <td>{display_in}</td>
            <td>{display_out}</td>
            <td>{duration}</td>
        </tr>\n"""

    if not rows:
        rows = f'<tr><td colspan="5" style="text-align:center;color:#888;">No history found in the last {days} days</td></tr>'

    day_links = ""
    for d in [1, 7, 14, 30]:
        active = "font-weight:700;" if d == days else ""
        day_links += (
            f'<a href="/admin/history/{emp_id}?days={d}" '
            f'style="margin-right:14px;color:#0066cc;{active}">{d} day{"s" if d > 1 else ""}</a>'
        )

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>History — {name}</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>{name}</h1>
        <p class="status">Tag: {u.get('ble_tag_id') or 'None assigned'} &nbsp;|&nbsp; Department: {u.get('department') or 'None'}</p>
        <p>Show: {day_links}</p>
        <table>
            <thead><tr><th>Location</th><th>Department</th><th>Time In</th><th>Time Out</th><th>Duration</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
        <br><a href="/admin/history" class="btn btn-primary">&larr; Back to Employees</a>
    </body>
    </html>
    """


# --- Receiver Health Admin ---

@app.get("/admin/health", response_class=HTMLResponse)
def admin_health():
    receiver_status = engine.get_receiver_status()

    # Station names now come from the in-memory station cache (Step 4a),
    # which is sourced from netsuite_production_stations. No SQLite here.
    with _station_map_lock:
        mac_to_name = {mac: entry["name"] for mac, entry in receiver_to_station.items()}

    rows = ""
    # Show registered receivers
    for mac, name in mac_to_name.items():
        if mac in receiver_status:
            s = receiver_status[mac]
            status_class = "online" if s["status"] == "online" else "offline"
            status_text = s["status"].upper()
            last = s.get("last_seen", "—")
            ago = f'{s.get("seconds_ago", "?")}s ago'
        else:
            status_class = "never"
            status_text = "NEVER SEEN"
            last = "—"
            ago = ""

        # WiFi RSSI
        wifi_rssi = engine.receiver_wifi_rssi.get(mac)
        if wifi_rssi is not None:
            if wifi_rssi > -65:
                rssi_color = "#2e7d32"  # green — strong
            elif wifi_rssi > -75:
                rssi_color = "#f57c00"  # orange — okay
            else:
                rssi_color = "#c62828"  # red — weak
            rssi_text = f'<span style="color:{rssi_color};font-weight:600;">{wifi_rssi} dBm</span>'
        else:
            rssi_text = "—"

        rows += f"""<tr>
            <td>{name}</td>
            <td><span class="tag-badge">{mac}</span></td>
            <td><span class="{status_class}">{status_text}</span></td>
            <td>{rssi_text}</td>
            <td>{last}</td>
            <td>{ago}</td>
        </tr>\n"""

    # Show unknown receivers (not mapped to a location)
    for mac, s in receiver_status.items():
        if mac not in mac_to_name:
            wifi_rssi = engine.receiver_wifi_rssi.get(mac)
            rssi_text = f'{wifi_rssi} dBm' if wifi_rssi else "—"
            rows += f"""<tr>
                <td style="color:#888;">UNMAPPED</td>
                <td><span class="tag-badge">{mac}</span></td>
                <td><span class="online">{s["status"].upper()}</span></td>
                <td>{rssi_text}</td>
                <td>{s.get("last_seen", "—")}</td>
                <td>{s.get("seconds_ago", "?")}s ago</td>
            </tr>\n"""

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>BLE Tracking — Receivers</title>
        <meta http-equiv="refresh" content="15">
        <style>{COMMON_STYLES}</style>
    </head>
    <body>
        {NAV_HTML}
        <h1>Receiver Health</h1>
        <p class="status">Auto-refreshes every 15 seconds</p>
        <table>
            <thead><tr><th>Location</th><th>MAC Address</th><th>Status</th><th>WiFi Signal</th><th>Last Seen</th><th>Age</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </body>
    </html>
    """


# --- Flash Receiver ---

def find_pio():
    """Locate the PlatformIO CLI. Returns (path, searched_paths) — path is None if not found.

    Note: this server may run under the SYSTEM account (e.g., as a Windows service), in which
    case `~` resolves to C:\\WINDOWS\\system32\\config\\systemprofile and not the install user's
    home. So we explicitly scan C:\\Users\\* for PlatformIO installs.
    """
    searched = []

    # 1. Try PATH
    for name in ["pio", "platformio"]:
        searched.append(f"PATH: {name}")
        try:
            result = subprocess.run([name, "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                return name, searched
        except (FileNotFoundError, OSError):
            continue

    # 2. Build candidate user-home roots: current ~, all C:\Users\* profiles
    home_roots = []
    cur_home = os.path.expanduser("~")
    home_roots.append(cur_home)
    users_dir = "C:\\Users"
    if os.path.isdir(users_dir):
        for entry in os.listdir(users_dir):
            full = os.path.join(users_dir, entry)
            if os.path.isdir(full) and full not in home_roots:
                home_roots.append(full)

    # 3. For each home, check standard PlatformIO Core locations
    for home in home_roots:
        for rel in [
            os.path.join(".platformio", "penv", "Scripts", "pio.exe"),
            os.path.join(".platformio", "penv", "Scripts", "platformio.exe"),
            os.path.join(".platformio", "penv", "bin", "pio"),
        ]:
            path = os.path.join(home, rel)
            searched.append(path)
            if os.path.isfile(path):
                return path, searched

        # 4. VSCode extension dir — scan for platformio.platformio-ide-*/penv/Scripts/pio.exe
        vscode_ext = os.path.join(home, ".vscode", "extensions")
        searched.append(f"{vscode_ext}\\platformio.platformio-ide-*\\...\\pio.exe")
        if os.path.isdir(vscode_ext):
            try:
                for entry in os.listdir(vscode_ext):
                    if entry.startswith("platformio."):
                        pio_path = os.path.join(vscode_ext, entry, "penv", "Scripts", "pio.exe")
                        if os.path.isfile(pio_path):
                            return pio_path, searched
            except OSError:
                pass

    return None, searched


@app.get("/api/flash/ports")
def flash_ports():
    ports = serial.tools.list_ports.comports()
    return [
        {"device": p.device, "description": p.description}
        for p in sorted(ports, key=lambda x: x.device)
    ]


@app.get("/api/flash/stream")
def flash_stream(port: str):
    """SSE — runs PlatformIO upload and streams output line by line."""
    def generate():
        pio, searched = find_pio()
        if not pio:
            yield "data: ERROR: PlatformIO not found. Install with: pip install platformio\n\n"
            yield "data: Searched the following:\n\n"
            for s in searched:
                yield f"data:   - {s}\n\n"
            yield "event: done\ndata: 1\n\n"
            return

        cmd = [pio, "run", "--target", "upload", "--upload-port", port]
        yield f"data: Running: {' '.join(cmd)}\n\n"
        yield f"data: Firmware dir: {FIRMWARE_DIR}\n\n"

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=FIRMWARE_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for line in proc.stdout:
                for part in line.rstrip("\n").splitlines():
                    yield f"data: {part}\n\n"
            proc.wait()
            yield f"event: done\ndata: {proc.returncode}\n\n"
        except Exception as exc:
            yield f"data: Exception: {exc}\n\n"
            yield "event: done\ndata: 1\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/flash/capture-mac")
def flash_capture_mac(port: str):
    """SSE — opens serial port after flash, waits for MAC in boot output."""
    import serial as pyserial

    def generate():
        yield "data: Waiting for device to boot...\n\n"
        time.sleep(3)
        try:
            ser = pyserial.Serial(port, 115200, timeout=1)
            start = time.time()
            mac_seen = False

            # Read for up to 30s total; emit `mac` event as soon as MAC is seen,
            # but keep streaming so the user can see MQTT connect/fail and the
            # first publish attempt.
            while time.time() - start < 30:
                line = ser.readline().decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                yield f"data: {line}\n\n"

                if not mac_seen and "[Info] Receiver MAC:" in line:
                    mac = line.split("Receiver MAC:")[-1].strip()
                    mac_seen = True
                    yield f"event: mac\ndata: {mac}\n\n"

                # Once we've seen a successful publish, the receiver is fully online — exit
                if mac_seen and "[MQTT] Published" in line:
                    ser.close()
                    yield "event: done\ndata: 0\n\n"
                    return

            ser.close()
            if mac_seen:
                yield "data: (MAC captured but no MQTT publish seen within 30s — check WiFi/MQTT logs above)\n\n"
                yield "event: done\ndata: 0\n\n"
            else:
                yield "data: Timed out waiting for MAC — check device is booting.\n\n"
                yield "event: done\ndata: 1\n\n"
        except Exception as exc:
            yield f"data: Error reading serial: {exc}\n\n"
            yield "event: done\ndata: 1\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# /api/flash/assign removed. Station <-> receiver MAC binding now lives on
# the NetSuite station record (`custrecord_bt_mac_address`); ops pastes
# the flashed MAC into that field, and the twice-daily mirror sync
# propagates it into `netsuite_production_stations`.


@app.get("/admin/flash", response_class=HTMLResponse)
def admin_flash():
    ports = serial.tools.list_ports.comports()
    port_options = "".join(
        f'<option value="{p.device}">{p.device} — {p.description}</option>'
        for p in sorted(ports, key=lambda x: x.device)
    )
    if not port_options:
        port_options = '<option value="">No COM ports found</option>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>BLE Tracking — Flash Receiver</title>
        <style>
            {COMMON_STYLES}
            #output {{
                background: #1e1e1e;
                color: #d4d4d4;
                font-family: Consolas, monospace;
                font-size: 13px;
                padding: 14px;
                border-radius: 4px;
                height: 360px;
                overflow-y: auto;
                white-space: pre-wrap;
                margin-top: 16px;
            }}
            #status-bar {{
                margin-top: 10px;
                font-weight: 600;
                font-size: 14px;
                min-height: 22px;
            }}
            #mac-box {{
                display: none;
                margin-top: 20px;
                padding: 16px;
                background: #f0f7ff;
                border: 1px solid #b3d1f7;
                border-radius: 6px;
            }}
            #mac-box h3 {{ margin: 0 0 12px 0; font-size: 15px; color: #1a4a7a; }}
            #mac-box label {{ font-size: 14px; }}
            #mac-box input {{ width: 340px; font-family: Consolas, monospace; }}
            .success {{ color: #2e7d32; }}
            .error   {{ color: #c62828; }}
            .running {{ color: #1a73e8; }}
            select {{ width: 420px; }}
            .controls {{ display: flex; align-items: center; gap: 12px; margin-top: 16px; }}
        </style>
    </head>
    <body>
        {NAV_HTML}
        <h1>Flash Receiver Firmware</h1>
        <p class="status">Uploads the ESP32 firmware via PlatformIO. The device must be connected via USB.</p>

        <label for="port-select">COM Port</label>
        <div class="controls">
            <select id="port-select">{port_options}</select>
            <button class="btn" onclick="refreshPorts()">Refresh</button>
        </div>

        <div class="controls" style="margin-top:20px;">
            <button id="upload-btn" class="btn btn-primary" onclick="startUpload()">Upload Firmware</button>
        </div>

        <div id="status-bar"></div>
        <div id="output"></div>

        <!-- Post-flash: show the captured MAC for ops to paste into NetSuite -->
        <div id="mac-box">
            <h3>Device MAC</h3>
            <p style="margin:0 0 8px 0;font-size:13px;color:#555;">
              Copy this MAC and paste it into the NetSuite production station record
              (field <code>custrecord_bt_mac_address</code>). The twice-daily
              mirror sync will pick it up.
            </p>
            <label>Detected MAC Address
                <input type="text" id="detected-mac" placeholder="Reading from device..." readonly>
            </label>
            <div style="margin-top:12px;">
                <button class="btn btn-primary" onclick="copyMac()">Copy to clipboard</button>
                <span id="copy-status" style="margin-left:12px;font-weight:600;font-size:14px;"></span>
            </div>
        </div>

        <script>
            function refreshPorts() {{
                fetch('/api/flash/ports')
                    .then(r => r.json())
                    .then(ports => {{
                        const sel = document.getElementById('port-select');
                        const current = sel.value;
                        sel.innerHTML = ports.length
                            ? ports.map(p => `<option value="${{p.device}}">${{p.device}} — ${{p.description}}</option>`).join('')
                            : '<option value="">No COM ports found</option>';
                        if ([...sel.options].some(o => o.value === current)) sel.value = current;
                    }});
            }}

            function startUpload() {{
                const port = document.getElementById('port-select').value;
                if (!port) {{ setStatus('No port selected.', 'error'); return; }}

                const output = document.getElementById('output');
                const btn    = document.getElementById('upload-btn');
                document.getElementById('mac-box').style.display = 'none';
                document.getElementById('detected-mac').value = '';
                document.getElementById('copy-status').textContent = '';
                output.textContent = '';
                btn.disabled = true;
                setStatus('Uploading...', 'running');

                const es = new EventSource('/api/flash/stream?port=' + encodeURIComponent(port));

                es.onmessage = e => {{
                    output.textContent += e.data + '\\n';
                    output.scrollTop = output.scrollHeight;
                }};

                es.addEventListener('done', e => {{
                    es.close();
                    btn.disabled = false;
                    const code = parseInt(e.data);
                    if (code === 0) {{
                        setStatus('Upload complete. Reading device MAC...', 'running');
                        captureMac(port);
                    }} else {{
                        setStatus('Upload failed - see output above.', 'error');
                    }}
                }});

                es.onerror = () => {{ es.close(); btn.disabled = false; setStatus('Connection lost.', 'error'); }};
            }}

            function captureMac(port) {{
                const output = document.getElementById('output');
                output.textContent += '\\n--- Reading boot output ---\\n';

                const es = new EventSource('/api/flash/capture-mac?port=' + encodeURIComponent(port));

                es.onmessage = e => {{
                    output.textContent += e.data + '\\n';
                    output.scrollTop = output.scrollHeight;
                }};

                es.addEventListener('mac', e => {{
                    document.getElementById('detected-mac').value = e.data;
                    document.getElementById('mac-box').style.display = 'block';
                    setStatus('MAC detected. Copy it into NetSuite.', 'success');
                }});

                es.addEventListener('done', e => {{
                    es.close();
                    document.getElementById('mac-box').style.display = 'block';
                    if (parseInt(e.data) !== 0 && !document.getElementById('detected-mac').value) {{
                        setStatus('Upload done. Enter MAC manually if needed.', 'success');
                    }}
                }});

                es.onerror = () => {{
                    es.close();
                    document.getElementById('mac-box').style.display = 'block';
                    setStatus('Upload done. Enter MAC manually if needed.', 'success');
                }};
            }}

            function copyMac() {{
                const mac = document.getElementById('detected-mac').value.trim();
                const statusEl = document.getElementById('copy-status');
                if (!mac) {{ statusEl.textContent = 'No MAC to copy.'; statusEl.className = 'error'; return; }}
                navigator.clipboard.writeText(mac).then(
                    () => {{ statusEl.textContent = 'Copied: ' + mac; statusEl.className = 'success'; }},
                    () => {{ statusEl.textContent = 'Copy failed - select manually.'; statusEl.className = 'error'; }}
                );
            }}

            function setStatus(msg, cls) {{
                const el = document.getElementById('status-bar');
                el.textContent = msg;
                el.className = cls || '';
            }}
        </script>
    </body>
    </html>
    """