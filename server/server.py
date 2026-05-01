"""
BLE Employee Location Tracking Server
======================================
FastAPI + MQTT + SQLite

Setup:
  pip install fastapi uvicorn paho-mqtt

Run:
  1. Start Mosquitto:  mosquitto -v
  2. Start server:     uvicorn server:app --reload --host 0.0.0.0 --port 8000

Test without hardware:
  mosquitto_pub -t "ble/readings" -m '{
    "receiver_mac": "AA:BB:CC:DD:EE:01",
    "readings": [
      {"tag_id": "11:22:33:44:55:01", "rssi": -58},
      {"tag_id": "11:22:33:44:55:02", "rssi": -74}
    ],
    "timestamp": "2026-03-03T10:00:00Z"
  }'
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from collections import defaultdict

import serial.tools.list_ports
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
import paho.mqtt.client as mqtt

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "ble/readings"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ble_tracking.db")

FIRMWARE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "firmware"))

# Location engine settings
WINDOW_SECONDS = 60           # 60-second averaging window
HOLD_PERIODS = 2              # Must hold for 2 consecutive periods (2 min)
RSSI_THRESHOLD_STRONG = -65   # Definitely here
RSSI_THRESHOLD_WEAK = -80     # Passing through / gone

# Receiver health
RECEIVER_TIMEOUT_SECONDS = 600  # 10 min with no data = offline


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def init_db():
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                tag_id TEXT UNIQUE,
                netsuite_employee_id TEXT
            );

            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                location_type TEXT CHECK(location_type IN ('workstation', 'zone')),
                department_id TEXT,
                receiver_mac TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS location_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER REFERENCES employees(id),
                location_id INTEGER REFERENCES locations(id),
                timestamp_in DATETIME NOT NULL,
                timestamp_out DATETIME,
                synced_to_netsuite BOOLEAN DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS raw_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receiver_mac TEXT,
                tag_id TEXT,
                rssi INTEGER,
                timestamp DATETIME
            );
        """)


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def seed_demo_data():
    """Insert sample data for testing without hardware."""
    with get_db() as db:
        count = db.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        if count > 0:
            return

        # Sample employees
        employees = [
            ("Alice Johnson", "11:22:33:44:55:01", "EMP001"),
            ("Bob Smith", "11:22:33:44:55:02", "EMP002"),
            ("Carol Davis", "11:22:33:44:55:03", "EMP003"),
            ("Dan Wilson", "11:22:33:44:55:04", "EMP004"),
            ("Eve Martinez", "11:22:33:44:55:05", "EMP005"),
        ]
        db.executemany(
            "INSERT INTO employees (name, tag_id, netsuite_employee_id) VALUES (?, ?, ?)",
            employees
        )

        # Sample locations (pilot: 4 workstations + 1 zone)
        locations = [
            ("UV Printer 1", "workstation", "DEPT_PRINT", "AA:BB:CC:DD:EE:01"),
            ("UV Printer 2", "workstation", "DEPT_PRINT", "AA:BB:CC:DD:EE:02"),
            ("Laser Cutter 1", "workstation", "DEPT_CUT", "AA:BB:CC:DD:EE:03"),
            ("Heat Press 1", "workstation", "DEPT_PRESS", "AA:BB:CC:DD:EE:04"),
            ("Picking Aisle 1", "zone", "DEPT_PICK", "AA:BB:CC:DD:EE:05"),
        ]
        db.executemany(
            "INSERT INTO locations (name, location_type, department_id, receiver_mac) VALUES (?, ?, ?, ?)",
            locations
        )
        print("[DB] Seeded demo employees and locations")


# ─────────────────────────────────────────────
# LOCATION ENGINE
# ─────────────────────────────────────────────
class LocationEngine:
    """
    Determines employee location using 'loudest signal wins' logic.

    Flow:
    1. Collect RSSI readings from all receivers
    2. Every WINDOW_SECONDS, average RSSI per receiver per tag
    3. Strongest average = candidate location
    4. Only log change if candidate holds for HOLD_PERIODS consecutive windows
    """

    def __init__(self):
        # Raw RSSI buffer: {tag_id: {receiver_mac: [rssi, rssi, ...]}}
        self.readings_buffer = defaultdict(lambda: defaultdict(list))
        # Current confirmed locations: {tag_id: location_id}
        self.current_locations = {}
        # Candidate tracking: {tag_id: {"location_id": id, "count": n}}
        self.candidates = {}
        # Receiver health: {receiver_mac: last_seen_timestamp}
        self.receiver_health = {}
        # WiFi signal strength: {receiver_mac: rssi}
        self.receiver_wifi_rssi = {}
        self.lock = threading.Lock()

    def add_reading(self, receiver_mac, tag_id, rssi, timestamp):
        """Buffer an incoming RSSI reading."""
        with self.lock:
            self.readings_buffer[tag_id][receiver_mac].append(rssi)
            self.receiver_health[receiver_mac] = timestamp

    def process_window(self):
        """
        Called every WINDOW_SECONDS. Averages readings, determines locations,
        and logs changes to the database.
        """
        with self.lock:
            buffer = dict(self.readings_buffer)
            self.readings_buffer = defaultdict(lambda: defaultdict(list))

        if not buffer:
            return

        now = datetime.now(timezone.utc).isoformat()

        with get_db() as db:
            # Build lookup: receiver_mac -> location_id
            loc_rows = db.execute("SELECT id, receiver_mac FROM locations").fetchall()
            mac_to_loc = {r["receiver_mac"]: r["id"] for r in loc_rows}

            # Build lookup: tag_id -> employee_id
            emp_rows = db.execute("SELECT id, tag_id FROM employees WHERE tag_id IS NOT NULL").fetchall()
            tag_to_emp = {r["tag_id"]: r["id"] for r in emp_rows}

            for tag_id, receivers in buffer.items():
                if tag_id not in tag_to_emp:
                    continue  # Unknown tag, skip

                # Average RSSI per receiver
                avg_rssi = {}
                for mac, rssi_list in receivers.items():
                    avg_rssi[mac] = sum(rssi_list) / len(rssi_list)

                # Find strongest (closest to 0 = strongest)
                strongest_mac = max(avg_rssi, key=avg_rssi.get)
                strongest_rssi = avg_rssi[strongest_mac]

                # Ignore if signal too weak
                if strongest_rssi < RSSI_THRESHOLD_WEAK:
                    continue

                # Map to location
                if strongest_mac not in mac_to_loc:
                    continue
                candidate_loc = mac_to_loc[strongest_mac]

                employee_id = tag_to_emp[tag_id]
                current_loc = self.current_locations.get(tag_id)

                if candidate_loc == current_loc:
                    # No change, reset candidate
                    self.candidates.pop(tag_id, None)
                    continue

                # Track candidate for hold period
                if tag_id in self.candidates and self.candidates[tag_id]["location_id"] == candidate_loc:
                    self.candidates[tag_id]["count"] += 1
                else:
                    self.candidates[tag_id] = {"location_id": candidate_loc, "count": 1}

                # Confirm change after HOLD_PERIODS
                if self.candidates[tag_id]["count"] >= HOLD_PERIODS:
                    self._log_transition(db, employee_id, tag_id, current_loc, candidate_loc, now)
                    self.candidates.pop(tag_id, None)

        print(f"[Engine] Processed window — {len(buffer)} tags seen")

    def _log_transition(self, db, employee_id, tag_id, old_loc, new_loc, timestamp):
        """Record a location change in the database."""
        # Close ONLY the most recent open location entry
        open_entry = db.execute(
            "SELECT id FROM location_log WHERE employee_id = ? AND timestamp_out IS NULL ORDER BY timestamp_in DESC LIMIT 1",
            (employee_id,)
        ).fetchone()

        if open_entry:
            db.execute(
                "UPDATE location_log SET timestamp_out = ? WHERE id = ?",
                (timestamp, open_entry["id"])
            )

        # Open new location
        db.execute(
            "INSERT INTO location_log (employee_id, location_id, timestamp_in) VALUES (?, ?, ?)",
            (employee_id, new_loc, timestamp)
        )

        self.current_locations[tag_id] = new_loc
        print(f"[Engine] Employee {employee_id}: location {old_loc} → {new_loc}")

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

        # Store raw readings
        with get_db() as db:
            for r in data["readings"]:
                db.execute(
                    "INSERT INTO raw_readings (receiver_mac, tag_id, rssi, timestamp) VALUES (?, ?, ?, ?)",
                    (receiver_mac, r["tag_id"], r["rssi"], timestamp)
                )

        # Feed to location engine
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
def processing_loop():
    """Run location engine every WINDOW_SECONDS."""
    while True:
        time.sleep(WINDOW_SECONDS)
        try:
            engine.process_window()
        except Exception as e:
            print(f"[Engine] Error in processing loop: {e}")


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="BLE Employee Tracking", version="0.1.0")


@app.on_event("startup")
def startup():
    init_db()
    seed_demo_data()

    # Reload current locations from database so we don't lose state on restart
    with get_db() as db:
        open_entries = db.execute("""
            SELECT e.tag_id, ll.location_id
            FROM location_log ll
            JOIN employees e ON e.id = ll.employee_id
            WHERE ll.timestamp_out IS NULL AND e.tag_id IS NOT NULL
        """).fetchall()
        for entry in open_entries:
            engine.current_locations[entry["tag_id"]] = entry["location_id"]
        if open_entries:
            print(f"[Engine] Restored {len(open_entries)} active locations from database")

    start_mqtt()
    t = threading.Thread(target=processing_loop, daemon=True)
    t.start()
    print("[Server] Started — MQTT listener and processing loop running")


# --- Employee endpoints ---

@app.get("/api/employees")
def list_employees():
    """List all employees with their current location."""
    with get_db() as db:
        employees = db.execute("SELECT * FROM employees").fetchall()
        result = []
        for emp in employees:
            # Get current location (open log entry)
            loc = db.execute("""
                SELECT l.name, l.location_type, l.department_id, ll.timestamp_in
                FROM location_log ll
                JOIN locations l ON l.id = ll.location_id
                WHERE ll.employee_id = ? AND ll.timestamp_out IS NULL
                ORDER BY ll.timestamp_in DESC LIMIT 1
            """, (emp["id"],)).fetchone()

            result.append({
                "id": emp["id"],
                "name": emp["name"],
                "tag_id": emp["tag_id"],
                "netsuite_employee_id": emp["netsuite_employee_id"],
                "current_location": dict(loc) if loc else None,
            })
        return result


@app.get("/api/employees/{employee_id}/history")
def employee_history(employee_id: int, hours: int = 8):
    """Location history for an employee (default: last 8 hours)."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as db:
        emp = db.execute("SELECT * FROM employees WHERE id = ?", (employee_id,)).fetchone()
        if not emp:
            raise HTTPException(404, "Employee not found")

        history = db.execute("""
            SELECT ll.*, l.name as location_name, l.department_id
            FROM location_log ll
            JOIN locations l ON l.id = ll.location_id
            WHERE ll.employee_id = ? AND ll.timestamp_in >= ?
            ORDER BY ll.timestamp_in DESC
        """, (employee_id, since)).fetchall()

        return {
            "employee": dict(emp),
            "history": [dict(h) for h in history],
        }


# --- Location endpoints ---

@app.get("/api/locations")
def list_locations():
    """List all locations."""
    with get_db() as db:
        locs = db.execute("SELECT * FROM locations").fetchall()
        return [dict(l) for l in locs]


@app.get("/api/locations/{location_id}/occupants")
def location_occupants(location_id: int):
    """Who's currently at this location."""
    with get_db() as db:
        loc = db.execute("SELECT * FROM locations WHERE id = ?", (location_id,)).fetchone()
        if not loc:
            raise HTTPException(404, "Location not found")

        occupants = db.execute("""
            SELECT e.id, e.name, e.tag_id, ll.timestamp_in
            FROM location_log ll
            JOIN employees e ON e.id = ll.employee_id
            WHERE ll.location_id = ? AND ll.timestamp_out IS NULL
        """, (location_id,)).fetchall()

        return {
            "location": dict(loc),
            "occupants": [dict(o) for o in occupants],
        }


# --- Tag management ---

@app.post("/api/tags/register")
def register_tag(employee_id: int, tag_id: str):
    """Assign a BLE tag to an employee."""
    with get_db() as db:
        emp = db.execute("SELECT * FROM employees WHERE id = ?", (employee_id,)).fetchone()
        if not emp:
            raise HTTPException(404, "Employee not found")

        existing = db.execute("SELECT * FROM employees WHERE tag_id = ?", (tag_id,)).fetchone()
        if existing:
            raise HTTPException(409, f"Tag already assigned to {existing['name']}")

        db.execute("UPDATE employees SET tag_id = ? WHERE id = ?", (tag_id, employee_id))
        return {"status": "ok", "employee_id": employee_id, "tag_id": tag_id}


@app.delete("/api/tags/{tag_id}")
def deactivate_tag(tag_id: str):
    """Deactivate a tag (lost/replacement)."""
    with get_db() as db:
        emp = db.execute("SELECT * FROM employees WHERE tag_id = ?", (tag_id,)).fetchone()
        if not emp:
            raise HTTPException(404, "Tag not found")

        # Close any open location log
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "UPDATE location_log SET timestamp_out = ? WHERE employee_id = ? AND timestamp_out IS NULL",
            (now, emp["id"])
        )
        db.execute("UPDATE employees SET tag_id = NULL WHERE id = ?", (emp["id"],))

        # Clear from engine
        engine.current_locations.pop(tag_id, None)
        engine.candidates.pop(tag_id, None)

        return {"status": "deactivated", "employee": emp["name"]}


# --- Health / Admin ---

@app.get("/api/health")
def health_check():
    """Receiver status and system health."""
    receiver_status = engine.get_receiver_status()

    with get_db() as db:
        locs = db.execute("SELECT receiver_mac, name FROM locations").fetchall()
        mac_to_name = {l["receiver_mac"]: l["name"] for l in locs}

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


@app.get("/api/netsuite/pending")
def pending_sync():
    """Entries not yet synced to NetSuite."""
    with get_db() as db:
        pending = db.execute("""
            SELECT ll.*, e.name as employee_name, e.netsuite_employee_id,
                   l.name as location_name, l.department_id
            FROM location_log ll
            JOIN employees e ON e.id = ll.employee_id
            JOIN locations l ON l.id = ll.location_id
            WHERE ll.synced_to_netsuite = 0 AND ll.timestamp_out IS NOT NULL
            ORDER BY ll.timestamp_in
        """).fetchall()
        return [dict(p) for p in pending]


@app.post("/api/netsuite/mark-synced")
def mark_synced(log_ids: list[int]):
    """Mark location log entries as synced to NetSuite."""
    with get_db() as db:
        placeholders = ",".join("?" * len(log_ids))
        db.execute(
            f"UPDATE location_log SET synced_to_netsuite = 1 WHERE id IN ({placeholders})",
            log_ids
        )
        return {"status": "ok", "marked": len(log_ids)}


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
        <a href="/admin/employees">Employees</a>
        <a href="/admin/locations">Locations</a>
        <a href="/admin/history">History</a>
        <a href="/admin/health">Receivers</a>
        <a href="/admin/flash">Flash Receiver</a>
        <a href="/docs">API Docs</a>
    </nav>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Dashboard — who's where right now."""
    with get_db() as db:
        rows = db.execute("""
            SELECT e.name, l.name as location, l.department_id, ll.timestamp_in
            FROM location_log ll
            JOIN employees e ON e.id = ll.employee_id
            JOIN locations l ON l.id = ll.location_id
            WHERE ll.timestamp_out IS NULL
            ORDER BY l.name, e.name
        """).fetchall()

    table_rows = ""
    for r in rows:
        table_rows += f"<tr><td>{r['name']}</td><td>{r['location']}</td><td>{r['department_id']}</td><td>{r['timestamp_in']}</td></tr>\n"

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


# --- Employee Admin ---

@app.get("/admin/employees", response_class=HTMLResponse)
def admin_employees(msg: str = "", err: str = ""):
    with get_db() as db:
        employees = db.execute("SELECT * FROM employees ORDER BY name").fetchall()

    msg_html = f'<div class="msg-ok">{msg}</div>' if msg else ""
    msg_html += f'<div class="msg-err">{err}</div>' if err else ""

    rows = ""
    for e in employees:
        tag = f'<span class="tag-badge">{e["tag_id"]}</span>' if e["tag_id"] else '<span style="color:#888;">None</span>'
        rows += f"""<tr>
            <td>{e['id']}</td>
            <td>{e['name']}</td>
            <td>{tag}</td>
            <td>{e['netsuite_employee_id'] or ''}</td>
            <td>
                <a href="/admin/employees/{e['id']}/edit" class="btn btn-primary btn-sm">Edit</a>
                <a href="/admin/employees/{e['id']}/delete" class="btn btn-danger btn-sm" onclick="return confirm('Delete {e["name"]}?')">Delete</a>
            </td>
        </tr>\n"""

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>BLE Tracking — Employees</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Employees</h1>
        {msg_html}
        <table>
            <thead><tr><th>ID</th><th>Name</th><th>Tag</th><th>NetSuite ID</th><th>Actions</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
        <h2 style="margin-top:30px;">Add Employee</h2>
        <form method="post" action="/admin/employees/add">
            <label>Name <input type="text" name="name" required></label>
            <label>Tag MAC Address <input type="text" name="tag_id" placeholder="dc:0d:30:48:30:2d (optional)"></label>
            <label>NetSuite Employee ID <input type="text" name="netsuite_id" placeholder="EMP001 (optional)"></label>
            <br><br><button type="submit" class="btn btn-primary">Add Employee</button>
        </form>
    </body>
    </html>
    """


from starlette.requests import Request as StarletteRequest
from starlette.responses import RedirectResponse


@app.post("/admin/employees/add")
async def admin_add_employee(request: StarletteRequest):
    form = await request.form()
    name = form.get("name", "").strip()
    tag_id = form.get("tag_id", "").strip() or None
    netsuite_id = form.get("netsuite_id", "").strip() or None

    if not name:
        return RedirectResponse(url="/admin/employees?err=Name+is+required", status_code=303)

    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO employees (name, tag_id, netsuite_employee_id) VALUES (?, ?, ?)",
                (name, tag_id, netsuite_id)
            )
        return RedirectResponse(url=f"/admin/employees?msg=Added+{name}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin/employees?err={str(e)}", status_code=303)


@app.get("/admin/employees/{emp_id}/edit", response_class=HTMLResponse)
def admin_edit_employee_form(emp_id: int):
    with get_db() as db:
        emp = db.execute("SELECT * FROM employees WHERE id = ?", (emp_id,)).fetchone()
    if not emp:
        return RedirectResponse(url="/admin/employees?err=Employee+not+found", status_code=303)

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Edit Employee</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Edit Employee</h1>
        <form method="post" action="/admin/employees/{emp_id}/edit">
            <label>Name <input type="text" name="name" value="{emp['name']}" required></label>
            <label>Tag MAC Address <input type="text" name="tag_id" value="{emp['tag_id'] or ''}" placeholder="dc:0d:30:48:30:2d"></label>
            <label>NetSuite Employee ID <input type="text" name="netsuite_id" value="{emp['netsuite_employee_id'] or ''}"></label>
            <br><br>
            <button type="submit" class="btn btn-primary">Save</button>
            <a href="/admin/employees" class="btn" style="margin-left:10px;">Cancel</a>
        </form>
    </body>
    </html>
    """


@app.post("/admin/employees/{emp_id}/edit")
async def admin_edit_employee(emp_id: int, request: StarletteRequest):
    form = await request.form()
    name = form.get("name", "").strip()
    tag_id = form.get("tag_id", "").strip() or None
    netsuite_id = form.get("netsuite_id", "").strip() or None

    if not name:
        return RedirectResponse(url=f"/admin/employees/{emp_id}/edit?err=Name+required", status_code=303)

    with get_db() as db:
        db.execute(
            "UPDATE employees SET name = ?, tag_id = ?, netsuite_employee_id = ? WHERE id = ?",
            (name, tag_id, netsuite_id, emp_id)
        )
    return RedirectResponse(url=f"/admin/employees?msg=Updated+{name}", status_code=303)


@app.get("/admin/employees/{emp_id}/delete")
def admin_delete_employee(emp_id: int):
    with get_db() as db:
        emp = db.execute("SELECT name FROM employees WHERE id = ?", (emp_id,)).fetchone()
        if emp:
            db.execute("DELETE FROM location_log WHERE employee_id = ?", (emp_id,))
            db.execute("DELETE FROM employees WHERE id = ?", (emp_id,))
            engine.current_locations = {k: v for k, v in engine.current_locations.items()}
    name = emp["name"] if emp else "Unknown"
    return RedirectResponse(url=f"/admin/employees?msg=Deleted+{name}", status_code=303)


# --- Location Admin ---

@app.get("/admin/locations", response_class=HTMLResponse)
def admin_locations(msg: str = "", err: str = ""):
    with get_db() as db:
        locations = db.execute("SELECT * FROM locations ORDER BY name").fetchall()

    msg_html = f'<div class="msg-ok">{msg}</div>' if msg else ""
    msg_html += f'<div class="msg-err">{err}</div>' if err else ""

    rows = ""
    for loc in locations:
        mac = f'<span class="tag-badge">{loc["receiver_mac"]}</span>' if loc["receiver_mac"] else ''
        rows += f"""<tr>
            <td>{loc['id']}</td>
            <td>{loc['name']}</td>
            <td>{loc['location_type']}</td>
            <td>{loc['department_id'] or ''}</td>
            <td>{mac}</td>
            <td>
                <a href="/admin/locations/{loc['id']}/edit" class="btn btn-primary btn-sm">Edit</a>
                <a href="/admin/locations/{loc['id']}/delete" class="btn btn-danger btn-sm" onclick="return confirm('Delete {loc["name"]}?')">Delete</a>
            </td>
        </tr>\n"""

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>BLE Tracking — Locations</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Locations</h1>
        {msg_html}
        <table>
            <thead><tr><th>ID</th><th>Name</th><th>Type</th><th>Department</th><th>Receiver MAC</th><th>Actions</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
        <h2 style="margin-top:30px;">Add Location</h2>
        <form method="post" action="/admin/locations/add">
            <label>Name <input type="text" name="name" required placeholder="UV Printer 1"></label>
            <label>Type
                <select name="location_type">
                    <option value="workstation">Workstation</option>
                    <option value="zone">Zone</option>
                </select>
            </label>
            <label>Department ID <input type="text" name="department_id" placeholder="DEPT_PRINT"></label>
            <label>Receiver MAC Address <input type="text" name="receiver_mac" placeholder="88:57:21:AE:35:18"></label>
            <br><br><button type="submit" class="btn btn-primary">Add Location</button>
        </form>
    </body>
    </html>
    """


@app.post("/admin/locations/add")
async def admin_add_location(request: StarletteRequest):
    form = await request.form()
    name = form.get("name", "").strip()
    loc_type = form.get("location_type", "workstation")
    dept = form.get("department_id", "").strip() or None
    mac = form.get("receiver_mac", "").strip() or None

    if not name:
        return RedirectResponse(url="/admin/locations?err=Name+is+required", status_code=303)

    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO locations (name, location_type, department_id, receiver_mac) VALUES (?, ?, ?, ?)",
                (name, loc_type, dept, mac)
            )
        return RedirectResponse(url=f"/admin/locations?msg=Added+{name}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin/locations?err={str(e)}", status_code=303)


@app.get("/admin/locations/{loc_id}/edit", response_class=HTMLResponse)
def admin_edit_location_form(loc_id: int):
    with get_db() as db:
        loc = db.execute("SELECT * FROM locations WHERE id = ?", (loc_id,)).fetchone()
    if not loc:
        return RedirectResponse(url="/admin/locations?err=Location+not+found", status_code=303)

    ws_sel = 'selected' if loc['location_type'] == 'workstation' else ''
    z_sel = 'selected' if loc['location_type'] == 'zone' else ''

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Edit Location</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Edit Location</h1>
        <form method="post" action="/admin/locations/{loc_id}/edit">
            <label>Name <input type="text" name="name" value="{loc['name']}" required></label>
            <label>Type
                <select name="location_type">
                    <option value="workstation" {ws_sel}>Workstation</option>
                    <option value="zone" {z_sel}>Zone</option>
                </select>
            </label>
            <label>Department ID <input type="text" name="department_id" value="{loc['department_id'] or ''}"></label>
            <label>Receiver MAC Address <input type="text" name="receiver_mac" value="{loc['receiver_mac'] or ''}"></label>
            <br><br>
            <button type="submit" class="btn btn-primary">Save</button>
            <a href="/admin/locations" class="btn" style="margin-left:10px;">Cancel</a>
        </form>
    </body>
    </html>
    """


@app.post("/admin/locations/{loc_id}/edit")
async def admin_edit_location(loc_id: int, request: StarletteRequest):
    form = await request.form()
    name = form.get("name", "").strip()
    loc_type = form.get("location_type", "workstation")
    dept = form.get("department_id", "").strip() or None
    mac = form.get("receiver_mac", "").strip() or None

    with get_db() as db:
        db.execute(
            "UPDATE locations SET name = ?, location_type = ?, department_id = ?, receiver_mac = ? WHERE id = ?",
            (name, loc_type, dept, mac, loc_id)
        )
    return RedirectResponse(url=f"/admin/locations?msg=Updated+{name}", status_code=303)


@app.get("/admin/locations/{loc_id}/delete")
def admin_delete_location(loc_id: int):
    with get_db() as db:
        loc = db.execute("SELECT name FROM locations WHERE id = ?", (loc_id,)).fetchone()
        if loc:
            db.execute("DELETE FROM location_log WHERE location_id = ?", (loc_id,))
            db.execute("DELETE FROM locations WHERE id = ?", (loc_id,))
    name = loc["name"] if loc else "Unknown"
    return RedirectResponse(url=f"/admin/locations?msg=Deleted+{name}", status_code=303)


# --- History ---

@app.get("/admin/history", response_class=HTMLResponse)
def admin_history():
    with get_db() as db:
        employees = db.execute("""
            SELECT e.*, COUNT(ll.id) as total_entries
            FROM employees e
            LEFT JOIN location_log ll ON ll.employee_id = e.id
            GROUP BY e.id
            ORDER BY e.name
        """).fetchall()

    rows = ""
    for e in employees:
        tag = f'<span class="tag-badge">{e["tag_id"]}</span>' if e["tag_id"] else '<span style="color:#888;">No tag</span>'
        entries = e["total_entries"] or 0
        rows += f"""<tr>
            <td><a href="/admin/history/{e['id']}" style="color:#0066cc;font-weight:500;">{e['name']}</a></td>
            <td>{tag}</td>
            <td>{entries}</td>
        </tr>\n"""

    if not rows:
        rows = '<tr><td colspan="3" style="text-align:center;color:#888;">No employees yet</td></tr>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>BLE Tracking — History</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>Employee History</h1>
        <p class="status">Click an employee to view their location history</p>
        <table>
            <thead><tr><th>Employee</th><th>Tag</th><th>Total Entries</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </body>
    </html>
    """


@app.get("/admin/history/{emp_id}", response_class=HTMLResponse)
def admin_employee_history(emp_id: int, days: int = 7):
    with get_db() as db:
        emp = db.execute("SELECT * FROM employees WHERE id = ?", (emp_id,)).fetchone()
        if not emp:
            return RedirectResponse(url="/admin/history?err=Employee+not+found", status_code=303)

        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        history = db.execute("""
            SELECT ll.*, l.name as location_name, l.department_id, l.location_type
            FROM location_log ll
            JOIN locations l ON l.id = ll.location_id
            WHERE ll.employee_id = ? AND ll.timestamp_in >= ?
            ORDER BY ll.timestamp_in DESC
        """, (emp_id, since)).fetchall()

    rows = ""
    for h in history:
        time_in = h["timestamp_in"] or ""
        time_out = h["timestamp_out"] or ""

        # Calculate duration
        duration = ""
        if h["timestamp_in"] and h["timestamp_out"]:
            try:
                t_in = datetime.fromisoformat(h["timestamp_in"].replace("Z", "+00:00"))
                t_out = datetime.fromisoformat(h["timestamp_out"].replace("Z", "+00:00"))
                diff = t_out - t_in
                total_min = int(diff.total_seconds() / 60)
                hours = total_min // 60
                mins = total_min % 60
                if hours > 0:
                    duration = f"{hours}h {mins}m"
                else:
                    duration = f"{mins}m"
            except Exception:
                duration = "—"
        elif h["timestamp_in"] and not h["timestamp_out"]:
            duration = '<span style="color:#2e7d32;font-weight:600;">Currently here</span>'

        status = "✅" if h["synced_to_netsuite"] else "—"
        loc_type = h["location_type"] or ""
        dept = h["department_id"] or ""

        # Format timestamps for readability
        display_in = time_in.replace("T", " ").replace("Z", "") if time_in else ""
        display_out = time_out.replace("T", " ").replace("Z", "") if time_out else ""

        rows += f"""<tr>
            <td>{h['location_name']}</td>
            <td>{loc_type}</td>
            <td>{dept}</td>
            <td>{display_in}</td>
            <td>{display_out}</td>
            <td>{duration}</td>
            <td>{status}</td>
        </tr>\n"""

    if not rows:
        rows = f'<tr><td colspan="7" style="text-align:center;color:#888;">No history found in the last {days} days</td></tr>'

    # Day filter links
    day_links = ""
    for d in [1, 7, 14, 30]:
        active = "font-weight:700;" if d == days else ""
        day_links += f'<a href="/admin/history/{emp_id}?days={d}" style="margin-right:14px;color:#0066cc;{active}">{d} day{"s" if d > 1 else ""}</a>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>History — {emp['name']}</title><style>{COMMON_STYLES}</style></head>
    <body>
        {NAV_HTML}
        <h1>{emp['name']}</h1>
        <p class="status">Tag: {emp['tag_id'] or 'None assigned'} &nbsp;|&nbsp; NetSuite ID: {emp['netsuite_employee_id'] or 'None'}</p>
        <p>Show: {day_links}</p>
        <table>
            <thead><tr><th>Location</th><th>Type</th><th>Department</th><th>Time In</th><th>Time Out</th><th>Duration</th><th>Synced</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
        <br><a href="/admin/history" class="btn btn-primary">← Back to Employees</a>
    </body>
    </html>
    """


# --- Receiver Health Admin ---

@app.get("/admin/health", response_class=HTMLResponse)
def admin_health():
    receiver_status = engine.get_receiver_status()

    with get_db() as db:
        locs = db.execute("SELECT receiver_mac, name FROM locations").fetchall()
        mac_to_name = {l["receiver_mac"]: l["name"] for l in locs}

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


@app.post("/api/flash/assign")
async def flash_assign(request: StarletteRequest):
    """Assign a receiver MAC to a location."""
    data = await request.json()
    location_id = data.get("location_id")
    receiver_mac = data.get("receiver_mac", "").strip().upper()

    if not location_id or not receiver_mac:
        raise HTTPException(400, "location_id and receiver_mac required")

    with get_db() as db:
        loc = db.execute("SELECT * FROM locations WHERE id = ?", (location_id,)).fetchone()
        if not loc:
            raise HTTPException(404, "Location not found")
        db.execute("UPDATE locations SET receiver_mac = ? WHERE id = ?", (receiver_mac, location_id))

    return {"status": "ok", "location_name": loc["name"], "receiver_mac": receiver_mac}


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
            #assign-box {{
                display: none;
                margin-top: 20px;
                padding: 16px;
                background: #f0f7ff;
                border: 1px solid #b3d1f7;
                border-radius: 6px;
            }}
            #assign-box h3 {{ margin: 0 0 12px 0; font-size: 15px; color: #1a4a7a; }}
            #assign-box label {{ font-size: 14px; }}
            #assign-box select, #assign-box input {{ width: 340px; }}
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

        <!-- Assign to location — shown after successful upload -->
        <div id="assign-box">
            <h3>Assign to Location</h3>
            <label>Detected MAC Address
                <input type="text" id="detected-mac" placeholder="Reading from device...">
            </label>
            <label style="margin-top:12px;">Location
                <select id="location-select"><option value="">Loading...</option></select>
            </label>
            <div style="margin-top:14px;">
                <button class="btn btn-primary" onclick="assignLocation()">Save Assignment</button>
            </div>
            <div id="assign-status" style="margin-top:10px;font-weight:600;font-size:14px;"></div>
        </div>

        <script>
            // Load locations into dropdown
            function loadLocations() {{
                fetch('/api/locations')
                    .then(r => r.json())
                    .then(locs => {{
                        const sel = document.getElementById('location-select');
                        sel.innerHTML = '<option value="">— Select a location —</option>' +
                            locs.map(l => `<option value="${{l.id}}">${{l.name}}</option>`).join('');
                    }});
            }}

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
                document.getElementById('assign-box').style.display = 'none';
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
                        setStatus('✔  Upload complete. Reading device MAC...', 'running');
                        captureMac(port);
                    }} else {{
                        setStatus('✘  Upload failed — see output above.', 'error');
                    }}
                }});

                es.onerror = () => {{ es.close(); btn.disabled = false; setStatus('✘  Connection lost.', 'error'); }};
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
                    setStatus('✔  MAC detected. Select a location and save.', 'success');
                }});

                es.addEventListener('done', e => {{
                    es.close();
                    loadLocations();
                    document.getElementById('assign-box').style.display = 'block';
                    if (parseInt(e.data) !== 0 && !document.getElementById('detected-mac').value) {{
                        setStatus('✔  Upload done. Enter MAC manually below.', 'success');
                    }}
                }});

                es.onerror = () => {{
                    es.close();
                    loadLocations();
                    document.getElementById('assign-box').style.display = 'block';
                    setStatus('✔  Upload done. Enter MAC manually if needed.', 'success');
                }};
            }}

            function assignLocation() {{
                const locationId  = document.getElementById('location-select').value;
                const receiverMac = document.getElementById('detected-mac').value.trim();
                const statusEl    = document.getElementById('assign-status');

                if (!locationId) {{ statusEl.textContent = 'Please select a location.'; statusEl.className = 'error'; return; }}
                if (!receiverMac) {{ statusEl.textContent = 'MAC address is required.'; statusEl.className = 'error'; return; }}

                fetch('/api/flash/assign', {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{location_id: parseInt(locationId), receiver_mac: receiverMac}})
                }})
                .then(r => r.json())
                .then(data => {{
                    statusEl.textContent = `✔  ${{data.receiver_mac}} assigned to ${{data.location_name}}`;
                    statusEl.className = 'success';
                }})
                .catch(() => {{
                    statusEl.textContent = '✘  Assignment failed.';
                    statusEl.className = 'error';
                }});
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