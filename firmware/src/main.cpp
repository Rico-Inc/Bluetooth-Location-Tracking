/*
 * BLE Employee Location Tracking — ESP32 Receiver Firmware
 * ==========================================================
 * 
 * Setup (PlatformIO):
 *   1. Install VS Code + PlatformIO extension
 *   2. Create new project: Board = "esp32dev"
 *   3. Replace src/main.cpp with this file
 *   4. Add to platformio.ini under [env]:
 *        lib_deps =
 *          h2zero/NimBLE-Arduino@^1.4.0
 *          knolleary/PubSubClient@^2.8
 *        monitor_speed = 115200
 *   5. Build and upload
 * 
 * What this does:
 *   - Scans for BLE advertisements (iBeacon tags)
 *   - Filters for tags matching a known MAC prefix
 *   - Collects readings over a scan window
 *   - Publishes tag sightings + RSSI to MQTT broker
 *   - Reconnects WiFi and MQTT automatically
 */

#include <NimBLEDevice.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <esp_wifi.h>
#include <time.h>

// ─────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────

// WiFi
const char* WIFI_SSID     = "ricoinc-whse";
const char* WIFI_PASSWORD = "RicoTag123!";

// MQTT
const char* MQTT_BROKER   = "192.168.2.40";
const int   MQTT_PORT     = 1883;
const char* MQTT_TOPIC    = "ble/readings";
const char* MQTT_CLIENT_PREFIX = "ble-rx-";

// BLE Tag filtering — first 3 bytes of tag MAC addresses
const char* TAG_MAC_PREFIX = "DC0D30";

// Scan timing
const int SCAN_DURATION_SEC  = 5;
const int REPORT_INTERVAL_MS = 15000;

// NTP for timestamps
const char* NTP_SERVER = "pool.ntp.org";
const long  GMT_OFFSET = 0;
const int   DST_OFFSET = 0;


// ─────────────────────────────────────────────
// GLOBALS
// ─────────────────────────────────────────────

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
NimBLEScan* bleScan;

struct TagReading {
    char mac[18];
    int  rssi;
    int  count;
    int  rssiSum;
};

#define MAX_TAGS 50
TagReading tagReadings[MAX_TAGS];
int tagCount = 0;

char receiverMac[18] = "";
char mqttClientId[32] = "";

unsigned long lastReportTime = 0;


// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

void getReceiverMac() {
    uint8_t mac[6];
    WiFi.macAddress(mac);
    snprintf(receiverMac, sizeof(receiverMac),
        "%02X:%02X:%02X:%02X:%02X:%02X",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    snprintf(mqttClientId, sizeof(mqttClientId), "%s%02X%02X%02X",
        MQTT_CLIENT_PREFIX, mac[3], mac[4], mac[5]);
}

bool matchesPrefix(const char* mac) {
    if (strlen(TAG_MAC_PREFIX) == 0) return true;

    char cleanMac[13] = "";
    int j = 0;
    for (int i = 0; mac[i] && j < 12; i++) {
        if (mac[i] != ':') {
            cleanMac[j++] = toupper(mac[i]);
        }
    }
    cleanMac[j] = '\0';

    return strncmp(cleanMac, TAG_MAC_PREFIX, strlen(TAG_MAC_PREFIX)) == 0;
}

void addReading(const char* mac, int rssi) {
    for (int i = 0; i < tagCount; i++) {
        if (strcmp(tagReadings[i].mac, mac) == 0) {
            tagReadings[i].rssiSum += rssi;
            tagReadings[i].count++;
            if (rssi > tagReadings[i].rssi) {
                tagReadings[i].rssi = rssi;
            }
            return;
        }
    }

    if (tagCount < MAX_TAGS) {
        strncpy(tagReadings[tagCount].mac, mac, 17);
        tagReadings[tagCount].mac[17] = '\0';
        tagReadings[tagCount].rssi = rssi;
        tagReadings[tagCount].rssiSum = rssi;
        tagReadings[tagCount].count = 1;
        tagCount++;
    }
}

void clearReadings() {
    tagCount = 0;
}

String getTimestamp() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo, 1000)) {
        unsigned long s = millis() / 1000;
        return "2026-01-01T00:00:" + String(s % 60) + "Z";
    }
    char buf[30];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
    return String(buf);
}


// ─────────────────────────────────────────────
// BLE SCAN CALLBACK
// ─────────────────────────────────────────────

class ScanCallbacks : public NimBLEAdvertisedDeviceCallbacks {
    void onResult(NimBLEAdvertisedDevice* device) {
        const char* mac = device->getAddress().toString().c_str();
        int rssi = device->getRSSI();

        if (matchesPrefix(mac)) {
            addReading(mac, rssi);
        }
    }
};


// ─────────────────────────────────────────────
// WiFi
// ─────────────────────────────────────────────

void connectWiFi() {
    if (WiFi.status() == WL_CONNECTED) return;

    Serial.print("[WiFi] Connecting to ");
    Serial.println(WIFI_SSID);

    WiFi.mode(WIFI_STA);
    WiFi.setTxPower(WIFI_POWER_19_5dBm);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 30) {
        delay(1000);
        Serial.print(".");
        attempts++;
    }

    if (WiFi.status() == WL_CONNECTED) {
        Serial.print("\n[WiFi] Connected — IP: ");
        Serial.println(WiFi.localIP());
        Serial.print("[WiFi] RSSI: ");
        Serial.print(WiFi.RSSI());
        Serial.println(" dBm");
    } else {
        Serial.println("\n[WiFi] FAILED — will retry next loop");
    }
}


// ─────────────────────────────────────────────
// MQTT
// ─────────────────────────────────────────────

void connectMQTT() {
    if (mqttClient.connected()) return;

    Serial.print("[MQTT] Connecting to ");
    Serial.print(MQTT_BROKER);
    Serial.print("...");

    mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
    mqttClient.setBufferSize(1024);

    int attempts = 0;
    while (!mqttClient.connected() && attempts < 5) {
        if (mqttClient.connect(mqttClientId)) {
            Serial.println(" connected");
            return;
        }
        Serial.print(".");
        delay(2000);
        attempts++;
    }
    Serial.println(" FAILED — will retry next loop");
}

void publishReadings() {
    String json = "{\"receiver_mac\":\"";
    json += receiverMac;
    json += "\",\"readings\":[";

    for (int i = 0; i < tagCount; i++) {
        if (i > 0) json += ",";
        int avgRssi = tagReadings[i].rssiSum / tagReadings[i].count;
        json += "{\"tag_id\":\"";
        json += tagReadings[i].mac;
        json += "\",\"rssi\":";
        json += String(avgRssi);
        json += "}";
    }

    json += "],\"timestamp\":";
    json += "\"" + getTimestamp() + "\"";
    json += ",\"wifi_rssi\":";
    json += String(WiFi.RSSI());
    json += "}";

    if (mqttClient.publish(MQTT_TOPIC, json.c_str())) {
        Serial.print("[MQTT] Published ");
        Serial.print(tagCount);
        Serial.print(tagCount == 1 ? " tag — " : " tags — ");
        Serial.print(json.length());
        Serial.println(" bytes");
    } else {
        Serial.println("[MQTT] Publish FAILED");
    }

    Serial.println(json);
}


// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(1000);
    Serial.println("\n========================================");
    Serial.println("  BLE Receiver — Employee Tracking");
    Serial.println("========================================");

    // WiFi first
    connectWiFi();
    getReceiverMac();
    Serial.print("[Info] Receiver MAC: ");
    Serial.println(receiverMac);
    Serial.print("[Info] MQTT Client ID: ");
    Serial.println(mqttClientId);

    // NTP time sync
    configTime(GMT_OFFSET, DST_OFFSET, NTP_SERVER);
    Serial.println("[NTP] Syncing time...");

    // MQTT
    connectMQTT();

    // BLE last
    Serial.println("[BLE] Initializing...");
    NimBLEDevice::init("");
    bleScan = NimBLEDevice::getScan();
    bleScan->setAdvertisedDeviceCallbacks(new ScanCallbacks(), true);
    bleScan->setActiveScan(false);
    bleScan->setInterval(100);
    bleScan->setWindow(80);
    Serial.println("[BLE] Scanner ready");

    Serial.print("[BLE] Tag MAC prefix filter: ");
    Serial.println(strlen(TAG_MAC_PREFIX) > 0 ? TAG_MAC_PREFIX : "(none — all devices)");
    Serial.println("[Ready] Scanning for tags...\n");
}

void loop() {
    // Always check WiFi at the top of every loop
    connectWiFi();
    mqttClient.loop();

    // === PHASE 1: BLE SCAN ===
    Serial.println("[BLE] Scanning...");
    bleScan->start(SCAN_DURATION_SEC, false);
    bleScan->clearResults();

    // === PHASE 2: REPORT READINGS ===
    unsigned long now = millis();
    if (now - lastReportTime >= REPORT_INTERVAL_MS || lastReportTime == 0) {

        // Stop BLE scan to free radio
        NimBLEDevice::getScan()->stop();
        delay(200);

        // Reconnect WiFi if scan dropped it
        connectWiFi();

        if (WiFi.status() == WL_CONNECTED) {
            Serial.print("[WiFi] RSSI: ");
            Serial.print(WiFi.RSSI());
            Serial.println(" dBm");

            connectMQTT();
            if (mqttClient.connected()) {
                publishReadings();
            } else {
                Serial.println("[MQTT] Not connected — skipping publish");
            }
        } else {
            Serial.println("[WiFi] Not connected — skipping this cycle");
        }

        clearReadings();
        lastReportTime = now;
    }

    delay(500);
}