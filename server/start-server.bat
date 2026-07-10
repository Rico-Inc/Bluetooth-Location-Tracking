@echo off
setlocal

REM ============================================================
REM BLE Tracking Server launcher
REM
REM Pulls Supabase credentials from Azure Key Vault (ricoincbikeyvault)
REM and exports them as env vars for the FastAPI process.
REM
REM Env selector:
REM   SUPABASE_ENV=dev   (default) — points at rico-dev sandbox
REM   SUPABASE_ENV=prod            — points at rico-prod (Phase 3 cutover)
REM
REM Requires: az CLI signed in as a user with 'Key Vault Secrets User'
REM role on ricoincbikeyvault. If this .bat is auto-launched as SYSTEM,
REM az has no cached credentials — run it under a real user account.
REM ============================================================

if not defined SUPABASE_ENV set SUPABASE_ENV=dev
set VAULT=ricoincbikeyvault
set AZ="C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"

echo [Launcher] Loading %SUPABASE_ENV% secrets from %VAULT%...

for /f "usebackq delims=" %%v in (`%AZ% keyvault secret show --vault-name %VAULT% --name supabase-url-%SUPABASE_ENV% --query value -o tsv`) do set SUPABASE_URL=%%v
if not defined SUPABASE_URL (
    echo [Launcher] ERROR: could not load supabase-url-%SUPABASE_ENV%. Check 'az login' and RBAC.
    pause
    exit /b 1
)

for /f "usebackq delims=" %%v in (`%AZ% keyvault secret show --vault-name %VAULT% --name supabase-service-role-key-%SUPABASE_ENV% --query value -o tsv`) do set SUPABASE_SERVICE_ROLE_KEY=%%v
if not defined SUPABASE_SERVICE_ROLE_KEY (
    echo [Launcher] ERROR: could not load supabase-service-role-key-%SUPABASE_ENV%.
    pause
    exit /b 1
)

for /f "usebackq delims=" %%v in (`%AZ% keyvault secret show --vault-name %VAULT% --name supabase-db-password-%SUPABASE_ENV% --query value -o tsv`) do set SUPABASE_DB_PASSWORD=%%v
if not defined SUPABASE_DB_PASSWORD (
    echo [Launcher] ERROR: could not load supabase-db-password-%SUPABASE_ENV%.
    pause
    exit /b 1
)

if not defined MQTT_BROKER set MQTT_BROKER=localhost

echo [Launcher] SUPABASE_URL = %SUPABASE_URL%
echo [Launcher] MQTT_BROKER  = %MQTT_BROKER%
echo [Launcher] Starting uvicorn...
echo.

cd C:\ble-employee-tracking\server
call C:\ble-tracking\ble-env\Scripts\activate
uvicorn server:app --host 0.0.0.0 --port 8000
pause
endlocal
