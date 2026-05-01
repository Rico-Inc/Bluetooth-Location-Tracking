@echo off
setlocal enabledelayedexpansion

echo ============================================
echo  Restarting BLE Tracking Server
echo ============================================

REM --- Step 1: Find and kill whatever is listening on port 8000 ---
echo.
echo [1/2] Stopping existing server on port 8000...

set "FOUND="
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    set "FOUND=%%a"
    echo   Found PID %%a — killing...
    taskkill /F /PID %%a >nul 2>&1
    if !errorlevel! equ 0 (
        echo   Stopped PID %%a
    ) else (
        echo   Failed to kill PID %%a — may need to run as Administrator
    )
)

if not defined FOUND (
    echo   No process listening on port 8000 — nothing to stop.
)

REM Give the OS a moment to release the port
timeout /t 2 /nobreak >nul

REM --- Step 2: Start the server fresh in a new window ---
echo.
echo [2/2] Starting server...
start "BLE Tracking Server" cmd /k "%~dp0start-server.bat"

echo.
echo Done. Server starting in new window.
timeout /t 3 /nobreak >nul
endlocal
