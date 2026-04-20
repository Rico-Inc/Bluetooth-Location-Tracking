@echo off
cd C:\ble-tracking
call ble-env\Scripts\activate
uvicorn server:app --reload --host 0.0.0.0 --port 8000
pause