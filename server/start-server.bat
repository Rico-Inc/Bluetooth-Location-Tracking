@echo off
cd C:\ble-employee-tracking\server
call C:\ble-tracking\ble-env\Scripts\activate
uvicorn server:app --host 0.0.0.0 --port 8000
pause