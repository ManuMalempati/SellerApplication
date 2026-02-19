@echo off
REM ============================================================
REM  Fee Estimates Cache Refresh Script
REM  Schedule this to run daily or hourly via Task Scheduler
REM ============================================================

REM ---- Activate Python virtual environment ----
set VENV_PATH=C:\Users\Manu\SellerAPIApplication\venv_win
set PYTHONUNBUFFERED=1

call "%VENV_PATH%\Scripts\activate.bat"

REM ---- Change to project root ----
cd /d C:\Users\Manu\SellerAPIApplication

REM ---- Ensure logs directory exists ----
if not exist logs mkdir logs

REM ---- Timestamp start ----
echo === Started %date% %time% === >> logs\fee_cache.log 2>&1

REM ---- Run the fee cache refresh script ----
"%VENV_PATH%\Scripts\python.exe" -u -m scripts.refresh_fee_cache >> logs\fee_cache.log 2>&1

REM ---- Timestamp end ----
echo === Ended %date% %time% === >> logs\fee_cache.log 2>&1
