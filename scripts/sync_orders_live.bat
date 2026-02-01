@echo off
REM Activate Python virtual environment (update path if changed)
set VENV_PATH=C:\Users\Manu\SellerAPIApplication\venv_win

REM Ensures print() outputs are never buffered (log writes immediately)
set PYTHONUNBUFFERED=1

REM Activate the virtual environment
call "%VENV_PATH%\Scripts\activate.bat"

REM Change to the project root (update path as needed)
cd /d C:\Users\Manu\SellerAPIApplication

REM Ensure logs directory exists
if not exist logs mkdir logs

REM Timestamp start of run for easy debugging (optional but handy)
echo === Started %date% %time% === >> logs\sync_orders_live.log 2>&1

REM Run your sync script, unbuffered stdout/stderr, append all output
"%VENV_PATH%\Scripts\python.exe" -u -m scripts.sync_orders_live >> logs\sync_orders_live.log 2>&1

REM Timestamp end of run
echo === Ended %date% %time% === >> logs\sync_orders_live.log 2>&1

REM Optionally, keep window open for manual debugging (when double-clicked)
REM pause