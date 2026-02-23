@echo off
REM Activate Python virtual environment (update path if changed)
set VENV_PATH=C:\Users\Manu\SellerAPIApplication\venv_win

REM Ensures print() outputs are never buffered (log updates immediately)
set PYTHONUNBUFFERED=1

REM Activate the virtual environment
call "%VENV_PATH%\Scripts\activate.bat"

REM Change to the project root (update path as needed)
cd /d C:\Users\Manu\SellerAPIApplication

REM Ensure logs directory exists
if not exist logs mkdir logs

REM Timestamp start of run for easy debugging
echo === Started %date% %time% === >> logs\transactions_sync_live_log.txt 2>&1

REM Run the live sync script with unbuffered output
"%VENV_PATH%\Scripts\python.exe" -u -m scripts.sync_transactions_live >> logs\transactions_sync_live_log.txt 2>&1

REM Timestamp end of run for easy debugging
echo === Ended %date% %time% === >> logs\transactions_sync_live_log.txt 2>&1

REM Optionally, keep window open on manual run for debugging
pause