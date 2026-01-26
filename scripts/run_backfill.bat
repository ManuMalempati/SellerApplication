@echo off
REM change to script folder to make relative paths safe
cd /d "C:\Users\Manu\SellerAPIApplication\scripts"

REM ensure logs folder exists (no-op if already present)
if not exist "C:\Users\Manu\SellerAPIApplication\logs" mkdir "C:\Users\Manu\SellerAPIApplication\logs"

REM run python and append stdout+stderr to rotating log
"C:\Users\Manu\SellerAPIApplication\venv_win\Scripts\python.exe" "C:\Users\Manu\SellerAPIApplication\scripts\backfill_orders.py" >> "C:\Users\Manu\SellerAPIApplication\logs\backfill_orders.log" 2>&1

REM capture the exit code Task Scheduler will see
echo %ERRORLEVEL% > "C:\Users\Manu\SellerAPIApplication\logs\backfill_last_exit_code.txt"

REM write a timestamped marker for easier debugging
echo %DATE% %TIME% ExitCode=%ERRORLEVEL% >> "C:\Users\Manu\SellerAPIApplication\logs\backfill_run_history.log"
