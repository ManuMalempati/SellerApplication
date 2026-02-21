@echo off
REM Run All FBA Imports Script
REM Schedule this using Windows Task Scheduler

REM Path to virtual environment
set VENV_PATH=C:\Users\Manu\SellerAPIApplication\venv_win

REM Ensures print() outputs are never buffered
set PYTHONUNBUFFERED=1

REM Activate the virtual environment
call "%VENV_PATH%\Scripts\activate.bat"

REM Change to the project root
cd /d C:\Users\Manu\SellerAPIApplication

REM Ensure logs directory exists
if not exist logs mkdir logs

REM Timestamp start of run
echo === Started %date% %time% === >> logs\run_all_imports.log 2>&1

REM Run the orchestrator
"%VENV_PATH%\Scripts\python.exe" -u -m scripts.all_imports >> logs\run_all_imports.log 2>&1

REM Timestamp end of run
echo === Ended %date% %time% === >> logs\run_all_imports.log 2>&1
