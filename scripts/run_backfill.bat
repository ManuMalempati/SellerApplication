:: backfill_orders.bat
@echo off
REM Edit VENV_PATH to point to your Python virtualenv folder
set VENV_PATH=C:\path\to\venv

if not exist "%VENV_PATH%\Scripts\activate.bat" (
  echo Virtualenv activate not found at %VENV_PATH%\Scripts\activate.bat
  echo Please edit this file and set VENV_PATH correctly.
  pause
  exit /b 1
)

call "%VENV_PATH%\Scripts\activate.bat"

REM Change to repository root if needed (adjust path)
cd /d "%~dp0"

REM Run backfill (module form uses package imports)
python -m app.backfill_orders

REM Uncomment to keep the window open when run manually
pause