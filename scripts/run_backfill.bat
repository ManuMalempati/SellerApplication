@echo off
REM Run from repo/scripts folder
pushd "%~dp0\.."
set REPO=%CD%

REM determine python: prefer venv_win\Scripts\python.exe
set VENV_PY=%REPO%\venv_win\Scripts\python.exe
if exist "%VENV_PY%" (
  set PY="%VENV_PY%"
) else (
  where python >nul 2>&1
  if errorlevel 1 (
    echo Python executable not found in venv or PATH. Exiting with code 9009.
    popd
    exit /b 9009
  )
  set PY=python
)

REM ensure logs dir exists
if not exist "%REPO%\logs" mkdir "%REPO%\logs"

REM compute start/end (last 1 year) and run the backfill script
for /f "usebackq tokens=1,2" %%A in (`powershell -NoProfile -Command "$s=(Get-Date).AddYears(-1).ToString('yyyy-MM-dd'); $e=(Get-Date).ToString('yyyy-MM-dd'); Write-Output \"$s $e\""` ) do (
  set START=%%A
  set END=%%B
)

echo Starting backfill from %START% to %END% >> "%REPO%\logs\backfill_orders.log"

REM call the python script (ensure script name matches: backfill_orders.py)
"%PY%" "%REPO%\scripts\backfill_orders.py" --start %START% --end %END% --verbose >> "%REPO%\logs\backfill_orders.log" 2>&1

echo %ERRORLEVEL% > "%REPO%\logs\backfill_last_exit_code.txt"
echo %DATE% %TIME% ExitCode=%ERRORLEVEL% >> "%REPO%\logs\backfill_run_history.log"

popd
exit /b %ERRORLEVEL%