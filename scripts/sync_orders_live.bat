@echo off
set VENV_PATH=C:\Users\Manu\SellerAPIApplication\venv_win
set PYTHONUNBUFFERED=1

call "%VENV_PATH%\Scripts\activate.bat"
cd /d C:\Users\Manu\SellerAPIApplication

"%VENV_PATH%\Scripts\python.exe" -u -m scripts.sync_orders_live
