@echo off
setlocal
cd /d "%~dp0"
set "PYTHON_EXE=..\Crypto Strategy Lab\.venv\Scripts\pythonw.exe"
if not exist "%PYTHON_EXE%" (
  echo ERROR: Crypto Strategy Lab Python environment was not found.
  pause
  exit /b 1
)
start "Binance Data Hub" "%PYTHON_EXE%" app.py

