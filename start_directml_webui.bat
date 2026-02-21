@echo off
setlocal
cd /d "%~dp0"
set "NO_PAUSE="
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"

echo [1/4] Stopping existing diffstock-tui process...
taskkill /F /IM diffstock-tui.exe >nul 2>nul

echo [2/4] Building release (directml)...
cargo build --release --features directml
if errorlevel 1 (
  echo Build failed. Please fix errors and retry.
  if not defined NO_PAUSE pause
  exit /b 1
)

echo [3/4] Setting runtime environment...
set "ORT_DYLIB_PATH=%CD%\.runtime\ort_dml_1_24_1\onnxruntime\capi\onnxruntime.dll"
set "DIFFSTOCK_ORT_MODEL=%CD%\model_weights.onnx"

echo [4/4] Starting web UI on port 8099...
start "" /min "%CD%\target\release\diffstock-tui.exe" --webui --webui-port 8099 --compute-backend directml

timeout /t 3 /nobreak >nul
echo Health check:
powershell -NoProfile -Command "try { (Invoke-WebRequest -Uri 'http://localhost:8099/api/health' -UseBasicParsing).Content } catch { Write-Host '{\"ok\":false}'; exit 1 }"

echo Done.
if not defined NO_PAUSE pause
