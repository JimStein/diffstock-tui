@echo off
setlocal
setlocal EnableDelayedExpansion
cd /d "%~dp0"
set "NO_PAUSE="
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"
set "BUILD_TARGET_DIR=target_webui"
set "WEBUI_PORT="

echo [1/4] Stopping existing diffstock-tui process...
taskkill /F /IM diffstock-tui.exe >nul 2>nul

echo [2/4] Building release (directml)...
cargo build --release --features directml --target-dir %BUILD_TARGET_DIR%
if errorlevel 1 (
  echo Build failed. Please fix errors and retry.
  if not defined NO_PAUSE pause
  exit /b 1
)

echo [3/4] Setting runtime environment...
set "ORT_DYLIB_PATH=%CD%\.runtime\ort_dml_1_24_1\onnxruntime\capi\onnxruntime.dll"
set "DIFFSTOCK_ORT_MODEL=%CD%\model_weights.onnx"

for %%P in (8099 8100 8101 8102 8103 8104 8105 8106 8107 8108 8109 8110 8111 8112 8113 8114 8115 8116 8117 8118 8119 8120 8121 8122 8123 8124 8125) do (
  netstat -ano -p tcp | findstr /R /C:":%%P .*LISTENING" >nul
  if errorlevel 1 if not defined WEBUI_PORT set "WEBUI_PORT=%%P"
)
if not defined WEBUI_PORT (
  echo No free port found in range 8099-8125.
  if not defined NO_PAUSE pause
  exit /b 1
)

echo [4/4] Starting web UI on port !WEBUI_PORT!... 
start "" /min "%CD%\%BUILD_TARGET_DIR%\release\diffstock-tui.exe" --webui --webui-port !WEBUI_PORT! --compute-backend directml

timeout /t 3 /nobreak >nul
echo Health check:
powershell -NoProfile -Command "try { $uri = 'http://localhost:!WEBUI_PORT!/api/health'; (Invoke-WebRequest -Uri $uri -UseBasicParsing).Content; Write-Host ('WebUI URL: http://localhost:!WEBUI_PORT!/'); } catch { Write-Host '{\"ok\":false}'; exit 1 }"

echo Done.
if not defined NO_PAUSE pause
