@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

set "APP_NAME=metacritic-harvester"
set "ENTRY=./cmd/metacritic-harvester"
set "OUTPUT_ROOT=%ROOT_DIR%output"
set "RELEASE_ROOT=%OUTPUT_ROOT%\releases"
set "LDFLAGS=-s -w -buildid="
set "WINDOWS_ICON=%ROOT_DIR%docs\icons\metacritic-harvester-wolf.ico"
set "WINDOWS_RSRC_TOOL=github.com/akavel/rsrc@v0.10.2"
set "WINDOWS_RSRC_AMD64=%ROOT_DIR%cmd\metacritic-harvester\rsrc_windows_amd64.syso"
set "WINDOWS_RSRC_ARM64=%ROOT_DIR%cmd\metacritic-harvester\rsrc_windows_arm64.syso"

if not exist "%OUTPUT_ROOT%" mkdir "%OUTPUT_ROOT%"
if exist "%RELEASE_ROOT%" rmdir /s /q "%RELEASE_ROOT%"
mkdir "%RELEASE_ROOT%"

echo Building release binaries for %APP_NAME%
echo Output: %RELEASE_ROOT%
echo.

call :prepare_windows_icon || goto :fail
call :build_target windows amd64 .exe || goto :fail
call :build_target windows arm64 .exe || goto :fail
call :build_target linux amd64 "" || goto :fail
call :build_target linux arm64 "" || goto :fail
call :build_target darwin amd64 "" || goto :fail
call :build_target darwin arm64 "" || goto :fail

call :cleanup_windows_icon

powershell -NoProfile -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$files = Get-ChildItem -Path '%RELEASE_ROOT%' -File | Where-Object { $_.Name -ne 'SHA256SUMS.txt' } | Sort-Object Name;" ^
  "$lines = foreach ($file in $files) { $hash = (Get-FileHash -Algorithm SHA256 $file.FullName).Hash.ToLowerInvariant(); '{0} *{1}' -f $hash, $file.Name };" ^
  "Set-Content -Path '%RELEASE_ROOT%\SHA256SUMS.txt' -Value $lines -Encoding ASCII"
if errorlevel 1 goto :fail

echo.
echo Release binaries created successfully:
dir /b "%RELEASE_ROOT%"
exit /b 0

:prepare_windows_icon
if not exist "%WINDOWS_ICON%" (
  echo Windows icon not found: %WINDOWS_ICON%
  exit /b 1
)

echo [windows] embedding icon from %WINDOWS_ICON%
go run %WINDOWS_RSRC_TOOL% -arch amd64 -ico "%WINDOWS_ICON%" -o "%WINDOWS_RSRC_AMD64%"
if errorlevel 1 exit /b 1
go run %WINDOWS_RSRC_TOOL% -arch arm64 -ico "%WINDOWS_ICON%" -o "%WINDOWS_RSRC_ARM64%"
if errorlevel 1 exit /b 1
exit /b 0

:cleanup_windows_icon
if exist "%WINDOWS_RSRC_AMD64%" del /q "%WINDOWS_RSRC_AMD64%"
if exist "%WINDOWS_RSRC_ARM64%" del /q "%WINDOWS_RSRC_ARM64%"
exit /b 0

:build_target
set "TARGET_GOOS=%~1"
set "TARGET_GOARCH=%~2"
set "TARGET_EXT=%~3"
set "BINARY_PATH=%RELEASE_ROOT%\%APP_NAME%_%TARGET_GOOS%_%TARGET_GOARCH%%TARGET_EXT%"

echo [%TARGET_GOOS%/%TARGET_GOARCH%] go build
set "CGO_ENABLED=0"
set "GOOS=%TARGET_GOOS%"
set "GOARCH=%TARGET_GOARCH%"
go build -trimpath -ldflags "%LDFLAGS%" -o "%BINARY_PATH%" "%ENTRY%"
if errorlevel 1 exit /b 1

exit /b 0

:fail
call :cleanup_windows_icon
echo.
echo Build failed.
exit /b 1
