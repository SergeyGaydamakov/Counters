echo Run this script in minimize console
@echo off
if "%1"=="done" goto runtime
start "" /min %0 done
exit

:runtime
@echo on
echo Kill mongod process with name rs-01, rs-02, rs-03, cfg, mongos
taskkill.exe /F /FI "WINDOWTITLE eq rs01-1"
taskkill.exe /F /FI "WINDOWTITLE eq rs02-1"
taskkill.exe /F /FI "WINDOWTITLE eq rs03-1"
taskkill.exe /F /FI "WINDOWTITLE eq cfg-1"
taskkill.exe /F /FI "WINDOWTITLE eq mongos"

rem pause
