CALL _paths.bat

@echo on

cd %SCRIPTS_PATH%
rem SET NOTABLESCAN=--notablescan
SET NOTABLESCAN=

start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos-i9.conf --port 27030
rem start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos.conf --port 27021
rem start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos.conf --port 27022

@echo ****************************************************
@echo *
@echo * MongoDB started in other windows
@echo *
@echo ****************************************************
pause
