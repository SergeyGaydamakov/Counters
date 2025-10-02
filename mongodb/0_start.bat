CALL _paths.bat

@echo on

rem cd %SCRIPTS_PATH%
rem SET NOTABLESCAN=--notablescan
SET NOTABLESCAN=

if not exist "%DB_PATH%/DB" mkdir "%DB_PATH%/DB"
start "mongodb" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --port 27017 --dbpath %DB_PATH%/DB
rem start "mongodb" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 27017 --dbpath %DB_PATH%/DB

@echo ****************************************************
@echo *
@echo * MongoDB started in other windows
@echo *
@echo ****************************************************
pause
