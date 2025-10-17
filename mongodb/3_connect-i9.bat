CALL _paths.bat
CALL _settings.bat

SET SERVER=192.168.88.54
rem SET SERVER=192.168.88.55
rem SET PORT_MONGOS=27030

"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% --file %RUN_PATH%/common.js --shell

pause
