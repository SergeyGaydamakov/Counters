CALL _paths.bat
CALL _settings.bat

SET PORT_MONGOS=27021

"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% --file %RUN_PATH%/common.js --shell

pause
