CALL _paths.bat
CALL _settings.bat

"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_RS02%/%DBNAME% --authenticationDatabase %DBNAME_AUTH%

pause
