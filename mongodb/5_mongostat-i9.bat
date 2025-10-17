CALL _paths.bat
CALL _settings.bat

SET SERVER=192.168.88.54
rem SET PORT_MONGOS=27030

"%MONGO_PATH%\mongostat.exe" -u=%USER_NAME% -p=%USER_PASSWORD% -h=%SERVER%:%PORT_MONGOS%,%SERVER%:29101,%SERVER%:29201,%SERVER%:29301 --authenticationDatabase %DBNAME_AUTH% --humanReadable=true --all

pause
