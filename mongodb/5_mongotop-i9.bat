CALL _paths.bat
CALL _settings.bat

SET SERVER=192.168.88.54
rem SET PORT_MONGOS=27030

rem "%MONGO_PATH%\mongotop.exe" 30 -u=%USER_NAME% -p=%USER_PASSWORD% -h=%SERVER%:%PORT_MONGOS%,%SERVER%:29101,%SERVER%:29201,%SERVER%:29301 --authenticationDatabase %DBNAME_AUTH%
"%MONGO_PATH%\mongotop.exe" 30 -u=%USER_NAME% -p=%USER_PASSWORD% -h=%SERVER%:29101 --authenticationDatabase %DBNAME_AUTH%

pause
