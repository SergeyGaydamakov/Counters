@echo off
CALL _paths.bat
CALL _settings.bat

echo ========================================
echo    Удаление баз данных
echo ========================================
echo.

echo Подключение к MongoDB...
echo База данных: %DBNAME%
echo.

"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% --file %RUN_PATH%/drop_databases.js

echo.
echo ========================================
echo    Удаление баз данных завершено
echo ========================================
echo.

pause
