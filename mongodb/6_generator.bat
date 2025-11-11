@echo off
CALL _paths.bat
CALL _settings.bat

echo ========================================
echo    Настройка шардирования MongoDB
echo ========================================
echo.

echo Подключение к MongoDB и настройка шардирования...
echo База данных: %DBNAME%
echo.

"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% --file %RUN_PATH%/generator.js

echo.
echo ========================================
echo    Настройка шардирования завершена
echo ========================================
echo.

pause
