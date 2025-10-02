@echo off
CALL _paths.bat

echo ========================================
echo    Проверка статуса шардирования
echo ========================================
echo.

echo Проверка статуса шардирования MongoDB...
echo База данных: %DBNAME%
echo.

"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_MONGOS%/%DBNAME% --username %user_name% --password %user_password% --file %RUN_PATH%/check_sharding.js

echo.
echo ========================================
echo    Проверка статуса завершена
echo ========================================
echo.

pause
