CALL _paths.bat

echo ========================================
echo    Интерактивное подключение к MongoDB
echo ========================================
echo.

set /p SERVER="Введите адрес сервера (по умолчанию localhost): "
if "%SERVER%"=="" set SERVER=localhost

set /p PORT="Введите порт (по умолчанию 27017): "
if "%PORT%"=="" set PORT=27017

set /p DBNAME="Введите имя базы данных (по умолчанию test): "
if "%DBNAME%"=="" set DBNAME=test

set /p USERNAME="Введите имя пользователя (оставьте пустым если не нужно): "

if not "%USERNAME%"=="" (
    set /p PASSWORD="Введите пароль: "
    set CONNECTION_STRING=mongodb://%USERNAME%:%PASSWORD%@%SERVER%:%PORT%/%DBNAME%
) else (
    set CONNECTION_STRING=mongodb://%SERVER%:%PORT%/%DBNAME%
)

echo.
echo Подключение к: %CONNECTION_STRING%
echo.

rem Подключение
"%MONGO_PATH_SHELL%\mongosh.exe" "%CONNECTION_STRING%"

pause
