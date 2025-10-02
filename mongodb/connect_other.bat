CALL _paths.bat

rem Настройки для подключения к другому серверу
SET OTHER_SERVER=192.168.1.100
SET OTHER_PORT=27017
SET OTHER_DBNAME=production_db
SET OTHER_USERNAME=
SET OTHER_PASSWORD=

rem Формируем строку подключения
if "%OTHER_USERNAME%"=="" (
    SET CONNECTION_STRING=%OTHER_SERVER%:%OTHER_PORT%/%OTHER_DBNAME%
) else (
    SET CONNECTION_STRING=%OTHER_USERNAME%:%OTHER_PASSWORD%@%OTHER_SERVER%:%OTHER_PORT%/%OTHER_DBNAME%
)

echo Подключение к серверу: %OTHER_SERVER%:%OTHER_PORT%
echo База данных: %OTHER_DBNAME%
echo.

rem Подключение к другому серверу
"%MONGO_PATH_SHELL%\mongosh.exe" "mongodb://%CONNECTION_STRING%"

pause
