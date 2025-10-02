@echo off
CALL _paths.bat

echo ========================================
echo    Управление балансировщиком
echo ========================================
echo.

if "%1"=="" (
    echo Использование: manage_balancer.bat [start^|stop^|status^|enable^|disable]
    echo.
    echo Доступные команды:
    echo   start   - запустить балансировщик
    echo   stop    - остановить балансировщик
    echo   enable  - включить балансировщик
    echo   disable - отключить балансировщик
    echo   status  - проверить состояние (по умолчанию)
    echo.
    set /p action="Введите команду (status): "
    if "!action!"=="" set action=status
) else (
    set action=%1
)

echo.
echo Выполнение команды: !action!
echo База данных: %DBNAME%
echo.

"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_MONGOS%/%DBNAME% --username %user_name% --password %user_password% --file %RUN_PATH%/manage_balancer.js --eval "args=['!action!']"

echo.
echo ========================================
echo    Управление балансировщиком завершено
echo ========================================
echo.

pause
