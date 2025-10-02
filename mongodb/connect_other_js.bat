CALL _paths.bat

rem Запуск JavaScript файла для подключения к другому серверу
"%MONGO_PATH_SHELL%\mongosh.exe" %RUN_PATH%/connect_other.js

pause
