CALL _paths.bat

"%MONGO_PATH_SHELL%\mongosh.exe"
rem "%MONGO_PATH_SHELL%\mongosh.exe"  %RUN_PATH%/connect_data.js --nodb --shell

rem "%MONGO_PATH_SHELL%\mongosh.exe"  %RUN_PATH%/connect_data.js --nodb --shell --eval "snippet load-all"

pause
