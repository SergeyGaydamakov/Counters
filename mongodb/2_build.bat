CALL _paths.bat
CALL _settings.bat

"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_CFG%/%DBNAME% %RUN_PATH%/build_rscfg.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS01%/%DBNAME% %RUN_PATH%/build_rs01.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS02%/%DBNAME% %RUN_PATH%/build_rs02.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS03%/%DBNAME% %RUN_PATH%/build_rs03.js
"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% %RUN_PATH%/build_cl.js

pause
