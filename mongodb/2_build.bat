CALL _paths.bat
CALL _settings.bat

"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_CFG%/%DBNAME% %RUN_PATH%/rscfg_build.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS01%/%DBNAME% %RUN_PATH%/rs01_build.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS02%/%DBNAME% %RUN_PATH%/rs02_build.js
"%MONGO_PATH_SHELL%\mongosh.exe" %SERVER%:%PORT_RS03%/%DBNAME% %RUN_PATH%/rs03_build.js
"%MONGO_PATH_SHELL%\mongosh.exe" mongodb://%USER_NAME%:%USER_PASSWORD%@%SERVER%:%PORT_MONGOS%/%DBNAME% --authenticationDatabase %DBNAME_AUTH% %RUN_PATH%/cl_build.js

pause
