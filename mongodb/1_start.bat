CALL _paths.bat

@echo on

cd %SCRIPTS_PATH%
rem SET NOTABLESCAN=--notablescan
SET NOTABLESCAN=

if not exist "%DB_PATH%/SH_1_1" mkdir "%DB_PATH%/SH_1_1"
start "rs01-1" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29101 --replSet rs01 --dbpath %DB_PATH%/SH_1_1
rem if not exist "%DB_PATH%/SH_1_2" mkdir "%DB_PATH%/SH_1_2"
rem start "rs01-2" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29102 --replSet rs01 --dbpath %DB_PATH%/SH_1_2
rem if not exist "%DB_PATH%/SH_1_3" mkdir "%DB_PATH%/SH_1_3"
rem start "rs01-3" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29103 --replSet rs01 --dbpath %DB_PATH%/SH_1_3
rem if not exist "%DB_PATH%/SH_1_4" mkdir "%DB_PATH%/SH_1_4"
rem start "rs01-4" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/backup.conf --port 29104 --replSet rs01 --dbpath %DB_PATH%/SH_1_4
rem if not exist "%DB_PATH%/SH_1_ARB" mkdir "%DB_PATH%/SH_1_ARB"
rem start "arb01" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/arbitr.conf --port 29100 --replSet rs01 --dbpath  %DB_PATH%/SH_1_ARB

if not exist "%DB_PATH%/SH_2_1" mkdir "%DB_PATH%/SH_2_1"
start "rs02-1" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29201 --replSet rs02 --dbpath %DB_PATH%/SH_2_1
rem if not exist "%DB_PATH%/SH_2_2" mkdir "%DB_PATH%/SH_2_2"
rem start "rs02-2" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29202 --replSet rs02 --dbpath %DB_PATH%/SH_2_2
rem if not exist "%DB_PATH%/SH_2_3" mkdir "%DB_PATH%/SH_2_3"
rem start "rs02-3" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29203 --replSet rs02 --dbpath %DB_PATH%/SH_2_3
rem if not exist "%DB_PATH%/SH_2_ARB" mkdir "%DB_PATH%/SH_2_ARB"
rem start "arb02" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/arbitr.conf --port 29200 --replSet rs02 --dbpath  %DB_PATH%/SH_2_ARB

if not exist "%DB_PATH%/SH_3_1" mkdir "%DB_PATH%/SH_3_1"
start "rs03-1" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29301 --replSet rs03 --dbpath %DB_PATH%/SH_3_1
rem if not exist "%DB_PATH%/SH_3_2" mkdir "%DB_PATH%/SH_3_2"
rem start "rs03-2" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29302 --replSet rs03 --dbpath %DB_PATH%/SH_3_2
rem if not exist "%DB_PATH%/SH_3_3" mkdir "%DB_PATH%/SH_3_3"
rem start "rs03-3" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/data.conf --port 29303 --replSet rs03 --dbpath %DB_PATH%/SH_3_3
rem if not exist "%DB_PATH%/SH_3_ARB" mkdir "%DB_PATH%/SH_3_ARB"
rem start "arb03" /MIN "%MONGO_PATH%\mongod.exe" %NOTABLESCAN% --config %CONF_PATH%/arbitr.conf --port 29300 --replSet rs03 --dbpath  %DB_PATH%/SH_3_ARB

if not exist "%DB_PATH%/SH_CFG_1" mkdir "%DB_PATH%/SH_CFG_1"
start "cfg-1" /MIN "%MONGO_PATH%\mongod.exe" --config %CONF_PATH%/config.conf --port 29001 --replSet cfg --dbpath %DB_PATH%/SH_CFG_1
rem if not exist "%DB_PATH%/SH_CFG_2" mkdir "%DB_PATH%/SH_CFG_2"
rem start "cfg-2" /MIN "%MONGO_PATH%\mongod.exe" --config %CONF_PATH%/config.conf --port 29002 --replSet cfg --dbpath %DB_PATH%/SH_CFG_2
rem if not exist "%DB_PATH%/SH_CFG_3" mkdir "%DB_PATH%/SH_CFG_3"
rem start "cfg-3" /MIN "%MONGO_PATH%\mongod.exe" --config %CONF_PATH%/config.conf --port 29003 --replSet cfg --dbpath %DB_PATH%/SH_CFG_3


start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos.conf --port 27020
rem start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos.conf --port 27021
rem start "mongos" /MIN "%MONGO_PATH%\mongos.exe" --config %CONF_PATH%/mongos.conf --port 27022

@echo ****************************************************
@echo *
@echo * MongoDB started in other windows
@echo *
@echo ****************************************************
pause
