cls

@echo off
CHCP 65001 
echo ***
echo *
echo * Кодировка UTF-8
echo *

rem Ищем MongoShell
SET MONGO_PATH_WRK=C:\Sergeyg\MongoDB\bin
SET MONGO_PATH_8_2=C:\Program Files\MongoDB\Server\8.2\bin
SET MONGO_PATH_7_0=C:\Program Files\MongoDB\Server\7.0\bin
SET MONGO_PATH_6_0=C:\Program Files\MongoDB\Server\6.0\bin


SET MONGO_PATH_SHELL_WRK=C:\Sergeyg\Distr\Mongodb\bin
SET MONGO_PATH_SHELL=C:\Program Files\mongosh

IF EXIST "%MONGO_PATH_6_0%\mongod.exe" SET MONGO_PATH=%MONGO_PATH_6_0%
IF EXIST "%MONGO_PATH_7_0%\mongod.exe" SET MONGO_PATH=%MONGO_PATH_7_0%
IF EXIST "%MONGO_PATH_8_2%\mongod.exe" SET MONGO_PATH=%MONGO_PATH_8_2%
IF EXIST "%MONGO_PATH_WRK%\mongod.exe" SET MONGO_PATH=%MONGO_PATH_WRK%

IF EXIST "%MONGO_PATH_SHELL_WRK%\mongosh.exe" SET MONGO_PATH_SHELL=%MONGO_PATH_SHELL_WRK%
IF NOT EXIST "%MONGO_PATH_SHELL%\mongosh.exe" GOTO mongo_not_found
rem IF NOT EXIST "%MONGO_PATH_SHELL%\mongosh.exe" GOTO mongo_not_found
IF NOT EXIST "%MONGO_PATH%\mongod.exe" GOTO mongo_not_found
IF NOT EXIST "%MONGO_PATH%\mongos.exe" GOTO mongo_not_found

echo * Каталог MongoDB:   %MONGO_PATH%
GOTO ok

:mongo_not_found
echo * На компьютере не найдены все или часть файлов MongoDB в следующих каталогах:
echo * %MONGO_PATH_6_0%
echo * %MONGO_PATH_7_0%
echo * %MONGO_PATH_8_2%
echo * %MONGO_PATH_SERGEYG%
echo * 
echo * Mongo Shell:
echo * %MONGO_PATH_SHELL%
echo *
echo * Работа не возможна.
echo *
echo ***
pause
exit

:ok
rem Устанавливаем переменные mongodb относительно BASE_PATH
set HOMEDRIVE=%~d0
set HOMEPATH=%~p0
set HOME=%~p0

rem Получаем путь на уровень выше
SET RUN_PATH=%~dp0
cd %RUN_PATH%

if {%RUN_PATH:~-1,1%}=={\} (set CONF_PATH=%RUN_PATH%Conf) else (set CONF_PATH=%RUN_PATH%\Conf)
if {%RUN_PATH:~-1,1%}=={\} (set DB_PATH=%RUN_PATH%DB) else (set DB_PATH=%RUN_PATH%\DB)

echo * Структура каталогов для работы относительно каталога запуска:
echo *
echo * HOMEDRIVE каталог:
echo * %HOMEDRIVE%
echo *
echo * HOMEPATH каталог:
echo * %HOMEPATH%
echo *
echo * .\Enviroment\Conf               - конфигурационные файлы запуска MongoDB
echo * %CONF_PATH%
echo *
echo * Mongo Shell path:
echo * %MONGO_PATH_SHELL%
echo *
echo * Каталог запуска скриптов:
echo * %cd%
echo *
echo * Запускаем скрипты...
echo *
echo ***
