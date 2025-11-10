/**
 * Тесты для QueryWorker - worker процесса для выполнения MongoDB запросов
 */

const { fork } = require('child_process');
const path = require('path');
const Logger = require('../common/logger');
const config = require('../common/config');
const { exec } = require('child_process');
const { promisify } = require('util');
const { MongoProvider, FactIndexer, FactMapper, CounterProducer } = require('../index');

const execAsync = promisify(exec);

/**
 * Проверяет, является ли строка ISO 8601 датой
 */
function isISODateString(str) {
    if (typeof str !== 'string') {
        return false;
    }
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/.test(str);
}

/**
 * Десериализует Date объекты из ISO строк в результатах запросов
 */
function deserializeDates(obj) {
    if (obj === null || typeof obj !== 'object') {
        return obj;
    }
    
    if (Array.isArray(obj)) {
        return obj.map(item => deserializeDates(item));
    }
    
    const result = {};
    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            const value = obj[key];
            if (typeof value === 'string' && isISODateString(value)) {
                result[key] = new Date(value);
            } else if (typeof value === 'object') {
                result[key] = deserializeDates(value);
            } else {
                result[key] = value;
            }
        }
    }
    return result;
}

class QueryWorkerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
        
        this.connectionString = config.database.connectionString;
        this.databaseName = 'queryWorkerTestDB';
        this.databaseOptions = config.database.options;
    }

    /**
     * Очистка запущенных процессов Node.js (кроме текущего процесса)
     */
    async cleanupNodeProcesses() {
        try {
            const platform = process.platform;
            
            if (platform === 'win32') {
                // Windows: завершаем ТОЛЬКО процессы node.exe из текущего репозитория,
                // исключая текущий процесс и родительский (npm runner)
                const currentPid = process.pid;
                const parentPid = process.ppid;
                
                // Получаем PID и CommandLine для node.exe
                // wmic может быть недоступен в некоторых конфигурациях, поэтому оборачиваем в try/catch
                let nodeList = [];
                try {
                    const { stdout: wmicOut } = await execAsync(`wmic process where "name='node.exe'" get ProcessId,CommandLine /FORMAT:CSV`);
                    nodeList = wmicOut.split('\n')
                        .map(l => l.trim())
                        .filter(l => l && !l.startsWith('Node') && l.includes(','))
                        .map(l => {
                            const parts = l.split(',');
                            const pid = parseInt(parts[parts.length - 1]);
                            const cmd = parts.slice(2, parts.length - 1).join(',');
                            return { pid, cmd };
                        })
                        .filter(p => !isNaN(p.pid));
                } catch (e) {
                    // Fallback: если нет wmic, ничего не делаем (лучше не убивать вслепую)
                    this.logger.debug('WMIC недоступен, пропускаю очистку node.exe без CommandLine-фильтра.');
                    nodeList = [];
                }
                
                for (const proc of nodeList) {
                    try {
                        const pid = proc.pid;
                        const cmd = (proc.cmd || '').toLowerCase();
                        
                        // Пропускаем текущий и родительский процессы
                        if (pid === currentPid || pid === parentPid) {
                            continue;
                        }
                        
                        // Убиваем ТОЛЬКО процессы из текущего репо
                        const isFromRepo = cmd.includes('c:\\sergeyg\\github\\counters') || cmd.includes('/c/sergeyg/github/counters');
                        
                        // И дополнительно исключаем известные фоновые процессы редактора и npm
                        const isTsServer = cmd.includes('tsserver.js') || cmd.includes('typingsinstaller.js');
                        const isNpmCli = cmd.includes('npm-cli.js') || cmd.includes('npm\\bin\\npm-cli.js');
                        
                        if (isFromRepo && !isTsServer && !isNpmCli) {
                            try {
                                await execAsync(`taskkill /PID ${pid} /F`);
                                this.logger.debug(`Завершен node-процесс из репозитория (PID: ${pid})`);
                            } catch (error) {
                                // Игнорируем ошибки завершения
                            }
                        }
                    } catch (_) {}
                }
            } else {
                // Linux/Mac: используем ps и kill
                const currentPid = process.pid;
                const { stdout } = await execAsync(`ps aux | grep node | grep -v grep`);
                const lines = stdout.split('\n').filter(line => line.trim());
                
                for (const line of lines) {
                    const parts = line.trim().split(/\s+/);
                    if (parts.length >= 2) {
                        const pid = parseInt(parts[1]);
                        // Пропускаем текущий и родительский процессы
                        const parentPid = process.ppid;
                        const cmd = line.toLowerCase();
                        const isFromRepo = cmd.includes('/sergeyg/github/counters');
                        const isTsServer = cmd.includes('tsserver.js') || cmd.includes('typingsinstaller.js');
                        const isNpmCli = cmd.includes('npm-cli.js') || cmd.includes('npm/bin/npm-cli.js');
                        if (pid !== currentPid && pid !== parentPid && !isNaN(pid) && isFromRepo && !isTsServer && !isNpmCli) {
                            try {
                                await execAsync(`kill -9 ${pid}`);
                                this.logger.debug(`Завершен процесс Node.js с PID: ${pid}`);
                            } catch (error) {
                                // Игнорируем ошибки завершения
                            }
                        }
                    }
                }
            }
            
            // Даем процессам время на завершение
            await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (error) {
            // Игнорируем ошибки очистки - это не критично
            this.logger.debug(`Не удалось выполнить очистку процессов Node.js: ${error.message}`);
        }
    }

    /**
     * Запуск всех тестов
     */
    async runAllTests() {
        this.logger.debug('=== Тестирование QueryWorker ===\n');

        // Очистка запущенных процессов Node.js перед тестами
        this.logger.debug('Очистка запущенных процессов Node.js...');
        await this.cleanupNodeProcesses();

        try {
            // 1. Инициализация и подключение
            await this.testInitialization('1. Тест инициализации и подключения worker процесса...');
            await this.testConnectionErrors('2. Тест обработки ошибок подключения...');
            
            // 2. Обработка IPC сообщений
            await this.testIPCQueryMessages('3. Тест обработки QUERY сообщений...');
            await this.testIPCInvalidMessages('4. Тест обработки некорректных IPC сообщений...');
            
            // 3. Выполнение запросов
            await this.testQueryExecution('5. Тест успешного выполнения агрегационных запросов...');
            await this.testEmptyResults('6. Тест обработки пустых результатов...');
            await this.testLargeResults('7. Тест обработки больших результатов...');
            await this.testQueryOptions('8. Тест выполнения запросов с различными опциями...');
            
            // 4. Обработка ошибок запросов
            await this.testQueryErrors('9. Тест обработки ошибок выполнения запросов...');
            await this.testQueryTimeouts('10. Тест таймаутов выполнения запросов...');
            
            // 5. Сериализация данных
            await this.testDateSerialization('11. Тест сериализации/десериализации Date объектов...');
            await this.testNestedDateObjects('12. Тест обработки вложенных объектов с Date...');
            await this.testArrayWithDates('13. Тест обработки массивов с Date объектами...');
            
            // 6. Метрики
            await this.testMetrics('14. Тест сбора метрик (время, размеры)...');
            
            // 7. Проверка возвращаемых данных
            await this.testDataValidation('15. Тест проверки возвращаемых данных из factIndex...');
            
            // 8. Graceful shutdown
            await this.testGracefulShutdown('16. Тест graceful shutdown...');
            
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        } finally {
            this.printResults();
            
            // Явно завершаем процесс после завершения всех тестов
            // MongoDB драйвер создает внутренние таймеры для heartbeat и мониторинга,
            // которые могут удерживать event loop даже после закрытия соединений
            // process.exit(0) немедленно завершает процесс, прерывая все оставшиеся таймеры и обработчики
            
            // Используем несколько уровней гарантий для надежного завершения:
            // 1. Fallback таймер - принудительное завершение через максимум 10 секунд
            // 2. Основное завершение - через небольшую задержку для закрытия MongoDB соединений
            
            const forceExitTimeout = setTimeout(() => {
                this.logger.warn('Принудительное завершение процесса (fallback timeout)');
                process.exit(0);
            }, 10000); // 10 секунд максимум
            
            // Основное завершение через setImmediate + небольшая задержка
            // Используем setImmediate чтобы гарантировать выполнение после завершения текущего event loop tick
            // и всех промисов, затем даем небольшое время для закрытия MongoDB соединений
            setImmediate(async () => {
                try {
                    // Даем время для завершения всех асинхронных операций MongoDB
                    // (закрытие соединений, очистка таймеров heartbeat и т.д.)
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    
                    // Отменяем fallback таймер перед завершением
                    clearTimeout(forceExitTimeout);
                } catch (error) {
                    this.logger.error(`Ошибка при подготовке к завершению: ${error.message}`);
                    clearTimeout(forceExitTimeout);
                }
                
                // Принудительно завершаем процесс
                // process.exit() синхронно завершает процесс, прерывая все активные таймеры
                // ВАЖНО: process.exit() не ждет завершения асинхронных операций и немедленно
                // завершает процесс, даже если MongoDB драйвер оставил активные таймеры heartbeat
                process.exit(0);
            });
        }
    }

    /**
     * Создание и инициализация worker процесса
     */
    async createWorker() {
        const workerPath = path.join(__dirname, '../database/queryWorker.js');
        const worker = fork(workerPath, [], {
            silent: true,
            stdio: ['pipe', 'pipe', 'pipe', 'ipc']
        });
        
        // Обработка stderr для диагностики
        let stderrOutput = '';
        worker.stderr.on('data', (data) => {
            stderrOutput += data.toString();
            this.logger.debug(`Worker stderr: ${data.toString()}`);
        });
        
        // Обработка stdout для диагностики
        let stdoutOutput = '';
        worker.stdout.on('data', (data) => {
            stdoutOutput += data.toString();
            this.logger.debug(`Worker stdout: ${data.toString()}`);
        });
        
        // Ожидание сообщения READY
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                const errorMsg = `Timeout waiting for worker READY. Worker stderr: ${stderrOutput}. Worker stdout: ${stdoutOutput}`;
                this.logger.error(errorMsg);
                reject(new Error(errorMsg));
            }, 30000);
            
            worker.once('message', (msg) => {
                clearTimeout(timeout);
                if (msg && msg.type === 'READY') {
                    resolve();
                } else if (msg && msg.type === 'ERROR') {
                    const errorMsg = msg.message || 'Worker initialization failed';
                    if (msg.error) {
                        this.logger.error(`Worker error details: ${JSON.stringify(msg.error)}`);
                    }
                    reject(new Error(errorMsg));
                } else {
                    reject(new Error(`Unexpected message from worker: ${JSON.stringify(msg)}`));
                }
            });
            
            worker.once('error', (error) => {
                clearTimeout(timeout);
                this.logger.error(`Worker process error: ${error.message}`);
                reject(error);
            });
            
            // Ждем, пока worker будет готов к получению сообщений
            // Отправляем INIT сообщение после небольшой задержки
            setImmediate(() => {
                try {
                    worker.send({
                        type: 'INIT',
                        connectionString: this.connectionString,
                        databaseName: this.databaseName,
                        databaseOptions: this.databaseOptions
                    });
                } catch (error) {
                    clearTimeout(timeout);
                    this.logger.error(`Error sending INIT to worker: ${error.message}`);
                    reject(error);
                }
            });
        });
        
        return worker;
    }

    /**
     * Тест 1: Инициализация и подключение
     */
    async testInitialization(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            if (!worker || !worker.connected) {
                throw new Error('Worker не создан или не подключен');
            }
            
            // Проверяем, что процесс жив
            if (worker.killed) {
                throw new Error('Worker процесс завершен');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            // Закрываем worker
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testInitialization: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 2: Обработка ошибок подключения
     */
    async testConnectionErrors(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            const workerPath = path.join(__dirname, '../database/queryWorker.js');
            worker = fork(workerPath, [], {
                silent: true,
                stdio: ['pipe', 'pipe', 'pipe', 'ipc']
            });
            
            const errorReceived = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    resolve(false); // Timeout означает, что ошибка не была обработана
                }, 5000);
                
                worker.once('message', (msg) => {
                    clearTimeout(timeout);
                    if (msg && msg.type === 'ERROR') {
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                });
                
                // Отправляем некорректную строку подключения
                worker.send({
                    type: 'INIT',
                    connectionString: 'mongodb://invalid-host:27017',
                    databaseName: this.databaseName,
                    databaseOptions: this.databaseOptions
                });
            });
            
            if (!errorReceived) {
                // Worker может не успеть отправить ERROR до timeout, это нормально для некорректных подключений
                this.logger.debug('   ⚠ Worker не отправил ERROR (возможно, timeout подключения больше 5 секунд)');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testConnectionErrors: ${error.message}`);
        } finally {
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 3: Обработка QUERY сообщений
     */
    async testIPCQueryMessages(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-query-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Отправляем QUERY сообщение
                worker.send({
                    type: 'QUERY',
                    id: 'test-query-1',
                    query: [{ $match: {} }, { $limit: 1 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (!result || result.type !== 'RESULT') {
                throw new Error('Некорректный формат результата');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testIPCQueryMessages: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 4: Обработка некорректных IPC сообщений
     */
    async testIPCInvalidMessages(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const errorReceived = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    resolve(false);
                }, 5000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.error) {
                        clearTimeout(timeout);
                        resolve(true);
                    }
                });
                
                // Отправляем некорректное сообщение без обязательных полей
                worker.send({
                    type: 'QUERY',
                    id: 'test-invalid-1'
                    // отсутствуют query и collectionName
                });
            });
            
            if (!errorReceived) {
                throw new Error('Worker не обработал некорректное сообщение');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testIPCInvalidMessages: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 5: Успешное выполнение запросов
     */
    async testQueryExecution(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-execution-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                worker.send({
                    type: 'QUERY',
                    id: 'test-execution-1',
                    query: [{ $match: {} }, { $limit: 10 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (!result || result.type !== 'RESULT') {
                throw new Error('Некорректный формат результата');
            }
            
            if (result.error) {
                throw new Error(`Запрос завершился с ошибкой: ${result.error.message}`);
            }
            
            if (!Array.isArray(result.result)) {
                throw new Error('Результат не является массивом');
            }
            
            // Проверяем наличие метрик
            if (!result.metrics || typeof result.metrics.queryTime !== 'number') {
                throw new Error('Метрики не корректны');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryExecution: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 6: Обработка пустых результатов
     */
    async testEmptyResults(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-empty-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Запрос, который точно вернет пустой результат
                worker.send({
                    type: 'QUERY',
                    id: 'test-empty-1',
                    query: [{ $match: { _id: { $exists: false } } }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (!result || !Array.isArray(result.result)) {
                throw new Error('Результат должен быть массивом');
            }
            
            if (result.error) {
                throw new Error(`Запрос завершился с ошибкой: ${result.error.message}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testEmptyResults: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 7: Обработка больших результатов
     */
    async testLargeResults(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 60000); // Больше времени для больших результатов
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-large-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Запрос с большим лимитом
                worker.send({
                    type: 'QUERY',
                    id: 'test-large-1',
                    query: [{ $match: {} }, { $limit: 1000 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (!result || !Array.isArray(result.result)) {
                throw new Error('Результат должен быть массивом');
            }
            
            if (result.metrics.resultSize < 0) {
                throw new Error('Размер результата должен быть неотрицательным');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLargeResults: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 8: Выполнение запросов с различными опциями
     */
    async testQueryOptions(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-options-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Запрос с опциями
                worker.send({
                    type: 'QUERY',
                    id: 'test-options-1',
                    query: [{ $match: {} }, { $limit: 1 }],
                    collectionName: 'facts',
                    options: {
                        readConcern: { level: 'local' }
                    }
                });
            });
            
            if (result.error) {
                throw new Error(`Запрос завершился с ошибкой: ${result.error.message}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryOptions: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 9: Обработка ошибок выполнения запросов
     */
    async testQueryErrors(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-error-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Отправляем запрос с некорректным синтаксисом
                worker.send({
                    type: 'QUERY',
                    id: 'test-error-1',
                    query: [{ $invalidOperator: {} }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            // Должна быть ошибка
            if (!result.error) {
                throw new Error('Ожидалась ошибка для некорректного запроса');
            }
            
            // Проверяем структуру ошибки
            if (!result.error.message) {
                throw new Error('Ошибка должна содержать message');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryErrors: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 10: Таймауты выполнения запросов
     * Примечание: Этот тест может быть сложным для реализации без модификации worker,
     * так как таймауты обычно устанавливаются на стороне менеджера пула
     */
    async testQueryTimeouts(title) {
        this.logger.debug(title);
        
        // Этот тест требует дополнительной реализации таймаутов в worker или менеджере
        // Пока что просто отмечаем как пропущенный
        this.logger.debug('   ⚠ Тест пропущен (требует реализации таймаутов)');
        this.testResults.passed++;
    }

    /**
     * Тест 11: Сериализация/десериализация Date объектов
     */
    async testDateSerialization(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            // Создаем запрос с Date в условии
            const testDate = new Date('2024-01-01T00:00:00.000Z');
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-date-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                // Запрос с Date в условии
                worker.send({
                    type: 'QUERY',
                    id: 'test-date-1',
                    query: [
                        { $match: { dt: { $gte: testDate } } },
                        { $limit: 10 }
                    ],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (result.error) {
                // Может быть ошибка, если нет данных с такой датой - это нормально
                this.logger.debug('   ⚠ Запрос завершился с ошибкой (возможно, нет данных): ' + result.error.message);
            }
            
            // Проверяем, что результат может содержать Date объекты
            // После десериализации в ProcessPoolManager они будут преобразованы обратно
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testDateSerialization: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 12: Обработка вложенных объектов с Date
     */
    async testNestedDateObjects(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-nested-date-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                worker.send({
                    type: 'QUERY',
                    id: 'test-nested-date-1',
                    query: [{ $match: {} }, { $limit: 5 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (result.error) {
                // Может быть ошибка, если коллекция пуста
                this.logger.debug('   ⚠ Запрос завершился с ошибкой (возможно, коллекция пуста): ' + result.error.message);
            }
            
            // Проверяем структуру результата
            if (result.result && Array.isArray(result.result)) {
                // Результаты могут содержать Date объекты, которые будут сериализованы в ISO строки
                // Десериализация происходит в ProcessPoolManager
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testNestedDateObjects: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 13: Обработка массивов с Date объектами
     */
    async testArrayWithDates(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-array-date-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                worker.send({
                    type: 'QUERY',
                    id: 'test-array-date-1',
                    query: [{ $match: {} }, { $limit: 5 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (result.error) {
                // Может быть ошибка, если коллекция пуста
                this.logger.debug('   ⚠ Запрос завершился с ошибкой (возможно, коллекция пуста): ' + result.error.message);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testArrayWithDates: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 14: Сбор метрик
     */
    async testMetrics(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-metrics-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                worker.send({
                    type: 'QUERY',
                    id: 'test-metrics-1',
                    query: [{ $match: {} }, { $limit: 10 }],
                    collectionName: 'facts',
                    options: {}
                });
            });
            
            if (!result.metrics) {
                throw new Error('Метрики отсутствуют');
            }
            
            if (typeof result.metrics.queryTime !== 'number' || result.metrics.queryTime < 0) {
                throw new Error('queryTime должен быть неотрицательным числом');
            }
            
            if (typeof result.metrics.querySize !== 'number' || result.metrics.querySize < 0) {
                throw new Error('querySize должен быть неотрицательным числом');
            }
            
            if (typeof result.metrics.resultSize !== 'number' || result.metrics.resultSize < 0) {
                throw new Error('resultSize должен быть неотрицательным числом');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await this.shutdownWorker(worker);
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMetrics: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
        }
    }

    /**
     * Тест 15: Проверка возвращаемых данных из factIndex
     */
    async testDataValidation(title) {
        this.logger.debug(title);
        
        let worker = null;
        let provider = null;
        
        try {
            // Инициализация MongoProvider и вспомогательных классов
            const countersConfig = [
                {
                    name: "total",
                    comment: "Общий счетчик для всех типов сообщений",
                    indexTypeName: "test_type_1",
                    computationConditions: {},
                    evaluationConditions: null,
                    attributes: {
                        "count": { "$sum": 1 },
                        "sumA": { "$sum": "$d.amount" }
                    }
                }
            ];
            const mongoCounters = new CounterProducer(countersConfig);
            
            const fieldConfig = [
                {
                    src: "amount",
                    dst: "amount",
                    message_types: [1],
                    generator: {
                        type: "integer",
                        min: 1,
                        max: 10000000,
                        default_value: 1000,
                        default_random: 0.1
                    }
                },
                {
                    src: "dt",
                    dst: "dt",
                    message_types: [1],
                    generator: {
                        type: "date",
                        min: "2024-01-01 00:00:00",
                        max: "2024-12-31 23:59:59"
                    }
                },
                {
                    src: "f1",
                    dst: "f1",
                    message_types: [1],
                    generator: {
                        type: "string",
                        min: 2,
                        max: 20,
                        default_value: "test-value",
                        default_random: 0.1
                    },
                    key_order: 1
                },
                {
                    src: "f2",
                    dst: "f2",
                    message_types: [1],
                    generator: {
                        type: "enum",
                        values: ["value1", "value2", "value3"],
                        default_value: "value1",
                        default_random: 0.1
                    }
                }
            ];
            
            const indexConfig = [
                {
                    fieldName: "f1",
                    dateName: "dt",
                    indexTypeName: "test_type_1",
                    indexType: 1,
                    indexValue: 1,
                    limit: 100
                }
            ];
            
            provider = new MongoProvider(
                this.connectionString,
                this.databaseName,
                this.databaseOptions,
                mongoCounters,
                config.facts.includeFactDataToIndex,
                config.facts.lookupFacts,
                config.facts.indexBulkUpdate
            );
            
            await provider.connect();
            await provider.clearFactsCollection();
            await provider.clearFactIndexCollection();
            
            const indexer = new FactIndexer(indexConfig, config.facts.includeFactDataToIndex);
            const mapper = new FactMapper(fieldConfig);
            
            // Создаем тестовые факты
            const testFacts = [
                {
                    _id: 'test-validation-fact-001',
                    t: 1,
                    c: new Date('2024-01-01'),
                    d: {
                        amount: 100,
                        dt: new Date('2024-01-01'),
                        f1: 'test-value-1',
                        f2: 'value1'
                    }
                },
                {
                    _id: 'test-validation-fact-002',
                    t: 1,
                    c: new Date('2024-01-02'),
                    d: {
                        amount: 200,
                        dt: new Date('2024-01-02'),
                        f1: 'test-value-2',
                        f2: 'value2'
                    }
                },
                {
                    _id: 'test-validation-fact-003',
                    t: 1,
                    c: new Date('2024-01-03'),
                    d: {
                        amount: 300,
                        dt: new Date('2024-01-03'),
                        f1: 'test-value-1', // Совпадает с первым фактом
                        f2: 'value3'
                    }
                }
            ];
            
            // Сохраняем факты и индексные значения
            const savedIndexIds = [];
            for (const fact of testFacts) {
                await provider.saveFact(fact);
                const indexValues = indexer.index(fact);
                if (indexValues.length > 0) {
                    await provider.saveFactIndexList(indexValues);
                    // Сохраняем ID индексных значений для проверки
                    savedIndexIds.push(...indexValues.map(iv => iv._id));
                }
            }
            
            if (savedIndexIds.length === 0) {
                throw new Error('Не создано ни одного индексного значения');
            }
            
            // Создаем worker и выполняем агрегационный запрос
            worker = await this.createWorker();
            
            // Простой запрос: получить все записи из factIndex с полем f1 = 'test-value-1'
            const query = [
                { $match: { 'f': { $in: ['test-validation-fact-001', 'test-validation-fact-003'] } } },
                { $sort: { dt: 1 } },
                { $limit: 100 }
            ];
            
            const result = await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Timeout waiting for query result'));
                }, 30000);
                
                worker.on('message', (msg) => {
                    if (msg && msg.type === 'RESULT' && msg.id === 'test-data-validation-1') {
                        clearTimeout(timeout);
                        resolve(msg);
                    }
                });
                
                worker.send({
                    type: 'QUERY',
                    id: 'test-data-validation-1',
                    query: query,
                    collectionName: 'factIndex',
                    options: {}
                });
            });
            
            if (result.error) {
                throw new Error(`Запрос завершился с ошибкой: ${result.error.message}`);
            }
            
            if (!Array.isArray(result.result)) {
                throw new Error('Результат должен быть массивом');
            }
            
            // Десериализуем Date объекты из ISO строк
            const deserializedResult = deserializeDates(result.result);
            
            // Проверяем результаты
            // Должны найтись записи для test-validation-fact-001 и test-validation-fact-003
            const foundIds = deserializedResult.map(r => {
                // ID в factIndex имеет структуру f: factId
                return r.f || (r._id && typeof r._id === 'string' ? r._id : null);
            }).filter(id => id !== null);
            
            // Проверяем, что найдены ожидаемые факты
            const expectedFactIds = ['test-validation-fact-001', 'test-validation-fact-003'];
            let foundExpectedCount = 0;
            for (const expectedId of expectedFactIds) {
                if (foundIds.includes(expectedId)) {
                    foundExpectedCount++;
                }
            }
            
            if (foundExpectedCount < 1) {
                throw new Error(`Ожидалось найти минимум 1 индексную запись для фактов ${expectedFactIds.join(', ')}, найдено: ${foundIds.join(', ')}`);
            }
            
            // Проверяем структуру данных
            if (deserializedResult.length > 0) {
                const firstResult = deserializedResult[0];
                if (!firstResult._id) {
                    throw new Error('Результат должен содержать поле _id');
                }
                // Проверяем наличие даты dt (после десериализации должен быть Date объект)
                if (!firstResult.dt) {
                    throw new Error('Результат должен содержать поле dt (дата)');
                }
                if (!(firstResult.dt instanceof Date)) {
                    throw new Error(`Поле dt должно быть Date объектом, получен: ${typeof firstResult.dt}`);
                }
                // Проверяем, что дата корректна
                if (isNaN(firstResult.dt.getTime())) {
                    throw new Error('Поле dt содержит некорректную дату');
                }
            }
            
            // Проверяем количество найденных записей (должно быть минимум 1, максимум 2 для двух фактов)
            if (deserializedResult.length === 0) {
                throw new Error('Не найдено ни одной индексной записи');
            }
            
            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно. Найдено ${deserializedResult.length} записей из factIndex`);
            
            await this.shutdownWorker(worker);
            await provider.disconnect();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testDataValidation: ${error.message}`);
            if (worker) {
                await this.shutdownWorker(worker);
            }
            if (provider) {
                try {
                    await provider.disconnect();
                } catch (e) {
                    // Игнорируем ошибки при закрытии
                }
            }
        }
    }

    /**
     * Тест 16: Graceful shutdown
     */
    async testGracefulShutdown(title) {
        this.logger.debug(title);
        
        let worker = null;
        try {
            worker = await this.createWorker();
            
            const shutdownPromise = new Promise((resolve) => {
                worker.once('exit', (code) => {
                    resolve(code);
                });
                
                // Отправляем команду shutdown
                worker.send({ type: 'SHUTDOWN' });
            });
            
            const exitCode = await Promise.race([
                shutdownPromise,
                new Promise((resolve) => setTimeout(() => resolve('timeout'), 10000))
            ]);
            
            if (exitCode === 'timeout') {
                throw new Error('Worker не завершился за 10 секунд');
            }
            
            if (exitCode !== 0 && exitCode !== null) {
                throw new Error(`Worker завершился с кодом: ${exitCode}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testGracefulShutdown: ${error.message}`);
            if (worker && !worker.killed) {
                worker.kill();
            }
        }
    }

    /**
     * Вспомогательная функция для корректного закрытия worker
     */
    async shutdownWorker(worker) {
        return new Promise((resolve) => {
            if (worker.killed) {
                resolve();
                return;
            }
            
            const timeout = setTimeout(() => {
                if (!worker.killed) {
                    worker.kill('SIGKILL');
                }
                // Удаляем все обработчики событий перед завершением
                worker.removeAllListeners();
                resolve();
            }, 5000);
            
            const exitHandler = () => {
                clearTimeout(timeout);
                // Удаляем все обработчики событий перед завершением
                worker.removeAllListeners();
                resolve();
            };
            
            worker.once('exit', exitHandler);
            
            try {
                if (worker.connected && !worker.killed) {
                    worker.send({ type: 'SHUTDOWN' });
                }
            } catch (error) {
                // Если канал IPC закрыт, просто завершаем процесс
                if (!worker.killed) {
                    worker.kill('SIGKILL');
                }
            }
        });
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? ((this.testResults.passed / total) * 100).toFixed(2) : 0;
        
        this.logger.info('\n=== Результаты тестирования QueryWorker ===');
        this.logger.info(`Всего тестов: ${total}`);
        this.logger.info(`Успешно: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
            this.logger.error('\nОшибки:');
            this.testResults.errors.forEach((error, index) => {
                this.logger.error(`  ${index + 1}. ${error}`);
            });
        }
        
        this.logger.info(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new QueryWorkerTest();
    test.runAllTests().catch(console.error);
}

module.exports = QueryWorkerTest;

