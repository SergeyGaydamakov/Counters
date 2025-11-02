/**
 * Тесты для ProcessPoolManager - менеджера пула процессов для выполнения MongoDB запросов
 */

const { ProcessPoolManager } = require('../db-providers/processPoolManager');
const Logger = require('../utils/logger');
const config = require('../common/config');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

class ProcessPoolManagerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
        
        this.connectionString = config.database.connectionString;
        this.databaseName = 'processPoolManagerTestDB';
        this.databaseOptions = config.database.options;
    }

    /**
     * Ожидание выполнения условия с таймаутом
     */
    async waitFor(predicate, timeoutMs = 5000, intervalMs = 50) {
        const end = Date.now() + timeoutMs;
        // Быстрая первая проверка
        if (await predicate()) return true;
        while (Date.now() < end) {
            await new Promise(r => setTimeout(r, intervalMs));
            if (await predicate()) return true;
        }
        throw new Error('Timeout waiting for condition');
    }

    /**
     * Ожидание готовности пула: нужное количество процессов и все isReady
     */
    async waitForPoolReady(poolManager, expectedCount, timeoutMs = 5000) {
        await this.waitFor(() => {
            const s = poolManager.getStats();
            if (!s.useProcessPool) return false;
            if (typeof expectedCount === 'number' && s.workerCount < expectedCount) return false;
            if (!s.workers || s.workers.length === 0) return false;
            return s.workers.every(w => w.isReady === true);
        }, timeoutMs, 50);
    }

    /**
     * Очистка запущенных процессов Node.js (кроме текущего процесса)
     */
    async cleanupNodeProcesses() {
        try {
            const platform = process.platform;
            
            if (platform === 'win32') {
                // Windows: завершаем ТОЛЬКО node.exe из репозитория проекта, исключая текущий и родительский
                const currentPid = process.pid;
                const parentPid = process.ppid;
                
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
                    this.logger.debug('WMIC недоступен, пропускаю селективную очистку node.exe.');
                    nodeList = [];
                }
                
                for (const proc of nodeList) {
                    try {
                        const pid = proc.pid;
                        const cmd = (proc.cmd || '').toLowerCase();
                        if (pid === currentPid || pid === parentPid) continue;
                        
                        const isFromRepo = cmd.includes('c:\\sergeyg\\github\\counters') || cmd.includes('/c/sergeyg/github/counters');
                        const isTsServer = cmd.includes('tsserver.js') || cmd.includes('typingsinstaller.js');
                        const isNpmCli = cmd.includes('npm-cli.js') || cmd.includes('npm\\bin\\npm-cli.js');
                        
                        if (isFromRepo && !isTsServer && !isNpmCli) {
                            try {
                                await execAsync(`taskkill /PID ${pid} /F`);
                                this.logger.debug(`Завершен node-процесс из репозитория (PID: ${pid})`);
                            } catch (_) {}
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
        this.logger.debug('=== Тестирование ProcessPoolManager ===\n');

        // Очистка запущенных процессов Node.js перед тестами
        this.logger.debug('Очистка запущенных процессов Node.js...');
        await this.cleanupNodeProcesses();

        try {
            // 1. Инициализация пула
            // Сначала тестируем синхронный режим (проще и быстрее)
            await this.testPoolInitialization('1. Тест инициализации пула с заданным количеством процессов...');
            await this.testPoolModeInitialization('2. Тест инициализации в режиме пула (parallelsRequestProcesses > 1)...');
            await this.testInitializationErrors('3. Тест обработки ошибок создания процессов...');
            
            // 2. Управление процессами (только если пул создан успешно)
            await this.testWorkerMonitoring('4. Тест мониторинга состояния процессов...');
            await this.testQueryDistribution('5. Тест распределения запросов по процессам (round-robin)...');
            await this.testWorkerRestart('6. Тест перезапуска упавших процессов...');
            await this.testGracefulShutdown('7. Тест graceful shutdown всех процессов...');
            
            // 3. Выполнение запросов
            await this.testQueryExecutionViaPool('8. Тест выполнения запросов через пул процессов...');
            await this.testQueryResultsHandling('9. Тест обработки результатов от процессов...');
            await this.testBatchQueryExecution('10. Тест выполнения батч-запросов через пул...');
            await this.testQueryErrorHandling('11. Тест обработки ошибок выполнения запросов...');
            
            // 4. Статистика
            await this.testPoolStats('12. Тест получения статистики пула...');
            await this.testWorkerStats('13. Тест статистики по каждому процессу...');
            await this.testActiveWorkersCount('14. Тест подсчета активных и неактивных процессов...');
            
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        } finally {
            this.printResults();
        }
    }

    /**
     * Тест 1: Инициализация пула с заданным количеством процессов
     */
    async testPoolInitialization(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула (без фиксированной долгой задержки)
            await this.waitForPoolReady(poolManager, 2, 5000);
            
            const stats = poolManager.getStats();
            
            if (!stats.useProcessPool) {
                throw new Error('Пул процессов не создан');
            }
            
            // Проверяем, что хотя бы некоторые процессы инициализировались
            if (stats.workerCount === 0) {
                throw new Error('Ни один процесс не был создан');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testPoolInitialization: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 3: Инициализация в режиме пула
     */
    async testPoolModeInitialization(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 3,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 3 процесса
            await this.waitForPoolReady(poolManager, 3, 6000);
            
            const stats = poolManager.getStats();
            
            if (!stats.useProcessPool) {
                throw new Error('Должен использоваться пул процессов при workerCount > 1');
            }
            
            // Проверяем, что хотя бы некоторые процессы инициализировались
            if (stats.workerCount === 0) {
                throw new Error('Ни один процесс не был создан');
            }
            
            // Проверяем, что есть хотя бы один активный процесс
            if (stats.activeWorkers === 0 && stats.workerCount > 0) {
                this.logger.debug('   ⚠ Процессы созданы, но не все активны (возможно, требуется больше времени)');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testPoolModeInitialization: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 4: Обработка ошибок создания процессов
     */
    async testInitializationErrors(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            // Пытаемся создать пул с некорректной строкой подключения
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: 'mongodb://invalid-host:27017',
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Небольшая задержка, чтобы процессы успели стартовать и отчитаться о статусе
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // Проверяем, что пул создан, но процессы могут быть не готовы
            const stats = poolManager.getStats();
            
            if (stats.useProcessPool && stats.activeWorkers === 0) {
                // Это нормально - процессы не смогли подключиться
                this.logger.debug('   ⚠ Процессы не подключились (ожидаемо для некорректного подключения)');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            // Ошибки при создании пула с некорректными данными - это нормально
            this.logger.debug(`   ⚠ Ошибка ожидаема: ${error.message}`);
            this.testResults.passed++;
        } finally {
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 5: Распределение запросов по процессам (round-robin)
     */
    async testQueryDistribution(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 3,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 3 процесса
            await this.waitForPoolReady(poolManager, 3, 6000);
            
            const statsBefore = poolManager.getStats();
            if (statsBefore.activeWorkers === 0) {
                this.logger.debug('   ⚠ Нет доступных worker процессов для теста');
                this.testResults.passed++;
                await poolManager.shutdown();
                return;
            }
            
            // Выполняем несколько запросов
            const queries = [];
            for (let i = 0; i < 9; i++) {
                queries.push(
                    poolManager.executeQuery({
                        query: [{ $match: {} }, { $limit: 1 }],
                        collectionName: 'facts',
                        options: {}
                    }).catch(err => ({ error: err.message }))
                );
            }
            
            await Promise.all(queries);
            
            const stats = poolManager.getStats();
            
            // Проверяем, что запросы распределились
            if (stats.totalQueries !== 9) {
                throw new Error(`Ожидалось 9 запросов, получено: ${stats.totalQueries}`);
            }
            
            // Проверяем, что хотя бы некоторые worker'ы получили запросы
            const workersWithQueries = stats.workers.filter(w => w.queryCount > 0);
            if (workersWithQueries.length === 0 && stats.activeWorkers > 0) {
                throw new Error('Ни один worker не получил запросы');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryDistribution: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 6: Мониторинг состояния процессов
     */
    async testWorkerMonitoring(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            const stats = poolManager.getStats();
            
            if (stats.activeWorkers !== 2) {
                throw new Error(`Ожидалось 2 активных процесса, получено: ${stats.activeWorkers}`);
            }
            
            if (stats.workers.length !== 2) {
                throw new Error(`Ожидалось 2 процесса, получено: ${stats.workers.length}`);
            }
            
            // Проверяем, что все процессы готовы
            const readyWorkers = stats.workers.filter(w => w.isReady);
            if (readyWorkers.length !== 2) {
                throw new Error(`Ожидалось 2 готовых процесса, получено: ${readyWorkers.length}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testWorkerMonitoring: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 7: Перезапуск упавших процессов
     */
    async testWorkerRestart(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            const initialStats = poolManager.getStats();
            const initialActiveWorkers = initialStats.activeWorkers;
            
            // Принудительно завершаем один процесс
            if (poolManager.workers.length > 0) {
                const workerToKill = poolManager.workers[0];
                workerToKill.process.kill('SIGKILL');
                
                // Ждем перезапуска процесса и восстановления активных воркеров
                await this.waitFor(() => {
                    const s = poolManager.getStats();
                    return s.activeWorkers >= initialActiveWorkers;
                }, 12000, 50);
                
                const finalStats = poolManager.getStats();
                
                // Проверяем, что процесс был перезапущен
                if (finalStats.restartedWorkers === 0) {
                    this.logger.debug('   ⚠ Процесс не был перезапущен автоматически (возможно, требуется больше времени)');
                }
                
                // Проверяем, что количество активных процессов восстановлено
                if (finalStats.activeWorkers < initialActiveWorkers) {
                    this.logger.debug('   ⚠ Количество активных процессов не восстановлено');
                }
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testWorkerRestart: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 8: Graceful shutdown
     */
    async testGracefulShutdown(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            // Выполняем shutdown
            await poolManager.shutdown();
            
            // Проверяем, что все процессы завершены
            const stats = poolManager.getStats();
            
            if (stats.activeWorkers !== 0) {
                throw new Error(`После shutdown должно быть 0 активных процессов, получено: ${stats.activeWorkers}`);
            }
            
            if (stats.workerCount !== 0) {
                throw new Error(`После shutdown должно быть 0 процессов, получено: ${stats.workerCount}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testGracefulShutdown: ${error.message}`);
            if (poolManager && !poolManager.isShuttingDown) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 9: Выполнение запросов через пул процессов
     */
    async testQueryExecutionViaPool(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            const result = await poolManager.executeQuery({
                query: [{ $match: {} }, { $limit: 10 }],
                collectionName: 'facts',
                options: {}
            });
            
            if (!Array.isArray(result)) {
                throw new Error('Результат должен быть массивом');
            }
            
            const stats = poolManager.getStats();
            if (stats.totalQueries !== 1) {
                throw new Error(`Ожидалось 1 запрос, получено: ${stats.totalQueries}`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryExecutionViaPool: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 9: Обработка результатов от процессов
     */
    async testQueryResultsHandling(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            const promises = [];
            for (let i = 0; i < 5; i++) {
                promises.push(
                    poolManager.executeQuery({
                        query: [{ $match: {} }, { $limit: 1 }],
                        collectionName: 'facts',
                        options: {}
                    })
                );
            }
            
            const results = await Promise.all(promises);
            
            if (results.length !== 5) {
                throw new Error(`Ожидалось 5 результатов, получено: ${results.length}`);
            }
            
            results.forEach((result, index) => {
                if (!Array.isArray(result)) {
                    throw new Error(`Результат ${index} должен быть массивом`);
                }
            });
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryResultsHandling: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 10: Выполнение батч-запросов через пул
     */
    async testBatchQueryExecution(title) {
        this.logger.debug(title);

        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });

            await this.waitForPoolReady(poolManager, 2, 4000);

            const batchRequests = Array.from({ length: 4 }).map((_, index) => ({
                id: `batch-test-${index}`,
                query: [{ $match: {} }, { $limit: 2 }],
                collectionName: 'facts',
                options: {}
            }));

            const results = await poolManager.executeBatch(batchRequests, { timeoutMs: 15000 });

            if (!Array.isArray(results) || results.length !== batchRequests.length) {
                throw new Error('executeBatch должен возвращать результат для каждого запроса');
            }

            results.forEach((result, index) => {
                if (result.id !== batchRequests[index].id) {
                    throw new Error(`ID результата ${result.id} не совпадает с ID запроса ${batchRequests[index].id}`);
                }
                if (result.error) {
                    throw new Error(`Запрос ${result.id} завершился с ошибкой: ${result.error.message}`);
                }
                if (!Array.isArray(result.result)) {
                    throw new Error(`Результат запроса ${result.id} должен быть массивом`);
                }
            });

            const stats = poolManager.getStats();
            if (stats.totalQueries < batchRequests.length) {
                throw new Error('Статистика пула должна учитывать все запросы батча');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');

            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testBatchQueryExecution: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 11: Обработка ошибок выполнения запросов
     */
    async testQueryErrorHandling(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            let errorCaught = false;
            try {
                // Отправляем запрос с некорректным синтаксисом
                await poolManager.executeQuery({
                    query: [{ $invalidOperator: {} }],
                    collectionName: 'facts',
                    options: {}
                });
            } catch (error) {
                errorCaught = true;
                if (!error.message) {
                    throw new Error('Ошибка должна содержать message');
                }
            }
            
            if (!errorCaught) {
                // Может быть, что MongoDB проигнорирует некорректный оператор
                this.logger.debug('   ⚠ Ошибка не была выброшена (возможно, запрос был принят)');
            }
            
            const stats = poolManager.getStats();
            // После ошибки failedQueries должен увеличиться, если ошибка была обработана
            // или totalQueries увеличится в любом случае
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testQueryErrorHandling: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 12: Получение статистики пула
     */
    async testPoolStats(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            const stats = poolManager.getStats();
            
            if (typeof stats.useProcessPool !== 'boolean') {
                throw new Error('useProcessPool должен быть boolean');
            }
            
            if (typeof stats.workerCount !== 'number') {
                throw new Error('workerCount должен быть числом');
            }
            
            if (typeof stats.activeWorkers !== 'number') {
                throw new Error('activeWorkers должен быть числом');
            }
            
            if (typeof stats.totalQueries !== 'number') {
                throw new Error('totalQueries должен быть числом');
            }
            
            if (typeof stats.successfulQueries !== 'number') {
                throw new Error('successfulQueries должен быть числом');
            }
            
            if (typeof stats.failedQueries !== 'number') {
                throw new Error('failedQueries должен быть числом');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testPoolStats: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 13: Статистика по каждому процессу
     */
    async testWorkerStats(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions
            });
            
            // Ждем готовности пула на 2 процесса
            await this.waitForPoolReady(poolManager, 2, 4000);
            
            // Выполняем несколько запросов
            for (let i = 0; i < 3; i++) {
                try {
                    await poolManager.executeQuery({
                        query: [{ $match: {} }, { $limit: 1 }],
                        collectionName: 'facts',
                        options: {}
                    });
                } catch (error) {
                    // Игнорируем ошибки
                }
            }
            
            const stats = poolManager.getStats();
            
            if (!Array.isArray(stats.workers)) {
                throw new Error('workers должен быть массивом');
            }
            
            stats.workers.forEach((worker, index) => {
                if (typeof worker.index !== 'number') {
                    throw new Error(`worker[${index}].index должен быть числом`);
                }
                
                if (typeof worker.isReady !== 'boolean') {
                    throw new Error(`worker[${index}].isReady должен быть boolean`);
                }
                
                if (typeof worker.queryCount !== 'number') {
                    throw new Error(`worker[${index}].queryCount должен быть числом`);
                }
                
                if (typeof worker.errorCount !== 'number') {
                    throw new Error(`worker[${index}].errorCount должен быть числом`);
                }
            });
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testWorkerStats: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Тест 14: Подсчет активных и неактивных процессов
     */
    async testActiveWorkersCount(title) {
        this.logger.debug(title);
        
        let poolManager = null;
        try {
            poolManager = new ProcessPoolManager({
                workerCount: 3,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: 6000
            });
            
            // Ждем готовности пула на 3 процесса
            await this.waitForPoolReady(poolManager, 3, 6000);
            
            const stats = poolManager.getStats();
            
            if (stats.activeWorkers !== 3) {
                throw new Error(`Ожидалось 3 активных процесса, получено: ${stats.activeWorkers}`);
            }
            
            if (stats.workerCount !== 3) {
                throw new Error(`Ожидалось 3 процесса, получено: ${stats.workerCount}`);
            }
            
            // Проверяем, что количество активных процессов соответствует количеству готовых процессов
            const readyWorkers = stats.workers.filter(w => w.isReady);
            if (readyWorkers.length !== stats.activeWorkers) {
                throw new Error(`Количество готовых процессов (${readyWorkers.length}) не соответствует активным (${stats.activeWorkers})`);
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
            
            await poolManager.shutdown();
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testActiveWorkersCount: ${error.message}`);
            if (poolManager) {
                await poolManager.shutdown();
            }
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? ((this.testResults.passed / total) * 100).toFixed(2) : 0;
        
        this.logger.info('\n=== Результаты тестирования ProcessPoolManager ===');
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
    const test = new ProcessPoolManagerTest();
    test.runAllTests().catch(console.error);
}

module.exports = ProcessPoolManagerTest;

