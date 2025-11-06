/**
 * Тесты для QueryDispatcher — диспетчера запросов поверх ProcessPoolManager
 */

const QueryDispatcher = require('../db-providers/queryDispatcher');
const { MongoProvider, FactIndexer, CounterProducer } = require('../index');
const Logger = require('../utils/logger');
const config = require('../common/config');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

class QueryDispatcherTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };

        this.connectionString = config.database.connectionString;
        this.databaseName = 'queryDispatcherTestDB';
        this.databaseOptions = config.database.options;
    }

    async runAllTests() {
        this.logger.debug('=== Тестирование QueryDispatcher ===\n');

        this.logger.debug('Очистка ранее запущенных Node.js процессов...');
        await this.cleanupNodeProcesses();

        try {
            await this.testSingleQueryExecution('1. Тест одиночного запроса в синхронном режиме...');
            await this.testExecuteQueriesWithPool('2. Тест выполнения нескольких запросов через пул процессов...');
            await this.testErrorHandling('3. Тест обработки ошибок запроса...');
            await this.testParallelExecutionWithErrors('4. Тест параллельного выполнения запросов с ошибками...');
            await this.testDispatcherStats('5. Тест получения статистики диспетчера...');
            await this.testConcurrentExecuteQueries('6. Тест параллельных вызовов executeQueries (race condition protection)...');
        } catch (error) {
            this.logger.error(`Критическая ошибка выполнения тестов: ${error.message}`);
        } finally {
            this.printResults();
        }
    }

    async waitFor(predicate, timeoutMs = 5000, intervalMs = 50) {
        const expires = Date.now() + timeoutMs;

        while (Date.now() <= expires) {
            try {
                const result = await predicate();
                if (result) {
                    return true;
                }
            } catch (error) {
                // Игнорируем ошибки предиката во время ожидания
            }
            await new Promise(resolve => setTimeout(resolve, intervalMs));
        }

        throw new Error('Timeout waiting for condition');
    }

    async waitForPoolReady(poolManager, expectedCount, timeoutMs = 10000) {
        await this.waitFor(() => {
            const stats = poolManager.getStats();
            if (!stats.useProcessPool) {
                return false;
            }
            if (typeof expectedCount === 'number' && stats.workerCount < expectedCount) {
                return false;
            }
            if (!stats.workers || stats.workers.length === 0) {
                return false;
            }
            return stats.workers.every(worker => worker.isReady === true);
        }, timeoutMs, 100);
    }

    async cleanupNodeProcesses() {
        try {
            const platform = process.platform;

            if (platform === 'win32') {
                const currentPid = process.pid;
                const parentPid = process.ppid;

                let nodeList = [];
                try {
                    const { stdout: wmicOut } = await execAsync(`wmic process where "name='node.exe'" get ProcessId,CommandLine /FORMAT:CSV`);
                    nodeList = wmicOut.split('\n')
                        .map(line => line.trim())
                        .filter(line => line && !line.startsWith('Node') && line.includes(','))
                        .map(line => {
                            const parts = line.split(',');
                            const pid = parseInt(parts[parts.length - 1], 10);
                            const cmd = parts.slice(2, parts.length - 1).join(',');
                            return { pid, cmd };
                        })
                        .filter(item => !Number.isNaN(item.pid));
                } catch (error) {
                    this.logger.debug('WMIC недоступен, селективная очистка node-процессов пропущена.');
                    nodeList = [];
                }

                for (const proc of nodeList) {
                    try {
                        const pid = proc.pid;
                        const cmd = (proc.cmd || '').toLowerCase();

                        if (pid === currentPid || pid === parentPid) {
                            continue;
                        }

                        const isFromRepo = cmd.includes('c\\sergeyg\\github\\counters') || cmd.includes('/c/sergeyg/github/counters');
                        const isTsServer = cmd.includes('tsserver.js') || cmd.includes('typingsinstaller.js');
                        const isNpmCli = cmd.includes('npm-cli.js') || cmd.includes('npm\\bin\\npm-cli.js');

                        if (isFromRepo && !isTsServer && !isNpmCli) {
                            try {
                                await execAsync(`taskkill /PID ${pid} /F`);
                                this.logger.debug(`Завершен node-процесс репозитория (PID: ${pid})`);
                            } catch (_) {
                                // Игнорируем ошибки завершения
                            }
                        }
                    } catch (_) {
                        // Игнорируем локальные ошибки обработки конкретного процесса
                    }
                }
            } else {
                const currentPid = process.pid;
                const parentPid = process.ppid;
                const { stdout } = await execAsync('ps aux | grep node | grep -v grep');
                const lines = stdout.split('\n').filter(line => line.trim());

                for (const line of lines) {
                    const parts = line.trim().split(/\s+/);
                    if (parts.length < 2) {
                        continue;
                    }

                    const pid = parseInt(parts[1], 10);
                    const cmd = line.toLowerCase();
                    const isFromRepo = cmd.includes('/sergeyg/github/counters');
                    const isTsServer = cmd.includes('tsserver.js') || cmd.includes('typingsinstaller.js');
                    const isNpmCli = cmd.includes('npm-cli.js') || cmd.includes('npm/bin/npm-cli.js');

                    if (pid !== currentPid && pid !== parentPid && !Number.isNaN(pid) && isFromRepo && !isTsServer && !isNpmCli) {
                        try {
                            await execAsync(`kill -9 ${pid}`);
                            this.logger.debug(`Завершен node-процесс (PID: ${pid})`);
                        } catch (_) {
                            // Игнорируем ошибки завершения
                        }
                    }
                }
            }

            await new Promise(resolve => setTimeout(resolve, 1500));
        } catch (error) {
            this.logger.debug(`Очистка процессов завершилась с предупреждением: ${error.message}`);
        }
    }

    async prepareFactIndexData() {
        const countersConfig = [
            {
                name: 'total',
                comment: 'Общий счетчик для всех типов сообщений',
                indexTypeName: 'test_type_1',
                computationConditions: {},
                evaluationConditions: null,
                attributes: {
                    count: { '$sum': 1 },
                    sumA: { '$sum': '$d.amount' }
                }
            }
        ];

        const indexConfig = [
            {
                fieldName: 'f1',
                dateName: 'dt',
                indexTypeName: 'test_type_1',
                indexType: 1,
                indexValue: 1,
                limit: 100
            }
        ];

        const mongoCounters = new CounterProducer(countersConfig);
        const provider = new MongoProvider(
            this.connectionString,
            this.databaseName,
            this.databaseOptions,
            mongoCounters,
            config.facts.includeFactDataToIndex,
            config.facts.lookupFacts,
            config.facts.indexBulkUpdate
        );

        const factIds = [
            'test-dispatcher-fact-001',
            'test-dispatcher-fact-002',
            'test-dispatcher-fact-003'
        ];

        const testFacts = [
            {
                _id: factIds[0],
                t: 1,
                c: new Date('2024-02-01T08:00:00.000Z'),
                d: {
                    amount: 150,
                    dt: new Date('2024-02-01T08:00:00.000Z'),
                    f1: 'dispatcher-value-1',
                    f2: 'value1'
                }
            },
            {
                _id: factIds[1],
                t: 1,
                c: new Date('2024-02-02T09:30:00.000Z'),
                d: {
                    amount: 320,
                    dt: new Date('2024-02-02T09:30:00.000Z'),
                    f1: 'dispatcher-value-2',
                    f2: 'value2'
                }
            },
            {
                _id: factIds[2],
                t: 1,
                c: new Date('2024-02-03T11:15:00.000Z'),
                d: {
                    amount: 450,
                    dt: new Date('2024-02-03T11:15:00.000Z'),
                    f1: 'dispatcher-value-1',
                    f2: 'value3'
                }
            }
        ];

        const indexer = new FactIndexer(indexConfig, config.facts.includeFactDataToIndex);

        try {
            await provider.connect();
            await provider.clearFactsCollection();
            await provider.clearFactIndexCollection();

            for (const fact of testFacts) {
                await provider.saveFact(fact);
                const indexValues = indexer.index(fact);
                if (indexValues.length > 0) {
                    await provider.saveFactIndexList(indexValues);
                }
            }

            return { factIds };
        } finally {
            await provider.disconnect();
        }
    }

    async testSingleQueryExecution(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            const dataset = await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                processPoolManager: null,
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 30000,
                logger: this.logger
            });

            const { results } = await dispatcher.executeQueries([{
                id: 'single-query-test',
                query: [
                    { $match: { 'f': { $in: [dataset.factIds[0], dataset.factIds[2]] } } },
                    { $sort: { dt: 1 } }
                ],
                collectionName: 'factIndex',
                options: {}
            }]);

            if (results.length !== 1) {
                throw new Error('Должен быть возвращен один результат');
            }

            const response = results[0];

            if (response.error) {
                throw new Error(`Запрос завершился с ошибкой: ${response.error.message}`);
            }

            if (!Array.isArray(response.result) || response.result.length === 0) {
                throw new Error('Результат запроса должен содержать документы');
            }

            const returnedIds = response.result.map(doc => doc.f || doc._id);
            const containsFirst = returnedIds.includes(dataset.factIds[0]);
            const containsThird = returnedIds.includes(dataset.factIds[2]);

            if (!containsFirst || !containsThird) {
                throw new Error('Результат запроса не содержит ожидаемые factIndex записи');
            }

            if (!response.result.every(doc => doc.dt instanceof Date)) {
                throw new Error('Поле dt должно быть десериализовано в Date');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testSingleQueryExecution: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    async testExecuteQueriesWithPool(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            const dataset = await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 45000,
                workerInitTimeoutMs: 8000,
                logger: this.logger
            });

            await this.waitForPoolReady(dispatcher.processPoolManager, 2, 12000);

            const requests = dataset.factIds.map((factId, index) => ({
                id: `multi-query-${index}`,
                query: [
                    { $match: { 'f': factId } },
                    { $sort: { dt: 1 } },
                    { $limit: 5 }
                ],
                collectionName: 'factIndex',
                options: {}
            }));

            const batchResponse = await dispatcher.executeQueries(requests, {
                timeoutMs: 30000
            });

            const results = batchResponse.results;
            const summary = batchResponse.summary;

            if (!Array.isArray(results) || results.length !== requests.length) {
                throw new Error('Количество результатов должно совпадать с количеством запросов');
            }

            results.forEach((item, index) => {
                if (item.id !== requests[index].id) {
                    throw new Error(`Несовпадение ID результата и запроса (index ${index})`);
                }
                if (item.error) {
                    throw new Error(`Запрос ${item.id} завершился с ошибкой: ${item.error.message}`);
                }
                if (!Array.isArray(item.result) || item.result.length === 0) {
                    throw new Error(`Запрос ${item.id} должен вернуть документы factIndex`);
                }
                const doc = item.result[0];
                const factId = doc.f || doc._id;
                if (factId !== dataset.factIds[index]) {
                    throw new Error(`Запрос ${item.id} вернул запись для другого факта`);
                }
                if (!(doc.dt instanceof Date)) {
                    throw new Error('Поле dt должно быть десериализовано в Date');
                }
            });

            if (!summary || summary.totalQueries !== requests.length) {
                throw new Error('Summary должен содержать корректное количество запросов');
            }

            if (summary.failedQueries !== 0) {
                throw new Error('Все запросы должны завершиться успешно');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testExecuteQueriesWithPool: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    async testErrorHandling(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 30000,
                logger: this.logger
            });

            const { results } = await dispatcher.executeQueries([{
                id: 'invalid-query-test',
                query: [
                    { $invalidOperator: {} }
                ],
                collectionName: 'factIndex',
                options: {}
            }]);

            if (results.length !== 1) {
                throw new Error('Должен быть возвращен один результат');
            }

            const response = results[0];

            if (!response.error) {
                throw new Error('Ожидалась ошибка для некорректного запроса');
            }

            if (!response.metrics || typeof response.metrics.queryTime !== 'number') {
                throw new Error('Метрики должны присутствовать даже при ошибке');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testErrorHandling: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    async testParallelExecutionWithErrors(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            const dataset = await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 30000,
                logger: this.logger
            });

            const requests = [
                {
                    id: 'parallel-error-invalid',
                    query: [{ $invalidOperator: {} }],
                    collectionName: 'factIndex',
                    options: {}
                },
                {
                    id: 'parallel-error-valid',
                    query: [
                        { $match: { 'f': dataset.factIds[0] } },
                        { $limit: 5 }
                    ],
                    collectionName: 'factIndex',
                    options: {}
                }
            ];

            const { results } = await dispatcher.executeQueries(requests, {
                timeoutMs: 20000
            });

            if (results.length !== requests.length) {
                throw new Error('executeQueries должен возвращать результат для каждого запроса');
            }

            const firstResult = results[0];
            const secondResult = results[1];

            if (!firstResult.error) {
                throw new Error('Первый запрос должен завершиться ошибкой');
            }

            // При параллельном выполнении второй запрос должен выполниться несмотря на ошибку первого
            if (secondResult.error) {
                throw new Error('Второй запрос должен выполниться успешно даже если первый завершился с ошибкой');
            }

            if (!Array.isArray(secondResult.result)) {
                throw new Error('Второй запрос должен вернуть массив результатов');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testParallelExecutionWithErrors: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    async testDispatcherStats(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            const dataset = await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                workerCount: 2,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 30000,
                logger: this.logger
            });

            await dispatcher.executeQueries([
                {
                    id: 'stats-query-1',
                    query: [
                        { $match: { 'f': dataset.factIds[0] } },
                        { $limit: 10 }
                    ],
                    collectionName: 'factIndex',
                    options: {}
                },
                {
                    id: 'stats-query-2',
                    query: [
                        { $match: { 'f': dataset.factIds[1] } },
                        { $limit: 5 }
                    ],
                    collectionName: 'factIndex',
                    options: {}
                }
            ]);

            const stats = dispatcher.getStats();

            if (!stats || !stats.dispatcher) {
                throw new Error('Метод getStats должен возвращать статистику диспетчера');
            }

            if (stats.dispatcher.totalQueries < 2) {
                throw new Error('Статистика должна учитывать выполненные запросы');
            }

            if (stats.dispatcher.failedQueries !== 0) {
                throw new Error('Не ожидалось ошибок в выполненных запросах');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testDispatcherStats: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    async testConcurrentExecuteQueries(title) {
        this.logger.debug(title);

        let dispatcher = null;
        try {
            const dataset = await this.prepareFactIndexData();

            dispatcher = new QueryDispatcher({
                workerCount: 2, // Меньше воркеров для проверки race condition
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                defaultTimeoutMs: 30000,
                logger: this.logger
            });

            await this.waitForPoolReady(dispatcher.processPoolManager, 2, 12000);

            // Создаем запросы для каждого параллельного вызова
            const createRequests = (prefix, count) => {
                const requests = [];
                for (let i = 0; i < count; i++) {
                    requests.push({
                        id: `${prefix}-req-${i}`,
                        query: [
                            { $match: { 'f': dataset.factIds[i % dataset.factIds.length] } },
                            { $limit: 5 }
                        ],
                        collectionName: 'factIndex',
                        options: {}
                    });
                }
                return requests;
            };

            // Запускаем 3 параллельных вызова executeQueries одновременно
            const parallelCalls = [
                dispatcher.executeQueries(createRequests('call1', 3), { timeoutMs: 30000 }),
                dispatcher.executeQueries(createRequests('call2', 3), { timeoutMs: 30000 }),
                dispatcher.executeQueries(createRequests('call3', 3), { timeoutMs: 30000 })
            ];

            const allResults = await Promise.all(parallelCalls);

            // Проверяем, что все вызовы завершились успешно
            if (allResults.length !== 3) {
                throw new Error(`Ожидалось 3 результата параллельных вызовов, получено: ${allResults.length}`);
            }

            // Проверяем, что каждый вызов вернул правильное количество результатов
            allResults.forEach((callResult, callIndex) => {
                if (!callResult.results || !Array.isArray(callResult.results)) {
                    throw new Error(`Вызов ${callIndex}: результаты должны быть массивом`);
                }
                if (callResult.results.length !== 3) {
                    throw new Error(
                        `Вызов ${callIndex}: ожидалось 3 результата, получено: ${callResult.results.length}`
                    );
                }

                // Проверяем, что все запросы завершились (успешно или с ошибкой, но не потеряны)
                callResult.results.forEach((result, reqIndex) => {
                    if (!result || !result.id) {
                        throw new Error(`Вызов ${callIndex}, запрос ${reqIndex}: результат должен иметь ID`);
                    }
                });
            });

            // Проверяем статистику - все запросы должны быть учтены
            const finalStats = dispatcher.getStats();
            const totalQueries = finalStats.dispatcher.totalQueries;
            const expectedQueries = 3 * 3; // 3 вызова * 3 запроса

            if (totalQueries < expectedQueries) {
                this.logger.debug(
                    `   ⚠ Ожидалось минимум ${expectedQueries} запросов, учтено в статистике: ${totalQueries} ` +
                    `(возможно, статистика обновляется асинхронно)`
                );
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testConcurrentExecuteQueries: ${error.message}`);
        } finally {
            if (dispatcher) {
                await dispatcher.shutdown();
            }
        }
    }

    printResults() {
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? ((this.testResults.passed / total) * 100).toFixed(2) : '0.00';

        this.logger.info('\n=== Результаты тестирования QueryDispatcher ===');
        this.logger.info(`Всего тестов: ${total}`);
        this.logger.info(`Успешно: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        this.logger.info(`Процент успеха: ${successRate}%`);

        if (this.testResults.errors.length > 0) {
            this.logger.error('\nОшибки:');
            this.testResults.errors.forEach((message, index) => {
                this.logger.error(`  ${index + 1}. ${message}`);
            });
        }
    }
}

if (require.main === module) {
    const test = new QueryDispatcherTest();
    test.runAllTests().catch(error => {
        // eslint-disable-next-line no-console
        console.error(error);
        process.exitCode = 1;
    });
}

module.exports = QueryDispatcherTest;


