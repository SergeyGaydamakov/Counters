/**
 * ProcessPoolManager - Менеджер пула процессов для выполнения MongoDB запросов
 * 
 * Управляет пулом worker-процессов для параллельного выполнения агрегационных запросов к MongoDB.
 * При parallelsRequestProcesses === 1 использует синхронное выполнение без создания пула.
 */

const { fork } = require('child_process');
const path = require('path');
const Logger = require('../utils/logger');

/**
 * Проверяет, является ли строка ISO 8601 датой
 * @param {string} str - Строка для проверки
 * @returns {boolean} true, если строка является ISO 8601 датой
 */
function isISODateString(str) {
    if (typeof str !== 'string') {
        return false;
    }
    // ISO 8601 формат: YYYY-MM-DDTHH:mm:ss.sssZ или YYYY-MM-DDTHH:mm:ssZ
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/.test(str);
}

/**
 * Десериализует Date объекты из ISO строк в результатах запросов
 * Рекурсивно обходит объекты и массивы, преобразуя ISO строки обратно в Date объекты
 * @param {*} obj - Объект для десериализации
 * @returns {*} Объект с преобразованными Date объектами
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

function createErrorFromSerialized(serializedError) {
    if (!serializedError) {
        return null;
    }

    if (serializedError instanceof Error) {
        return serializedError;
    }

    const error = new Error(serializedError.message || 'Unknown error');
    error.name = serializedError.name || 'Error';
    if (serializedError.stack) {
        error.stack = serializedError.stack;
    }
    return error;
}

/**
 * Класс ProcessPoolManager
 * Управляет пулом worker-процессов для выполнения MongoDB запросов
 */
class ProcessPoolManager {
    /**
     * Конструктор ProcessPoolManager
     * @param {Object} options - Опции для создания пула
     * @param {number} options.workerCount - Количество worker-процессов
     * @param {string} options.connectionString - Строка подключения MongoDB
     * @param {string} options.databaseName - Имя базы данных
     * @param {Object} options.databaseOptions - Опции подключения MongoDB
     */
    constructor(options) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

        this.workerCount = options.workerCount || 1;
        if (this.workerCount <= 1) {
            throw new Error('ProcessPoolManager requires workerCount > 1');
        }
        this.connectionString = options.connectionString;
        this.databaseName = options.databaseName;
        this.databaseOptions = options.databaseOptions || {};
        
        // Таймаут инициализации worker-процесса (мс). Позволяет ускорить тесты
        this.workerInitTimeoutMs = options.workerInitTimeoutMs || 10000;
        
        // Для пула процессов
        this.workers = [];
        this.currentWorkerIndex = 0;
        this.pendingQueries = new Map(); // Map<queryId, {resolve, reject, timeout}>
        this.stats = {
            totalQueries: 0,
            successfulQueries: 0,
            failedQueries: 0,
            activeWorkers: 0,
            restartedWorkers: 0
        };
        
        this.isShuttingDown = false;
        this._initializationPromise = null;
        this._initializationError = null;
        
        // Инициализация
        this._initializationPromise = this._initializeProcessPool().catch((error) => {
            this._initializationError = error;
            return false;
        });
    }
    
    /**
     * Инициализация пула процессов
     * Создает процессы параллельно, не блокируя на ошибках отдельных процессов
     */
    async _initializeProcessPool() {
        this.logger.debug(`ProcessPoolManager: Инициализация пула из ${this.workerCount} процессов`);
        
        // Создаем процессы параллельно
        const createPromises = [];
        for (let i = 0; i < this.workerCount; i++) {
            createPromises.push(
                this._createWorker(i).catch(error => {
                    this.logger.error(`ProcessPoolManager: Не удалось создать worker ${i}: ${error.message}`);
                    // Продолжаем работу даже если некоторые worker'ы не созданы
                    return null;
                })
            );
        }
        
        // Ждем завершения всех попыток создания
        await Promise.allSettled(createPromises);
        
        this.logger.debug(`ProcessPoolManager: ✓ Пул из ${this.workers.length}/${this.workerCount} процессов инициализирован`);
        
        if (this.workers.length === 0) {
            this.logger.warn('ProcessPoolManager: ⚠ Ни один worker процесс не был успешно инициализирован');
            throw new Error('ProcessPoolManager initialization failed');
        }

        return true;
    }
    
    /**
     * Создает новый worker процесс
     * @param {number} index - Индекс worker процесса
     * @returns {Promise<void>}
     */
    async _createWorker(index) {
        const workerPath = path.join(__dirname, 'queryWorker.js');
        
        return new Promise((resolve, reject) => {
            const worker = fork(workerPath, [], {
                silent: true,
                stdio: ['pipe', 'pipe', 'pipe', 'ipc']
            });
            
            const timeout = setTimeout(() => {
                if (!isResolved) {
                    isResolved = true;
                    worker.removeAllListeners();
                    try {
                        if (!worker.killed) {
                            worker.kill('SIGKILL');
                        }
                    } catch (error) {
                        // Игнорируем ошибки
                    }
                    reject(new Error(`Timeout waiting for worker ${index} to initialize`));
                }
            }, this.workerInitTimeoutMs);
            
            let isResolved = false;
            
            const messageHandler = (msg) => {
                if (isResolved) return;
                
                if (msg && msg.type === 'RESULT') {
                    this._handleWorkerResult(msg);
                } else if (msg && msg.type === 'RESULT_BATCH') {
                    this._handleWorkerBatchResult(msg);
                } else if (msg && msg.type === 'READY') {
                    if (!isResolved) {
                        isResolved = true;
                        clearTimeout(timeout);
                        worker.removeListener('message', messageHandler);
                        worker.removeListener('exit', exitHandler);
                        worker.removeListener('error', errorHandler);
                        
                        this.workers.push({
                            process: worker,
                            index: index,
                            isReady: true,
                            queryCount: 0,
                            errorCount: 0
                        });
                        this.stats.activeWorkers++;
                        this.logger.debug(`ProcessPoolManager: ✓ Worker ${index} готов`);
                        
                        // Устанавливаем обработчики для работающего worker
                        const workerExitHandler = (code, signal) => {
                            if (!this.isShuttingDown && code !== 0 && code !== null) {
                                this.logger.warn(`ProcessPoolManager: Worker ${index} завершился с кодом ${code}, сигнал: ${signal}`);
                                // Находим worker и помечаем как неготовый
                                const w = this.workers.find(w => w.index === index);
                                if (w) {
                                    w.isReady = false;
                                    this.stats.activeWorkers = Math.max(0, this.stats.activeWorkers - 1);
                                }
                                this._restartWorker(index).catch(err => {
                                    this.logger.error(`ProcessPoolManager: Ошибка при перезапуске worker ${index}: ${err.message}`);
                                });
                            }
                        };
                        
                        worker.on('exit', workerExitHandler);
                        worker.on('message', (msg) => {
                            if (msg && msg.type === 'RESULT') {
                                this._handleWorkerResult(msg);
                            } else if (msg && msg.type === 'RESULT_BATCH') {
                                this._handleWorkerBatchResult(msg);
                            }
                        });
                        
                        resolve();
                    }
                } else if (msg && msg.type === 'ERROR') {
                    if (!isResolved) {
                        isResolved = true;
                        clearTimeout(timeout);
                        worker.removeAllListeners();
                        worker.kill();
                        reject(new Error(`Worker ${index} initialization failed: ${msg.message || 'Unknown error'}`));
                    }
                }
            };
            
            const errorHandler = (error) => {
                if (isResolved) return;
                isResolved = true;
                clearTimeout(timeout);
                worker.removeAllListeners();
                worker.kill();
                reject(error);
            };
            
            const exitHandler = (code, signal) => {
                if (isResolved) return;
                
                // Если процесс завершился до инициализации
                if (!this.isShuttingDown && (code !== 0 || signal)) {
                    isResolved = true;
                    clearTimeout(timeout);
                    worker.removeAllListeners();
                    reject(new Error(`Worker ${index} exited before initialization (code: ${code}, signal: ${signal})`));
                }
            };
            
            worker.on('message', messageHandler);
            worker.on('error', errorHandler);
            worker.on('exit', exitHandler);
            
            // Даем процессу немного времени на инициализацию перед отправкой INIT
            // Используем setImmediate для гарантии, что обработчики установлены
            setImmediate(() => {
                try {
                    if (worker.connected && !worker.killed && !isResolved) {
                        worker.send({
                            type: 'INIT',
                            connectionString: this.connectionString,
                            databaseName: this.databaseName,
                            databaseOptions: this.databaseOptions
                        });
                    }
                } catch (error) {
                    if (!isResolved) {
                        isResolved = true;
                        clearTimeout(timeout);
                        worker.removeAllListeners();
                        worker.kill();
                        reject(new Error(`Failed to send INIT to worker ${index}: ${error.message}`));
                    }
                }
            });
        });
    }
    
    /**
     * Перезапуск упавшего worker процесса
     * @param {number} index - Индекс worker процесса
     */
    async _restartWorker(index) {
        if (this.isShuttingDown) {
            return;
        }
        
        this.logger.debug(`ProcessPoolManager: Перезапуск worker ${index}`);
        this.stats.restartedWorkers++;
        
        // Удаляем старый worker из массива
        const workerIndex = this.workers.findIndex(w => w.index === index);
        if (workerIndex !== -1) {
            const oldWorker = this.workers[workerIndex];
            
            // Отменяем все ожидающие запросы для этого worker
            for (const [queryId, pendingQuery] of this.pendingQueries.entries()) {
                // Это не идеально, но мы не можем точно сопоставить queryId с worker
                // В реальной ситуации это не должно быть проблемой, так как результаты все равно придут
            }
            
            if (oldWorker.process) {
                try {
                    oldWorker.process.removeAllListeners();
                    if (!oldWorker.process.killed && oldWorker.process.connected) {
                        try {
                            oldWorker.process.send({ type: 'SHUTDOWN' });
                            // Даем процессу время на graceful shutdown
                            setTimeout(() => {
                                if (!oldWorker.process.killed) {
                                    oldWorker.process.kill();
                                }
                            }, 1000);
                        } catch (error) {
                            // Если канал закрыт, просто убиваем процесс
                            if (!oldWorker.process.killed) {
                                oldWorker.process.kill();
                            }
                        }
                    } else if (!oldWorker.process.killed) {
                        oldWorker.process.kill();
                    }
                } catch (error) {
                    // Игнорируем ошибки при завершении процесса
                }
            }
            this.workers.splice(workerIndex, 1);
            if (this.stats.activeWorkers > 0) {
                this.stats.activeWorkers--;
            }
        }
        
        // Создаем новый worker
        try {
            await this._createWorker(index);
        } catch (error) {
            this.logger.error(`ProcessPoolManager: Не удалось перезапустить worker ${index}: ${error.message}`);
            // Пытаемся еще раз через некоторое время
            setTimeout(() => {
                if (!this.isShuttingDown) {
                    this._restartWorker(index).catch(err => {
                        this.logger.error(`ProcessPoolManager: Повторная попытка перезапуска worker ${index} не удалась: ${err.message}`);
                    });
                }
            }, 5000);
        }
    }
    
    /**
     * Обработка результата от worker процесса
     * @param {Object} msg - Сообщение от worker {type: 'RESULT', id, result, error, metrics}
     */
    _handleWorkerResult(msg) {
        const { id, result, error, metrics } = msg || {};

        if (!id) {
            this.logger.warn('ProcessPoolManager: Получено сообщение результата без идентификатора запроса');
            return;
        }

        const pendingQuery = this.pendingQueries.get(id);
        if (!pendingQuery) {
            this.logger.warn(`ProcessPoolManager: Получен результат для неизвестного запроса: ${id}`);
            return;
        }

        if (pendingQuery.timeout) {
            clearTimeout(pendingQuery.timeout);
        }

        this.pendingQueries.delete(id);

        const normalizedMetrics = metrics && typeof metrics === 'object' ? metrics : {};
        const errorObj = error ? createErrorFromSerialized(error) : null;

        if (errorObj) {
            this.stats.failedQueries++;

            if (pendingQuery.mode === 'batch') {
                pendingQuery.resolve({
                    result: null,
                    error: errorObj,
                    metrics: normalizedMetrics
                });
            } else {
                pendingQuery.reject(errorObj);
            }
            return;
        }

        this.stats.successfulQueries++;
        const deserializedResult = deserializeDates(result);

        if (pendingQuery.mode === 'batch') {
            pendingQuery.resolve({
                result: deserializedResult,
                error: null,
                metrics: normalizedMetrics
            });
        } else {
            pendingQuery.resolve({
                result: deserializedResult,
                metrics: normalizedMetrics
            });
        }
    }

    _handleWorkerBatchResult(msg) {
        if (!msg || !Array.isArray(msg.results)) {
            this.logger.warn('ProcessPoolManager: Получен RESULT_BATCH с некорректной структурой');
            return;
        }

        msg.results.forEach((item) => {
            if (!item || !item.id) {
                this.logger.warn('ProcessPoolManager: В RESULT_BATCH найден элемент без id');
                return;
            }

            this._handleWorkerResult({
                id: item.id,
                result: item.result,
                error: item.error,
                metrics: item.metrics
            });
        });
    }
    
    /**
     * Получение следующего доступного worker процесса (round-robin)
     * @returns {Object|null} Worker объект или null, если нет доступных
     */
    _getNextWorker() {
        if (this.workers.length === 0) {
            return null;
        }
        
        // Round-robin распределение
        const worker = this.workers[this.currentWorkerIndex];
        this.currentWorkerIndex = (this.currentWorkerIndex + 1) % this.workers.length;
        
        return worker;
    }
    
    /**
     * Graceful shutdown пула процессов
     * @returns {Promise<void>}
     */
    async shutdown() {
        if (this.isShuttingDown) {
            return;
        }
        
        this.isShuttingDown = true;
        this.logger.debug('ProcessPoolManager: Начало graceful shutdown');
        
        if (this._initializationPromise) {
            try {
                await this._initializationPromise;
            } catch (error) {
                // Игнорируем ошибки и продолжаем завершение
            }
        }

        // Отменяем все ожидающие запросы
        for (const [queryId, pendingQuery] of this.pendingQueries.entries()) {
            clearTimeout(pendingQuery.timeout);
            pendingQuery.reject(new Error('ProcessPoolManager shutdown'));
            this.pendingQueries.delete(queryId);
        }
        
        // Закрываем все worker процессы
        const shutdownPromises = this.workers.map(async (worker) => {
            return new Promise((resolve) => {
                if (worker.process.killed || !worker.process.connected) {
                    resolve();
                    return;
                }
                
                const timeout = setTimeout(() => {
                    if (!worker.process.killed) {
                        try {
                            worker.process.kill('SIGKILL');
                        } catch (error) {
                            // Игнорируем ошибки
                        }
                    }
                    resolve();
                }, 5000);
                
                worker.process.once('exit', () => {
                    clearTimeout(timeout);
                    resolve();
                });
                
                try {
                    if (worker.process.connected) {
                        worker.process.send({ type: 'SHUTDOWN' });
                    } else {
                        clearTimeout(timeout);
                        resolve();
                    }
                } catch (error) {
                    // Если канал закрыт, просто завершаем процесс
                    clearTimeout(timeout);
                    if (!worker.process.killed) {
                        try {
                            worker.process.kill('SIGKILL');
                        } catch (killError) {
                            // Игнорируем ошибки
                        }
                    }
                    resolve();
                }
            });
        });
        
        await Promise.all(shutdownPromises);
        this.workers = [];
        this.stats.activeWorkers = 0;
        
        this.logger.debug('ProcessPoolManager: ✓ Graceful shutdown завершен');
    }
    
    /**
     * Получение списка свободных (готовых) воркеров
     * @returns {Array<Object>} Массив готовых воркеров
     */
    getReadyWorkers() {
        return this.workers.filter(w => w.isReady);
    }

    /**
     * Отправка батча запросов конкретному воркеру
     * @param {Object} worker - Воркер для выполнения батча
     * @param {Array<Object>} requests - Массив запросов
     * @param {Object} [options] - Опции выполнения
     * @param {number} [options.timeoutMs] - Таймаут для каждого запроса
     * @returns {Promise<Array>} Массив результатов запросов
     */
    async executeBatchOnWorker(worker, requests, options = {}) {
        if (this.isShuttingDown) {
            throw new Error('ProcessPoolManager is shutting down');
        }

        if (this._initializationPromise) {
            await this._initializationPromise;
        }

        if (this._initializationError) {
            throw this._initializationError;
        }

        if (!worker || !worker.isReady) {
            throw new Error('Worker is not ready');
        }

        if (!Array.isArray(requests) || requests.length === 0) {
            return [];
        }

        const normalizedRequests = requests.map((request, index) => {
            if (!request || !Array.isArray(request.query)) {
                throw new Error(`Invalid batch request at index ${index}: query must be an array`);
            }

            if (typeof request.collectionName !== 'string' || request.collectionName.trim() === '') {
                throw new Error(`Invalid batch request at index ${index}: collectionName must be a non-empty string`);
            }

            return {
                id: request.id || `batch_query_${Date.now()}_${Math.random().toString(36).slice(2, 9)}_${index}`,
                query: request.query,
                collectionName: request.collectionName,
                options: request.options || {}
            };
        });

        const timeoutMs = typeof options.timeoutMs === 'number' && options.timeoutMs > 0 ? options.timeoutMs : 60000;

        const perRequestPromises = normalizedRequests.map((request) => {
            return new Promise((resolve) => {
                const timeoutHandle = setTimeout(() => {
                    if (this.pendingQueries.has(request.id)) {
                        this.pendingQueries.delete(request.id);
                    }
                    worker.errorCount = (worker.errorCount || 0) + 1;
                    this.stats.failedQueries++;
                    resolve({
                        id: request.id,
                        result: null,
                        error: new Error(`Query timeout after ${timeoutMs}ms: ${request.id}`),
                        metrics: {
                            queryTime: timeoutMs,
                            querySize: 0,
                            resultSize: 0
                        }
                    });
                }, timeoutMs);

                this.pendingQueries.set(request.id, {
                    mode: 'batch',
                    timeout: timeoutHandle,
                    resolve: (payload) => {
                        clearTimeout(timeoutHandle);
                        const metrics = payload?.metrics && typeof payload.metrics === 'object' ? payload.metrics : {};
                        resolve({
                            id: request.id,
                            result: payload?.result ?? null,
                            error: payload?.error ?? null,
                            metrics
                        });
                    },
                    reject: (payload) => {
                        clearTimeout(timeoutHandle);
                        const errorObj = payload?.error instanceof Error
                            ? payload.error
                            : createErrorFromSerialized(payload?.error) || new Error('Unknown error');
                        const metrics = payload?.metrics && typeof payload.metrics === 'object' ? payload.metrics : {};
                        resolve({
                            id: request.id,
                            result: null,
                            error: errorObj,
                            metrics
                        });
                    }
                });
            });
        });

        const batchId = `batch_${Date.now()}_${Math.random().toString(36).slice(2, 7)}_${worker.index}`;
        this.stats.totalQueries += normalizedRequests.length;
        worker.queryCount += normalizedRequests.length;

        const payload = normalizedRequests.map((request) => ({
            id: request.id,
            query: request.query,
            collectionName: request.collectionName,
            options: request.options
        }));

        try {
            worker.process.send({
                type: 'QUERY_BATCH',
                batchId,
                requests: payload
            });
        } catch (error) {
            this.logger.error(`ProcessPoolManager: Не удалось отправить батч ${batchId} worker ${worker.index}: ${error.message}`);
            worker.errorCount = (worker.errorCount || 0) + normalizedRequests.length;
            normalizedRequests.forEach((request) => {
                const pendingQuery = this.pendingQueries.get(request.id);
                if (!pendingQuery) {
                    return;
                }

                this.pendingQueries.delete(request.id);
                if (pendingQuery.timeout) {
                    clearTimeout(pendingQuery.timeout);
                }

                this.stats.failedQueries++;
                pendingQuery.reject({
                    error: new Error(`Failed to send request ${request.id} to worker ${worker.index}: ${error.message}`),
                    metrics: {
                        queryTime: 0,
                        querySize: 0,
                        resultSize: 0
                    }
                });
            });
        }

        return Promise.all(perRequestPromises);
    }

    getStats() {
        return {
            useProcessPool: true,
            workerCount: this.workers.length,
            activeWorkers: this.stats.activeWorkers,
            totalQueries: this.stats.totalQueries,
            successfulQueries: this.stats.successfulQueries,
            failedQueries: this.stats.failedQueries,
            restartedWorkers: this.stats.restartedWorkers,
            pendingQueries: this.pendingQueries.size,
            workers: this.workers.map(w => ({
                index: w.index,
                isReady: w.isReady,
                queryCount: w.queryCount,
                errorCount: w.errorCount
            }))
        };
    }
}

module.exports = {
    ProcessPoolManager,
    deserializeDates,
    isISODateString
};

