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
        const { id, result, error, metrics } = msg;
        
        const pendingQuery = this.pendingQueries.get(id);
        if (!pendingQuery) {
            this.logger.warn(`ProcessPoolManager: Получен результат для неизвестного запроса: ${id}`);
            return;
        }
        
        clearTimeout(pendingQuery.timeout);
        this.pendingQueries.delete(id);
        
        if (error) {
            this.stats.failedQueries++;
            // Создаем Error объект из сериализованной ошибки
            const errorObj = new Error(error.message || 'Unknown error');
            errorObj.name = error.name || 'Error';
            errorObj.stack = error.stack || '';
            pendingQuery.reject(errorObj);
        } else {
            this.stats.successfulQueries++;
            // Десериализуем Date объекты в результате
            const deserializedResult = deserializeDates(result);
            pendingQuery.resolve({
                result: deserializedResult,
                metrics: metrics
            });
        }
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
     * Выполнение запроса через пул процессов или синхронно
     * @param {Object} queryData - Данные запроса
     * @param {Array} queryData.query - Агрегационный пайплайн MongoDB
     * @param {string} queryData.collectionName - Имя коллекции
     * @param {Object} queryData.options - Опции aggregate()
     * @returns {Promise<Array>} Результат запроса (массив документов)
     */
    async executeQuery(queryData) {
        const { query, collectionName, options } = queryData;
        
        if (this.isShuttingDown) {
            throw new Error('ProcessPoolManager is shutting down');
        }
        
        if (this._initializationPromise) {
            await this._initializationPromise;
        }

        if (this._initializationError) {
            throw this._initializationError;
        }

        return await this._executeQueryViaPool(query, collectionName, options);
    }
    
    /**
     * Выполнение запроса через пул процессов
     * @param {Array} query - Агрегационный пайплайн
     * @param {string} collectionName - Имя коллекции
     * @param {Object} options - Опции aggregate()
     * @returns {Promise<Array>} Результат запроса
     */
    async _executeQueryViaPool(query, collectionName, options) {
        // Проверяем наличие доступных worker'ов
        const availableWorkers = this.workers.filter(w => w.isReady);
        if (availableWorkers.length === 0) {
            throw new Error(`No available workers in process pool (total: ${this.workers.length}, ready: ${availableWorkers.length})`);
        }
        
        const worker = this._getNextWorker();
        if (!worker || !worker.isReady) {
            // Если текущий worker не готов, попробуем найти другой
            const readyWorker = this.workers.find(w => w.isReady);
            if (!readyWorker) {
                throw new Error('No available workers in process pool');
            }
            this.currentWorkerIndex = this.workers.indexOf(readyWorker);
            return await this._executeQueryViaPool(query, collectionName, options);
        }
        
        const queryId = `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        this.stats.totalQueries++;
        worker.queryCount++;
        
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pendingQueries.delete(queryId);
                this.stats.failedQueries++;
                worker.errorCount++;
                reject(new Error(`Query timeout after 60000ms: ${queryId}`));
            }, 60000); // 60 секунд таймаут
            
            this.pendingQueries.set(queryId, {
                resolve,
                reject,
                timeout
            });
            
            worker.process.send({
                type: 'QUERY',
                id: queryId,
                query: query,
                collectionName: collectionName,
                options: options || {}
            });
        }).then(({ result }) => {
            return result;
        });
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
     * Получение статистики пула процессов
     * @returns {Object} Статистика пула
     */
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

