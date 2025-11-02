/**
 * QueryDispatcher - Диспетчер запросов для ProcessPoolManager
 *
 * Предоставляет высокоуровневый интерфейс поверх ProcessPoolManager для
 * выполнения одиночных и множественных запросов, агрегации результатов,
 * обработки ошибок и сбора метрик.
 */

const Logger = require('../utils/logger');
const config = require('../common/config');
const { ProcessPoolManager } = require('./processPoolManager');

/**
 * Генерирует уникальный идентификатор запроса
 * @param {string} [prefix]
 * @returns {string}
 */
function generateRequestId(prefix = 'dispatch') {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

/**
 * Безопасная оценка размера объекта в байтах через JSON.stringify
 * @param {*} value
 * @returns {number}
 */
function estimateSize(value) {
    if (!config.logging?.debugMode) {
        return 0;
    }

    try {
        const json = JSON.stringify(value);
        return Buffer.byteLength(json || '');
    } catch (error) {
        return 0;
    }
}

class QueryDispatcher {
    /**
     * @param {Object} [options]
     * @param {ProcessPoolManager} [options.processPoolManager]
     * @param {number} [options.workerCount]
     * @param {string} [options.connectionString]
     * @param {string} [options.databaseName]
     * @param {Object} [options.databaseOptions]
     * @param {number} [options.defaultTimeoutMs]
     * @param {number} [options.maxConcurrency]
     * @param {Logger} [options.logger]
     * @param {number} [options.workerInitTimeoutMs]
     */
    constructor(options = {}) {
        this.logger = options.logger || Logger.fromEnv('LOG_LEVEL', 'INFO');

        this.connectionString = options.connectionString || config.database.connectionString;
        this.databaseName = options.databaseName || config.database.databaseName;
        this.databaseOptions = options.databaseOptions || config.database.options;

        // Параметры из конфигурации с возможностью переопределения через options
        this.defaultTimeoutMs = typeof options.defaultTimeoutMs === 'number'
            ? options.defaultTimeoutMs
            : (config.queryDispatcher?.timeoutMs || 60000);
        
        this.minWorkers = typeof options.minWorkers === 'number' && options.minWorkers > 0
            ? options.minWorkers
            : (config.queryDispatcher?.minWorkers || 2);
        
        this.maxWaitForWorkersMs = typeof options.maxWaitForWorkersMs === 'number' && options.maxWaitForWorkersMs > 0
            ? options.maxWaitForWorkersMs
            : (config.queryDispatcher?.maxWaitForWorkersMs || 500);
        
        this.checkIntervalMs = typeof options.checkIntervalMs === 'number' && options.checkIntervalMs > 0
            ? options.checkIntervalMs
            : (config.queryDispatcher?.checkIntervalMs || 20);

        this.maxConcurrency = typeof options.maxConcurrency === 'number' && options.maxConcurrency > 0
            ? options.maxConcurrency
            : null;

        this.processPoolManager = options.processPoolManager;
        this._ownsProcessPool = false;

        if (!this.processPoolManager) {
            const workerCount = options.workerCount || config.queryDispatcher?.workerCount || 1;
            this.processPoolManager = new ProcessPoolManager({
                workerCount,
                connectionString: this.connectionString,
                databaseName: this.databaseName,
                databaseOptions: this.databaseOptions,
                workerInitTimeoutMs: options.workerInitTimeoutMs
            });
            this._ownsProcessPool = true;
        }

        this.metrics = {
            totalQueries: 0,
            successfulQueries: 0,
            failedQueries: 0,
            totalQueryTime: 0,
            totalResultSize: 0,
            totalQuerySize: 0,
            lastError: null
        };
    }

    /**
     * Ожидание освобождения воркеров с таймаутом
     * В кластерном режиме каждый HTTP worker процесс имеет свои queryWorkers,
     * поэтому ожидание помогает, когда все воркеры текущего процесса заняты
     * @param {number} [minWorkers] - Минимальное количество воркеров (по умолчанию из config)
     * @param {number} [maxWaitMs] - Максимальное время ожидания в миллисекундах (по умолчанию из config)
     * @param {number} [checkIntervalMs] - Интервал проверки в миллисекундах (по умолчанию из config)
     * @returns {Promise<Array>} Массив готовых воркеров
     */
    async _waitForReadyWorkers(minWorkers = null, maxWaitMs = null, checkIntervalMs = null) {
        const effectiveMinWorkers = minWorkers !== null ? minWorkers : this.minWorkers;
        const effectiveMaxWaitMs = maxWaitMs !== null ? maxWaitMs : this.maxWaitForWorkersMs;
        const effectiveCheckIntervalMs = checkIntervalMs !== null ? checkIntervalMs : this.checkIntervalMs;
        const startTime = Date.now();
        
        while (Date.now() - startTime < effectiveMaxWaitMs) {
            const readyWorkers = this.processPoolManager?.getReadyWorkers() || [];
            
            // Проверяем не только количество готовых воркеров, но и наличие pending запросов
            // Если есть воркеры, но все заняты, ждем их освобождения
            const pendingQueries = this.processPoolManager?.pendingQueries?.size || 0;
            const totalWorkers = this.processPoolManager?.workers?.length || 0;
            
            // Если есть готовые воркеры и не все заняты (или вообще нет активных запросов)
            if (readyWorkers.length >= effectiveMinWorkers && (pendingQueries === 0 || readyWorkers.length > effectiveMinWorkers)) {
                return readyWorkers;
            }
            
            // Если все воркеры заняты, логируем для отладки
            if (readyWorkers.length > 0 && pendingQueries > 0 && readyWorkers.length <= effectiveMinWorkers) {
                const waitTime = Date.now() - startTime;
                if (waitTime > 1000 && waitTime % 2000 < effectiveCheckIntervalMs) {
                    // Логируем каждые ~2 секунды ожидания
                    this.logger.debug(
                        `QueryDispatcher: Ожидание освобождения воркеров (${readyWorkers.length} готовых, ` +
                        `${pendingQueries} активных запросов, ожидание ${Math.round(waitTime)}мс)`
                    );
                }
            }
            
            // Ждем перед следующей проверкой
            await new Promise(resolve => setTimeout(resolve, effectiveCheckIntervalMs));
        }
        
        // Если не удалось дождаться, возвращаем то, что есть
        const readyWorkers = this.processPoolManager?.getReadyWorkers() || [];
        if (readyWorkers.length === 0) {
            const stats = this.processPoolManager?.getStats();
            throw new Error(
                `No ready workers available after waiting ${effectiveMaxWaitMs}ms. ` +
                `Total workers: ${stats?.workerCount || 0}, Active: ${stats?.activeWorkers || 0}, ` +
                `Pending queries: ${stats?.pendingQueries || 0}`
            );
        }
        
        // Логируем предупреждение, если пришлось использовать меньше воркеров, чем запрошено
        if (readyWorkers.length < effectiveMinWorkers) {
            this.logger.warn(
                `QueryDispatcher: Используется ${readyWorkers.length} воркеров вместо запрошенных ${effectiveMinWorkers} ` +
                `(ожидание ${effectiveMaxWaitMs}мс завершено)`
            );
        }
        
        return readyWorkers;
    }

    /**
     * Выполнение массива запросов с возможностью ограничения параллелизма
     * Запросы распределяются по процессам в пуле через batch API
     * Поддерживает ожидание освобождения воркеров в кластерном режиме
     * @param {Array<Object>} requests
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @param {number} [options.maxConcurrency]
     * @param {number} [options.maxWaitForWorkersMs] - Максимальное время ожидания освобождения воркеров (по умолчанию 500мс)
     * @returns {Promise<{results: Array, summary: Object}>}
     */
    async executeQueries(requests, options = {}) {
        if (!Array.isArray(requests)) {
            throw new Error('requests must be an array');
        }

        if (requests.length === 0) {
            return { results: [], summary: this._buildSummary([]) };
        }

        const timeoutMs = this._resolveTimeout(options.timeoutMs);
        const maxWaitForWorkersMs = typeof options.maxWaitForWorkersMs === 'number' && options.maxWaitForWorkersMs > 0
            ? options.maxWaitForWorkersMs
            : this.maxWaitForWorkersMs;
        
        // Убеждаемся, что пул инициализирован
        if (this.processPoolManager?._initializationPromise) {
            await this.processPoolManager._initializationPromise;
        }
        
        // Получаем список свободных воркеров с ожиданием освобождения
        const requestedConcurrency = this._resolveConcurrency(options.maxConcurrency, requests.length);
        let readyWorkers;
        
        try {
            readyWorkers = await this._waitForReadyWorkers(
                Math.min(requestedConcurrency, this.minWorkers),
                maxWaitForWorkersMs,
                this.checkIntervalMs
            );
        } catch (error) {
            this.logger.warn(`QueryDispatcher: Не удалось получить готовых воркеров: ${error.message}`);
            throw error;
        }
        
        if (readyWorkers.length === 0) {
            throw new Error('No ready workers available');
        }
        
        const effectiveWorkerCount = Math.min(readyWorkers.length, requestedConcurrency);
        
        const preparedRequests = requests.map((request) => this._normalizeRequest(request));
        
        // Разделяем запросы на батчи для распределения по свободным воркерам
        const batches = [];
        const requestsPerBatch = Math.ceil(preparedRequests.length / effectiveWorkerCount);
        
        for (let i = 0; i < preparedRequests.length; i += requestsPerBatch) {
            batches.push(preparedRequests.slice(i, i + requestsPerBatch));
        }

        // Параллельная отправка батчей напрямую в свободные воркеры
        const batchPromises = batches.map(async (batch, batchIndex) => {
            // Выбираем воркер для батча (циклически по индексу)
            const worker = readyWorkers[batchIndex % effectiveWorkerCount];
            
            if (!worker || !worker.isReady) {
                // Если воркер стал недоступен, возвращаем ошибки для всех запросов
                return batch.map((prepared) => ({
                    id: prepared.id,
                    result: null,
                    error: new Error('Worker is not ready'),
                    metrics: {
                        queryTime: 0,
                        querySize: estimateSize(prepared.query),
                        resultSize: 0
                    }
                }));
            }

            try {
                // Отправляем батч напрямую в выбранный воркер
                const batchResults = await this.processPoolManager.executeBatchOnWorker(
                    worker,
                    batch.map((prepared) => ({
                        id: prepared.id,
                        query: prepared.query,
                        collectionName: prepared.collectionName,
                        options: prepared.options
                    })),
                    { timeoutMs }
                );
                return batchResults;
            } catch (error) {
                // При ошибке батча возвращаем ошибки для всех запросов в батче
                return batch.map((prepared) => ({
                    id: prepared.id,
                    result: null,
                    error: error instanceof Error ? error : new Error(error?.message || 'Batch execution failed'),
                    metrics: {
                        queryTime: timeoutMs || 0,
                        querySize: estimateSize(prepared.query),
                        resultSize: 0
                    }
                }));
            }
        });

        const batchResultsArray = await Promise.all(batchPromises);
        const allResults = batchResultsArray.flat();

        // Создаем Map для быстрого доступа к результатам по ID
        const resultsById = new Map();
        allResults.forEach((result) => {
            if (result && result.id) {
                resultsById.set(result.id, result);
            }
        });

        // Восстанавливаем порядок результатов согласно исходному порядку запросов и обновляем метрики
        const orderedResults = preparedRequests.map((prepared) => {
            const result = resultsById.get(prepared.id);
            let finalResult;
            
            if (result) {
                finalResult = result;
            } else {
                // Если результат не найден, создаем ошибку
                finalResult = {
                    id: prepared.id,
                    result: null,
                    error: new Error('Missing result from batch execution'),
                    metrics: {
                        queryTime: 0,
                        querySize: estimateSize(prepared.query),
                        resultSize: 0
                    }
                };
            }

            // Обновляем метрики диспетчера
            const metrics = finalResult.metrics || {};
            const querySize = typeof metrics.querySize === 'number'
                ? metrics.querySize
                : estimateSize(prepared.query);
            const resultSize = typeof metrics.resultSize === 'number'
                ? metrics.resultSize
                : estimateSize(finalResult.result ?? null);
            const queryTime = typeof metrics.queryTime === 'number'
                ? metrics.queryTime
                : 0;
            const error = finalResult.error instanceof Error
                ? finalResult.error
                : finalResult.error
                    ? new Error(finalResult.error.message || 'Unknown error')
                    : null;

            this._updateMetrics({
                success: !error,
                queryTime,
                resultSize,
                querySize,
                error
            });

            return finalResult;
        });

        const summary = this._buildSummary(orderedResults);
        return { results: orderedResults, summary };
    }

    /**
     * Получение статистики диспетчера и пула
     * @returns {Object}
     */
    getStats() {
        return {
            dispatcher: {
                totalQueries: this.metrics.totalQueries,
                successfulQueries: this.metrics.successfulQueries,
                failedQueries: this.metrics.failedQueries,
                totalQueryTime: this.metrics.totalQueryTime,
                totalResultSize: this.metrics.totalResultSize,
                totalQuerySize: this.metrics.totalQuerySize,
                lastError: this.metrics.lastError ? {
                    message: this.metrics.lastError.message,
                    name: this.metrics.lastError.name
                } : null
            },
            processPool: this.processPoolManager.getStats()
        };
    }


    /**
     * Graceful shutdown
     * @returns {Promise<void>}
     */
    async shutdown() {
        if (this._ownsProcessPool && this.processPoolManager) {
            await this.processPoolManager.shutdown();
        }
    }

    /**
     * Алиас для совместимости
     * @returns {Promise<void>}
     */
    async close() {
        await this.shutdown();
    }

    _normalizeRequest(request) {
        if (!request || typeof request !== 'object') {
            throw new Error('Query request must be an object');
        }

        const { query, collectionName, options, id } = request;

        if (!Array.isArray(query)) {
            throw new Error('query must be an array');
        }

        if (typeof collectionName !== 'string' || collectionName.trim() === '') {
            throw new Error('collectionName must be a non-empty string');
        }

        return {
            id: id || generateRequestId('query'),
            query,
            collectionName,
            options: options || {}
        };
    }

    _resolveTimeout(timeoutMs) {
        if (typeof timeoutMs === 'number' && timeoutMs > 0) {
            return timeoutMs;
        }
        return this.defaultTimeoutMs;
    }

    _resolveConcurrency(maxConcurrencyOption, totalRequests) {
        let value = this.maxConcurrency || totalRequests;
        if (typeof maxConcurrencyOption === 'number' && maxConcurrencyOption > 0) {
            value = maxConcurrencyOption;
        }
        return Math.max(1, Math.min(value, totalRequests || 1));
    }

    _updateMetrics({ success, queryTime, resultSize, querySize, error }) {
        this.metrics.totalQueries += 1;
        this.metrics.totalQueryTime += queryTime;
        this.metrics.totalResultSize += resultSize;
        this.metrics.totalQuerySize += querySize;

        if (success) {
            this.metrics.successfulQueries += 1;
        } else {
            this.metrics.failedQueries += 1;
            this.metrics.lastError = error || null;
        }
    }

    _buildSummary(results) {
        let totalQueries = 0;
        let successfulQueries = 0;
        let failedQueries = 0;
        let totalQueryTime = 0;
        let totalResultSize = 0;
        let totalQuerySize = 0;

        results.forEach((item) => {
            totalQueries += 1;
            if (item.error) {
                failedQueries += 1;
            } else {
                successfulQueries += 1;
            }

            if (item.metrics) {
                totalQueryTime += item.metrics.queryTime || 0;
                totalResultSize += item.metrics.resultSize || 0;
                totalQuerySize += item.metrics.querySize || 0;
            }
        });

        return {
            totalQueries,
            successfulQueries,
            failedQueries,
            totalQueryTime,
            totalResultSize,
            totalQuerySize
        };
    }
}

module.exports = QueryDispatcher;


