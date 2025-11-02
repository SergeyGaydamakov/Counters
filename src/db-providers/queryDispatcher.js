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

        this.defaultTimeoutMs = typeof options.defaultTimeoutMs === 'number'
            ? options.defaultTimeoutMs
            : 60000;

        this.maxConcurrency = typeof options.maxConcurrency === 'number' && options.maxConcurrency > 0
            ? options.maxConcurrency
            : null;

        this.processPoolManager = options.processPoolManager;
        this._ownsProcessPool = false;

        if (!this.processPoolManager) {
            const workerCount = options.workerCount || config.parallelsRequestProcesses || 1;
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
     * Выполнение одного или нескольких запросов
     * @param {Object|Array<Object>} requestOrRequests
     * @param {Array} requestOrRequests[].query
     * @param {string} requestOrRequests[].collectionName
     * @param {Object} [requestOrRequests[].options]
     * @param {string} [requestOrRequests[].id]
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @returns {Promise<Object>|Promise<{result: Array, results: Array, summary: Object, errors: Array}>}
     */
    async executeQuery(requestOrRequests, options = {}) {
        if (Array.isArray(requestOrRequests)) {
            const preparedRequests = requestOrRequests.map((request) => this._normalizeRequest(request));
            return this._executePreparedBatch(preparedRequests, options);
        }

        const preparedRequest = this._normalizeRequest(requestOrRequests);
        return this._executePreparedQuery(preparedRequest, options);
    }

    /**
     * Выполнение массива запросов с возможностью ограничения параллелизма
     * Запросы распределяются по процессам в пуле через batch API
     * @param {Array<Object>} requests
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @param {number} [options.maxConcurrency]
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
        
        // Получаем количество доступных процессов из пула
        const poolStats = this.processPoolManager?.getStats();
        const availableWorkers = poolStats?.activeWorkers || poolStats?.workerCount || 1;
        const requestedConcurrency = this._resolveConcurrency(options.maxConcurrency, requests.length);
        
        const preparedRequests = requests.map((request) => this._normalizeRequest(request));
        
        // Разделяем запросы на батчи для распределения по процессам
        const batches = [];
        const batchCount = Math.min(availableWorkers, requestedConcurrency, preparedRequests.length);
        const requestsPerBatch = Math.ceil(preparedRequests.length / batchCount);
        
        for (let i = 0; i < preparedRequests.length; i += requestsPerBatch) {
            batches.push(preparedRequests.slice(i, i + requestsPerBatch));
        }

        // Параллельное выполнение всех батчей
        const batchPromises = batches.map(async (batch) => {
            try {
                const batchResponse = await this._executePreparedBatch(batch, { timeoutMs });
                return batchResponse.results;
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

        // Восстанавливаем порядок результатов согласно исходному порядку запросов
        const orderedResults = preparedRequests.map((prepared) => {
            const result = resultsById.get(prepared.id);
            if (result) {
                return result;
            }

            // Если результат не найден, создаем ошибку
            return {
                id: prepared.id,
                result: null,
                error: new Error('Missing result from batch execution'),
                metrics: {
                    queryTime: 0,
                    querySize: estimateSize(prepared.query),
                    resultSize: 0
                }
            };
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

    async _executePreparedQuery(preparedRequest, options = {}) {
        const timeoutMs = this._resolveTimeout(options.timeoutMs);

        const querySize = estimateSize(preparedRequest.query);
        const startTime = Date.now();
        let result = null;
        let error = null;

        try {
            result = await this._executeWithTimeout(preparedRequest, timeoutMs);
        } catch (err) {
            error = err;
        }

        const queryTime = Date.now() - startTime;
        const resultSize = result ? estimateSize(result) : 0;

        this._updateMetrics({
            success: !error,
            queryTime,
            resultSize,
            querySize,
            error
        });

        return {
            id: preparedRequest.id,
            result,
            error,
            metrics: {
                queryTime,
                querySize,
                resultSize
            }
        };
    }

    async _executePreparedBatch(preparedRequests, options = {}) {
        if (!Array.isArray(preparedRequests) || preparedRequests.length === 0) {
            return {
                result: [],
                results: [],
                errors: [],
                summary: this._buildSummary([]),
                metrics: {
                    totalQueryTime: 0,
                    totalResultSize: 0,
                    totalQuerySize: 0
                }
            };
        }

        const timeoutMs = this._resolveTimeout(options.timeoutMs);
        let rawResults;

        try {
            rawResults = await this._executeBatchWithTimeout(preparedRequests, timeoutMs);
        } catch (error) {
            preparedRequests.forEach((prepared) => {
                this._updateMetrics({
                    success: false,
                    queryTime: timeoutMs || 0,
                    resultSize: 0,
                    querySize: estimateSize(prepared.query),
                    error
                });
            });
            throw error;
        }

        const resultsById = new Map();
        if (Array.isArray(rawResults)) {
            rawResults.forEach((item) => {
                if (item && item.id) {
                    resultsById.set(item.id, item);
                }
            });
        }

        const orderedResults = preparedRequests.map((prepared) => {
            let raw = resultsById.get(prepared.id);
            if (!raw) {
                raw = {
                    result: null,
                    error: new Error('Missing result from worker process'),
                    metrics: null
                };
            }

            const error = raw.error instanceof Error
                ? raw.error
                : raw.error
                    ? new Error(raw.error.message || 'Unknown error')
                    : null;

            const metricsFromWorker = raw.metrics && typeof raw.metrics === 'object' ? raw.metrics : {};
            const querySize = typeof metricsFromWorker.querySize === 'number'
                ? metricsFromWorker.querySize
                : estimateSize(prepared.query);
            const resultSize = typeof metricsFromWorker.resultSize === 'number'
                ? metricsFromWorker.resultSize
                : estimateSize(raw.result ?? null);
            const queryTime = typeof metricsFromWorker.queryTime === 'number'
                ? metricsFromWorker.queryTime
                : 0;

            this._updateMetrics({
                success: !error,
                queryTime,
                resultSize,
                querySize,
                error
            });

            return {
                id: prepared.id,
                request: prepared,
                result: raw.result ?? null,
                error,
                metrics: {
                    queryTime,
                    querySize,
                    resultSize
                }
            };
        });

        const aggregatedResult = orderedResults.reduce((acc, item) => {
            if (Array.isArray(item.result)) {
                acc.push(...item.result);
            } else if (item.result !== null && item.result !== undefined) {
                acc.push(item.result);
            }
            return acc;
        }, []);

        const summary = this._buildSummary(orderedResults);
        const errors = orderedResults.filter(item => item.error);

        return {
            result: aggregatedResult,
            results: orderedResults,
            errors,
            summary,
            metrics: {
                totalQueryTime: summary.totalQueryTime,
                totalResultSize: summary.totalResultSize,
                totalQuerySize: summary.totalQuerySize
            }
        };
    }

    async _executeBatchWithTimeout(preparedRequests, timeoutMs) {
        if (!this.processPoolManager || typeof this.processPoolManager.executeBatch !== 'function') {
            throw new Error('ProcessPoolManager does not support batch execution');
        }

        const execPromise = this.processPoolManager.executeBatch(
            preparedRequests.map((prepared) => ({
                id: prepared.id,
                query: prepared.query,
                collectionName: prepared.collectionName,
                options: prepared.options
            })),
            { timeoutMs }
        );

        if (!timeoutMs || timeoutMs <= 0) {
            return execPromise;
        }

        let timeoutHandle;
        const timeoutPromise = new Promise((_, reject) => {
            timeoutHandle = setTimeout(() => {
                reject(new Error(`Batch query timeout after ${timeoutMs}ms`));
            }, timeoutMs);
        });

        try {
            return await Promise.race([execPromise, timeoutPromise]);
        } finally {
            if (timeoutHandle) {
                clearTimeout(timeoutHandle);
            }
        }
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

    async _executeWithTimeout(preparedRequest, timeoutMs) {
        const execPromise = this.processPoolManager.executeQuery({
            query: preparedRequest.query,
            collectionName: preparedRequest.collectionName,
            options: preparedRequest.options
        });

        if (!timeoutMs || timeoutMs <= 0) {
            return execPromise;
        }

        let timeoutHandle;
        const timeoutPromise = new Promise((_, reject) => {
            timeoutHandle = setTimeout(() => {
                reject(new Error(`Query timeout after ${timeoutMs}ms`));
            }, timeoutMs);
        });

        try {
            return await Promise.race([execPromise, timeoutPromise]);
        } finally {
            if (timeoutHandle) {
                clearTimeout(timeoutHandle);
            }
        }
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


