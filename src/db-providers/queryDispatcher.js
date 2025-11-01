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
     * Выполнение одиночного запроса
     * @param {Object} request
     * @param {Array} request.query
     * @param {string} request.collectionName
     * @param {Object} [request.options]
     * @param {string} [request.id]
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @returns {Promise<{id: string, result: Array|null, error: Error|null, metrics: Object}>}
     */
    async executeQuery(request, options = {}) {
        const preparedRequest = this._normalizeRequest(request);
        return this._executePreparedQuery(preparedRequest, options);
    }

    /**
     * Выполнение массива запросов с возможностью ограничения параллелизма
     * @param {Array<Object>} requests
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @param {boolean} [options.stopOnError]
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
        const stopOnError = options.stopOnError === true;
        const requestedConcurrency = this._resolveConcurrency(options.maxConcurrency, requests.length);
        const effectiveConcurrency = stopOnError ? 1 : requestedConcurrency;

        const preparedRequests = requests.map((request) => this._normalizeRequest(request));
        const results = new Array(preparedRequests.length);
        let nextIndex = 0;
        let stopRequested = false;

        const acquireNext = () => {
            if (stopOnError && stopRequested) {
                return null;
            }

            if (nextIndex >= preparedRequests.length) {
                return null;
            }

            const current = {
                preparedRequest: preparedRequests[nextIndex],
                index: nextIndex
            };
            nextIndex += 1;
            return current;
        };

        const runWorker = async () => {
            while (true) {
                const item = acquireNext();
                if (!item) {
                    break;
                }

                const result = await this._executePreparedQuery(item.preparedRequest, { timeoutMs });
                results[item.index] = result;

                if (stopOnError && result.error) {
                    stopRequested = true;
                }
            }
        };

        const workerCount = Math.min(effectiveConcurrency, Math.max(1, preparedRequests.length));
        const workers = Array.from({ length: workerCount }, () => runWorker());
        await Promise.all(workers);

        for (let i = 0; i < results.length; i++) {
            if (!results[i]) {
                const prepared = preparedRequests[i];
                const skipped = stopOnError && stopRequested;
                results[i] = {
                    id: prepared.id,
                    result: null,
                    error: skipped ? new Error('Query skipped due to previous error') : null,
                    metrics: {
                        queryTime: 0,
                        querySize: estimateSize(prepared.query),
                        resultSize: 0
                    },
                    skipped
                };
            }
        }

        const summary = this._buildSummary(results);
        return { results, summary };
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


