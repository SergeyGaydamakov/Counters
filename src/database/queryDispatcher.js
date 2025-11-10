/**
 * QueryDispatcher - Диспетчер запросов для ProcessPoolManager
 *
 * Предоставляет высокоуровневый интерфейс поверх ProcessPoolManager для
 * выполнения одиночных и множественных запросов, агрегации результатов,
 * обработки ошибок и сбора метрик.
 */

const Logger = require('../common/logger');
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

        // Сохраняем промис инициализации для ожидания один раз при первом использовании
        this._initializationPromise = null;
        if (this.processPoolManager?._initializationPromise) {
            // Сохраняем промис, чтобы ждать его только один раз
            this._initializationPromise = this.processPoolManager._initializationPromise;
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
     * Выполнение массива запросов с оптимизированным распределением по воркерам
     * Использует work queue pattern - батчи распределяются по воркерам по мере их освобождения
     * Запросы делятся на батчи (не более this.minWorkers и не более количества запросов), которые обрабатываются динамически
     * @param {Array<Object>} requests
     * @param {Object} [options]
     * @param {number} [options.timeoutMs]
     * @param {number} [options.maxWaitForWorkersMs] - Максимальное время ожидания освобождения воркеров (по умолчанию 500мс)
     * @returns {Promise<{results: Array, summary: Object}>}
     */
    async executeQueries(requests, options = {}) {
        if (!Array.isArray(requests)) {
            throw new Error('requests must be an array');
        }

        if (requests.length === 0) {
            return { results: [], summary: this._buildSummary([], {}) };
        }

        const timeoutMs = this._resolveTimeout(options.timeoutMs);
        const maxWaitForWorkersMs = typeof options.maxWaitForWorkersMs === 'number' && options.maxWaitForWorkersMs > 0
            ? options.maxWaitForWorkersMs
            : this.maxWaitForWorkersMs;
        
        // Измеряем время инициализации пула (ожидаем только один раз при первом вызове)
        const poolInitStartTime = Date.now();
        if (this._initializationPromise) {
            await this._initializationPromise;
            // После первого ожидания очищаем промис, чтобы последующие вызовы были мгновенными
            this._initializationPromise = null;
        }
        const poolInitTime = Date.now() - poolInitStartTime;
        
        // Нормализуем запросы
        const batchPreparationStartTime = Date.now();
        const preparedRequests = requests.map((request) => this._normalizeRequest(request));
        
        // Делим запросы на батчи (не более this.minWorkers и не более количества запросов)
        const batches = [];
        const actualBatchCount = Math.min(this.minWorkers, preparedRequests.length);
        const requestsPerBatch = Math.ceil(preparedRequests.length / actualBatchCount);
        
        for (let i = 0; i < preparedRequests.length; i += requestsPerBatch) {
            batches.push(preparedRequests.slice(i, i + requestsPerBatch));
        }
        const batchPreparationTime = Date.now() - batchPreparationStartTime;
        
        // Выполняем батчи через ProcessPoolManager с динамическим распределением
        // waitTime и queryTime вычисляются внутри executeBatchesAsync для каждого батча
        const batchExecutionStartTime = Date.now();
        const batchResultsArray = await this.processPoolManager.executeBatchesAsync(
            batches.map(batch => batch.map((prepared) => ({
                id: prepared.id,
                query: prepared.query,
                collectionName: prepared.collectionName,
                options: prepared.options
            }))),
            {
                timeoutMs,
                maxWaitForWorkersMs
            }
        );
        const batchExecutionTime = Date.now() - batchExecutionStartTime;
        
        // Собираем все результаты
        const allResults = batchResultsArray.flat();

        // Измеряем время обработки результатов
        const resultsProcessingStartTime = Date.now();
        
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
                // Метрики уже содержат waitTime и queryTime из executeBatchesAsync
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
                        resultSize: 0,
                        waitTime: 0
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
        
        const resultsProcessingTime = Date.now() - resultsProcessingStartTime;

        // Вычисляем средний waitTime из результатов для summary
        const avgWaitTime = orderedResults.length > 0
            ? orderedResults.reduce((sum, r) => sum + (r.metrics?.waitTime || 0), 0) / orderedResults.length
            : 0;
        
        const summary = this._buildSummary(orderedResults, {
            waitTime: Math.round(avgWaitTime),
            poolInitTime,
            batchPreparationTime,
            batchExecutionTime,
            resultsProcessingTime
        });
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

    _buildSummary(results, additionalMetrics = {}) {
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
            totalQuerySize,
            waitTime: additionalMetrics.waitTime || 0,
            poolInitTime: additionalMetrics.poolInitTime || 0,
            batchPreparationTime: additionalMetrics.batchPreparationTime || 0,
            batchExecutionTime: additionalMetrics.batchExecutionTime || 0,
            resultsProcessingTime: additionalMetrics.resultsProcessingTime || 0
        };
    }
}

module.exports = QueryDispatcher;


