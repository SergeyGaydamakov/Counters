const client = require('prom-client');
const Logger = require('../utils/logger');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

/**
 * Модуль для сбора метрик в worker'ах кластера
 * Собирает метрики локально и периодически отправляет их мастеру
 */
class WorkerMetricsCollector {
    constructor(workerId) {
        this.workerId = workerId;
        this.register = new client.Registry();
        
        // Создаем локальные метрики
        this.createLocalMetrics();
        
        // Интервал отправки метрик мастеру
        this.sendInterval = setInterval(async () => {
            await this.sendMetricsToMaster();
        }, 10000); // Отправляем каждые 10 секунд
    }

    /**
     * Создает локальные метрики
     */
    createLocalMetrics() {
        // 1. Счетчик запросов
        this.requestCounter = new client.Counter({
            name: 'requests_total',
            help: 'Total number of requests by message type',
            labelNames: ['message_type', 'worker_id', 'endpoint'],
            registers: [this.register]
        });

        // 2. Гистограмма длительности запросов
        this.requestDurationHistogram = new client.Histogram({
            name: 'request_duration_seconds',
            help: 'Request duration in msec by message type',
            labelNames: ['message_type', 'worker_id', 'endpoint'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // 3. Гистограмма длительности сохранения фактов
        this.saveFactDurationHistogram = new client.Histogram({
            name: 'save_fact_duration_seconds',
            help: 'Save fact duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // 4. Гистограмма длительности сохранения индексов
        this.saveIndexDurationHistogram = new client.Histogram({
            name: 'save_index_duration_seconds',
            help: 'Save index duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // 5. Гистограмма длительности вычисления счетчиков
        this.countersDurationHistogram = new client.Histogram({
            name: 'counters_calculation_duration_seconds',
            help: 'Counters calculation duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // 6. Гистограмма длительности общей обработки
        this.totalProcessingDurationHistogram = new client.Histogram({
            name: 'total_processing_duration_seconds',
            help: 'Total processing duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });
        

        // + 7. Гистограмма количества счетчиков
        this.totalIndexCountHistogram = new client.Histogram({
            name: 'total_index_count',
            help: 'Number of fact indexes ',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30],
            registers: [this.register]
        });

        // + 8. Гистограмма количества одновременных запросов на получение
        this.parallelCountersRequestsCountHistogram = new client.Histogram({
            name: 'parallel_counters_requests_count',
            help: 'Number of parallel counters requests',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 45, 50, 55, 60],
            registers: [this.register]
        });

        // 9. Гистограмма количества счетчиков
        this.factCountersCountHistogram = new client.Histogram({
            name: 'fact_counters_count',
            help: 'Number of calculated counters per fact',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500],
            registers: [this.register]
        });


        // 9.1 Гистограмма количества потенциально изменяемых счетчиков
        this.evaluationCountersCountHistogram = new client.Histogram({
            name: 'evaluation_counters_count',
            help: 'Number of evaluation counters per fact',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500],
            registers: [this.register]
        });

        // 10. Гистограмма длительности запросов релевантных фактов
        this.relevantFactsQueryTimeHistogram = new client.Histogram({
            name: 'relevant_facts_query_duration_seconds',
            help: 'Relevant facts query duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // 11. Гистограмма длительности запросов счетчиков
        this.countersQueryTimeHistogram = new client.Histogram({
            name: 'counters_calculation_query_duration_seconds',
            help: 'Counters calculation query duration in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000],
            registers: [this.register]
        });

        // + 12. Гистограмма количества запрошенных счетчиков
        this.queryCountersCountHistogram = new client.Histogram({
            name: 'query_counters_count',
            help: 'Number of query counters',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500],
            registers: [this.register]
        });

        // + 13. Гистограмма количества полученных счетчиков
        this.resultCountersCountHistogram = new client.Histogram({
            name: 'result_counters_count',
            help: 'Number of result counters',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500],
            registers: [this.register]
        });

        // 14. Gauge для текущего количества checked out соединений в connection pool
        this.connectionPoolCheckedOut = new client.Gauge({
            name: 'mongodb_connection_pool_checked_out',
            help: 'Number of connections currently checked out from the pool',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 15. Counter для количества checkout операций
        this.connectionPoolCheckoutCounter = new client.Counter({
            name: 'mongodb_connection_pool_checkout_total',
            help: 'Total number of connection checkout operations',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 16. Counter для количества checkin операций
        this.connectionPoolCheckinCounter = new client.Counter({
            name: 'mongodb_connection_pool_checkin_total',
            help: 'Total number of connection checkin operations',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 17. Counter для количества checkout попыток (started)
        this.connectionCheckoutStartedCounter = new client.Counter({
            name: 'mongodb_connection_checkout_started_total',
            help: 'Total number of connection checkout attempts',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 18. Counter для количества неудачных checkout операций
        this.connectionCheckoutFailedCounter = new client.Counter({
            name: 'mongodb_connection_checkout_failed_total',
            help: 'Total number of failed connection checkout attempts',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 19. Counter для создания соединений
        this.connectionCreatedCounter = new client.Counter({
            name: 'mongodb_connection_created_total',
            help: 'Total number of connections created',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 20. Counter для закрытия соединений
        this.connectionClosedCounter = new client.Counter({
            name: 'mongodb_connection_closed_total',
            help: 'Total number of connections closed',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 21. Counter для создания пула соединений
        this.connectionPoolCreatedCounter = new client.Counter({
            name: 'mongodb_connection_pool_created_total',
            help: 'Total number of connection pools created',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 22. Counter для готовности пула соединений
        this.connectionPoolReadyCounter = new client.Counter({
            name: 'mongodb_connection_pool_ready_total',
            help: 'Total number of connection pools ready',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 23. Counter для закрытия пула соединений
        this.connectionPoolClosedCounter = new client.Counter({
            name: 'mongodb_connection_pool_closed_total',
            help: 'Total number of connection pools closed',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 24. Counter для очистки пула соединений
        this.connectionPoolClearedCounter = new client.Counter({
            name: 'mongodb_connection_pool_cleared_total',
            help: 'Total number of connection pools cleared',
            labelNames: ['worker_id', 'client_type'],
            registers: [this.register]
        });

        // 25. Гистограмма длительности ожидания запросов в QueryDispatcher
        this.queryDispatcherWaitTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_wait_time_msec',
            help: 'Query dispatcher wait time in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200],
            registers: [this.register]
        });

        // 26. Гистограмма длительности подготовки батчей в QueryDispatcher
        this.queryDispatcherBatchPreparationTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_batch_preparation_time_msec',
            help: 'Query dispatcher batch preparation time in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50],
            registers: [this.register]
        });

        // 27. Гистограмма длительности обработки результатов в QueryDispatcher
        this.queryDispatcherResultsProcessingTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_results_processing_time_msec',
            help: 'Query dispatcher results processing time in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50],
            registers: [this.register]
        });

        // 28. Гистограмма длительности преобразования результатов QueryDispatcher в формат mongoProvider
        this.queryDispatcherResultsTransformationTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_results_transformation_time_msec',
            help: 'Query dispatcher results transformation time in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50],
            registers: [this.register]
        });

        // 28.1 Гистограмма длительности выполнения батчей (IPC коммуникация и ожидание результатов)
        this.queryDispatcherBatchExecutionTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_batch_execution_time_msec',
            help: 'Query dispatcher batch execution time in msec (IPC communication and waiting for worker results)',
            labelNames: ['message_type', 'worker_id'],
            buckets: [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 150, 200, 500, 1000],
            registers: [this.register]
        });

        // 29. Гистограмма длительности инициализации пула процессов в QueryDispatcher
        this.queryDispatcherPoolInitTimeHistogram = new client.Histogram({
            name: 'query_dispatcher_pool_init_time_msec',
            help: 'Query dispatcher pool initialization time in msec',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 100, 150, 200, 500, 1000, 1500, 2000],
            registers: [this.register]
        });
    }

    /**
     * Регистрирует checkout операции из connection pool
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     * @param {number} checkedOutCount - текущее количество checked out соединений
     */
    recordConnectionCheckout(clientType, checkedOutCount) {
        try {
            this.connectionPoolCheckedOut.set({ worker_id: this.workerId, client_type: clientType }, checkedOutCount);
            this.connectionPoolCheckoutCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации checkout: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует checkin операции в connection pool
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     * @param {number} checkedOutCount - текущее количество checked out соединений
     */
    recordConnectionCheckin(clientType, checkedOutCount) {
        try {
            this.connectionPoolCheckedOut.set({ worker_id: this.workerId, client_type: clientType }, checkedOutCount);
            this.connectionPoolCheckinCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации checkin: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует событие начала попытки checkout
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionCheckoutStarted(clientType) {
        try {
            this.connectionCheckoutStartedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации checkout started: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует неудачную попытку checkout
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionCheckoutFailed(clientType) {
        try {
            this.connectionCheckoutFailedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации checkout failed: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует создание соединения
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionCreated(clientType) {
        try {
            this.connectionCreatedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации connection created: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует закрытие соединения
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionClosed(clientType) {
        try {
            this.connectionClosedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации connection closed: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует создание пула соединений
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionPoolCreated(clientType) {
        try {
            this.connectionPoolCreatedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации pool created: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует готовность пула соединений
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionPoolReady(clientType) {
        try {
            this.connectionPoolReadyCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации pool ready: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует закрытие пула соединений
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionPoolClosed(clientType) {
        try {
            this.connectionPoolClosedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации pool closed: ${error.message}`);
            }
        }
    }

    /**
     * Регистрирует очистку пула соединений
     * @param {string} clientType - тип клиента ('counter' или 'aggregate')
     */
    recordConnectionPoolCleared(clientType) {
        try {
            this.connectionPoolClearedCounter.inc({ worker_id: this.workerId, client_type: clientType });
        } catch (error) {
            if (logger) {
                logger.error(`Ошибка при регистрации pool cleared: ${error.message}`);
            }
        }
    }

    /**
     * Собирает метрики Prometheus на основе результата обработки сообщения
     * @param {number} messageType - тип сообщения
     * @param {string} endpoint - тип эндпоинта (json/iris)
     * @param {Object} processingTime - время обработки
     * @param {Object} metrics - метрики
     * @param {Object} logger - логгер для записи ошибок
     */
    collectPrometheusMetrics(messageType, endpoint, processingTime, metrics, logger) {
        try {
            const messageTypeStr = messageType.toString();
            
            // 1. Увеличиваем счетчик запросов
            this.requestCounter.inc({ message_type: messageTypeStr, worker_id:this.workerId, endpoint });
            
            // 2. Общая длительность запроса
            if (processingTime && typeof processingTime.total === 'number' && processingTime.total) {
                this.requestDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId, endpoint }, processingTime.total);
            }
            
            // 3. Длительность сохранения факта
            if (processingTime && typeof processingTime.saveFact === 'number' && processingTime.saveFact) {
                this.saveFactDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.saveFact);
            }
            
            // 4. Длительность сохранения индексных значений
            if (processingTime && typeof processingTime.saveIndex === 'number' && processingTime.saveIndex) {
                this.saveIndexDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.saveIndex);
            }
            
            // 5. Длительность вычисления счетчиков
            if (processingTime && typeof processingTime.counters === 'number' && processingTime.counters) {
                this.countersDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.counters);
            }
            
            // 6. Общая длительность обработки
            if (processingTime && typeof processingTime.total === 'number' && processingTime.total) {
                this.totalProcessingDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.total);
            }
            
            // *7. Количество сохраняемых индексов
            if (metrics && typeof metrics.totalIndexCount === 'number' && metrics.totalIndexCount !== undefined) {
                this.totalIndexCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.totalIndexCount);
            }

            // *8. Количество одновременных запросов на получение счетчиков
            if (metrics && typeof metrics.relevantIndexCount === 'number' && metrics.relevantIndexCount !== undefined) {
                this.parallelCountersRequestsCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.relevantIndexCount);
            }
            
            // 9. Количество вычисляемых счетчиков
            if (metrics && typeof metrics.factCountersCount === 'number' && metrics.factCountersCount !== undefined) {
                this.factCountersCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.factCountersCount);
            }

            // 9.1 Количество потенциально изменяемых счетчиков
            if (metrics && typeof metrics.evaluationCountersCount === 'number' && metrics.evaluationCountersCount !== undefined) {
                this.evaluationCountersCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.evaluationCountersCount);
            }
            
            // 10. Длительность запросов к базе данных для получения ИД фактов
            if (metrics && typeof metrics.relevantFactsQueryTime === 'number' && metrics.relevantFactsQueryTime !== undefined) {
                this.relevantFactsQueryTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.relevantFactsQueryTime);
            }
            
            // 11. Длительность запросов к базе данных для вычисления значений счетчиков
            if (metrics && typeof metrics.countersQueryTime === 'number' && metrics.countersQueryTime !== undefined) {
                this.countersQueryTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersQueryTime);
            }
            
            // + 12. Количество запрошенных счетчиков
            if (metrics && typeof metrics.queryCountersCount === 'number' && metrics.queryCountersCount !== undefined) {
                this.queryCountersCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.queryCountersCount);
            }
            
            // + 13. Количество полученных счетчиков
            if (metrics && typeof metrics.resultCountersCount === 'number' && metrics.resultCountersCount !== undefined) {
                this.resultCountersCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.resultCountersCount);
            }

            // + 14. Длительность ожидания запросов в QueryDispatcher
            if (metrics && typeof metrics.countersQueryWaitTime === 'number' && metrics.countersQueryWaitTime !== undefined) {
                this.queryDispatcherWaitTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersQueryWaitTime);
            }

            // + 14.1 Длительность инициализации пула процессов в QueryDispatcher
            if (metrics && typeof metrics.countersPoolInitTime === 'number' && metrics.countersPoolInitTime !== undefined) {
                this.queryDispatcherPoolInitTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersPoolInitTime);
            }

            // + 15. Длительность подготовки батчей в QueryDispatcher
            if (metrics && typeof metrics.countersBatchPreparationTime === 'number' && metrics.countersBatchPreparationTime !== undefined) {
                this.queryDispatcherBatchPreparationTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersBatchPreparationTime);
            }

            // + 15.1 Длительность выполнения батчей в QueryDispatcher (IPC коммуникация и ожидание результатов)
            if (metrics && typeof metrics.countersBatchExecutionTime === 'number' && metrics.countersBatchExecutionTime !== undefined) {
                this.queryDispatcherBatchExecutionTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersBatchExecutionTime);
            }

            // + 16. Длительность обработки результатов в QueryDispatcher
            if (metrics && typeof metrics.countersResultsProcessingTime === 'number' && metrics.countersResultsProcessingTime !== undefined) {
                this.queryDispatcherResultsProcessingTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersResultsProcessingTime);
            }

            // + 17. Длительность преобразования результатов QueryDispatcher в формат mongoProvider
            if (metrics && typeof metrics.countersResultsTransformationTime === 'number' && metrics.countersResultsTransformationTime !== undefined) {
                this.queryDispatcherResultsTransformationTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersResultsTransformationTime);
            }
        } catch (error) {
            if (logger) {
                logger.error('Ошибка при сборе метрик Prometheus:', {
                    error: error.message,
                    stack: error.stack,
                    messageType,
                    endpoint
                });
            }
        }
    }

    /**
     * Отправляет метрики мастеру
     */
    async sendMetricsToMaster() {
        try {
            // Получаем сырые данные метрик из реестра
            const metricsData = await this.register.metrics();
            
            // Отправляем мастеру
            if (process.send) {
                process.send({
                    type: 'worker-metrics',
                    workerId: this.workerId,
                    metricsData: metricsData
                });
            } else {
                logger.warn(`Worker ${this.workerId} - мастер процесс для отправки логов недоступен process.send`);
            }
        } catch (error) {
            logger.error('Ошибка при отправке метрик мастеру:', error);
        }
    }

    /**
     * Возвращает локальный реестр метрик
     * @returns {Object} реестр метрик Prometheus
     */
    getRegister() {
        return this.register;
    }

    /**
     * Завершает работу коллектора
     */
    destroy() {
        if (this.sendInterval) {
            clearInterval(this.sendInterval);
        }
    }
}

module.exports = WorkerMetricsCollector;
