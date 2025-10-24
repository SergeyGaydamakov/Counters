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
        
        // Данные для отправки мастеру
        this.metricsData = {
            requestCounts: {},
            requestDurations: {},
            saveFactDurations: {},
            saveIndexDurations: {},
            countersDurations: {},
            totalProcessingDurations: {},
            totalIndexCounts: {},
            parallelCountersRequestsCounts: {},
            factCountersCounts: {},
            relevantFactsTimes: {},
            countersQueryTimes: {}
        };
        
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
            buckets: [2, 5, 7, 10, 15, 20],
            registers: [this.register]
        });

        // + 8. Гистограмма количества одновременных запросов на получение
        this.parallelCountersRequestsCountHistogram = new client.Histogram({
            name: 'parallel_counters_requests_count',
            help: 'Number of parallel counters requests',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20],
            registers: [this.register]
        });

        // 9. Гистограмма количества счетчиков
        this.factCountersCountHistogram = new client.Histogram({
            name: 'fact_counters_count',
            help: 'Number of calculated counters per fact',
            labelNames: ['message_type', 'worker_id'],
            buckets: [1, 5, 10, 25, 50, 100, 250, 500, 750, 1000, 1250, 1500],
            registers: [this.register]
        });

        // 10. Гистограмма длительности запросов релевантных фактов
        this.relevantFactsTimeHistogram = new client.Histogram({
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
            if (processingTime && processingTime.total) {
                this.requestDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId, endpoint }, processingTime.total);
            }
            
            // 3. Длительность сохранения факта
            if (processingTime && processingTime.saveFact) {
                this.saveFactDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.saveFact);
            }
            
            // 4. Длительность сохранения индексных значений
            if (processingTime && processingTime.saveIndex) {
                this.saveIndexDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.saveIndex);
            }
            
            // 5. Длительность вычисления счетчиков
            if (processingTime && processingTime.counters) {
                this.countersDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.counters);
            }
            
            // 6. Общая длительность обработки
            if (processingTime && processingTime.total) {
                this.totalProcessingDurationHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, processingTime.total);
            }
            
            // *7. Количество сохраняемых индексов
            if (metrics && typeof metrics.totalIndexCount === 'number') {
                this.totalIndexCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.totalIndexCount);
            }

            // *7. Количество сохраняемых индексов
            if (metrics && typeof metrics.counterIndexCountWithGroup === 'number') {
                this.parallelCountersRequestsCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.counterIndexCountWithGroup);
            }
            
            
            // 8. Количество вычисляемых счетчиков
            if (metrics && typeof metrics.factCountersCount === 'number') {
                this.factCountersCountHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.factCountersCount);
            }
            
            // 8. Длительность запросов к базе данных для получения ИД фактов
            if (metrics && typeof metrics.relevantFactsTime === 'number') {
                this.relevantFactsTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.relevantFactsTime);
            }
            
            // 9. Длительность запросов к базе данных для вычисления значений счетчиков
            if (metrics && typeof metrics.countersQueryTime === 'number') {
                this.countersQueryTimeHistogram.observe({ message_type: messageTypeStr, worker_id:this.workerId }, metrics.countersQueryTime);
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
                logger.warn(`Worker ${this.workerId} - process.send недоступен`);
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
