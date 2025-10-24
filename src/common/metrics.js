const WorkerMetricsCollector = require('./workerMetricsCollector');

// Глобальный экземпляр коллектора метрик для worker'а
let metricsCollector = null;

/**
 * Инициализирует коллектор метрик для worker'а
 * @param {string} workerId - ID worker'а
 */
function initializeMetricsCollector(workerId) {
    if (!metricsCollector) {
        metricsCollector = new WorkerMetricsCollector(workerId);
    }
    return metricsCollector;
}

/**
 * Получает коллектор метрик
 * @returns {WorkerMetricsCollector|null} коллектор метрик
 */
function getMetricsCollector() {
    return metricsCollector;
}

/**
 * Собирает метрики Prometheus на основе результата обработки сообщения
 * @param {number} messageType - тип сообщения
 * @param {string} endpoint - тип эндпоинта (json/iris)
 * @param {Object} processingTime - время обработки
 * @param {Object} metrics - метрики
 * @param {Object} logger - логгер для записи ошибок
 */
function collectPrometheusMetrics(messageType, endpoint, processingTime, metrics, logger) {
    if (metricsCollector) {
        metricsCollector.collectPrometheusMetrics(messageType, endpoint, processingTime, metrics, logger);
    } else {
        // Автоматически инициализируем коллектор, если он не был инициализирован
        // Это нужно для режима одного worker'а (не кластера)
        const workerId = `worker-${process.pid}`;
        initializeMetricsCollector(workerId);
        if (metricsCollector) {
            metricsCollector.collectPrometheusMetrics(messageType, endpoint, processingTime, metrics, logger);
        }
    }
}

/**
 * Получает реестр метрик
 * @returns {Object|null} реестр метрик Prometheus
 */
function getRegister() {
    return metricsCollector ? metricsCollector.getRegister() : null;
}

/**
 * Завершает работу коллектора метрик
 */
function destroyMetricsCollector() {
    if (metricsCollector) {
        metricsCollector.destroy();
        metricsCollector = null;
    }
}

module.exports = {
    initializeMetricsCollector,
    getMetricsCollector,
    collectPrometheusMetrics,
    getRegister,
    destroyMetricsCollector
};
