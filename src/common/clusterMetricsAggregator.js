const client = require('prom-client');
const Logger = require('../utils/logger');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');


/**
 * Агрегатор метрик для кластерной среды
 * Собирает метрики от всех worker'ов и объединяет их
 */
class ClusterMetricsAggregator {
    constructor() {
        this.registry = new client.Registry();
        this.workerMetrics = new Map(); // Map<workerId, {rawMetrics, lastUpdate}>
        
        // Добавляем системные метрики
        client.collectDefaultMetrics({ register: this.registry });
        
        // Создаем метрику активных worker'ов
        this.activeWorkersGauge = new client.Gauge({
            name: 'active_workers',
            help: 'Number of active workers',
            registers: [this.registry]
        });
        
        // Интервал обновления метрик
        this.updateInterval = setInterval(() => {
            this.updateWorkerList();
        }, 5000); // Обновляем каждые 5 секунд
    }

    /**
     * Обновляет метрики от конкретного worker'а
     * @param {string} workerId - ID worker'а
     * @param {string} metricsData - сырые данные метрик в формате Prometheus
     */
    updateWorkerMetrics(workerId, metricsData) {
        try {
            // Сохраняем сырые данные метрик
            this.workerMetrics.set(workerId, {
                rawMetrics: metricsData,
                lastUpdate: Date.now()
            });            
        } catch (error) {
            logger.error(`Ошибка при обновлении метрик от worker ${workerId}:`, error);
        }
    }

    /**
     * Удаляет метрики worker'а при его завершении
     * @param {string} workerId - ID worker'а
     */
    removeWorkerMetrics(workerId) {
        this.workerMetrics.delete(workerId);
    }

    /**
     * Обновляет список процессов, выдающих метрики
     */
    updateWorkerList() {
        try {
            // Обновляем количество активных worker'ов
            this.activeWorkersGauge.set(this.workerMetrics.size);
            
            // Удаляем устаревшие метрики (старше 30 секунд)
            const now = Date.now();
            for (const [workerId, workerData] of this.workerMetrics) {
                if (now - workerData.lastUpdate > 30000) {
                    this.workerMetrics.delete(workerId);
                }
            }
        } catch (error) {
            logger.error('Ошибка при обновлении списка процессов:', error);
        }
    }

    /**
     * Возвращает объединенные метрики от всех worker'ов
     * @returns {string} объединенные метрики в формате Prometheus
     */
    async getCombinedMetrics() {
        let combinedMetrics = '';
        
        // Добавляем метрику активных worker'ов
        combinedMetrics += `# HELP active_workers Number of active workers\n`;
        combinedMetrics += `# TYPE active_workers gauge\n`;
        // combinedMetrics += `active_workers ${this.workerMetrics.size}\n\n`;
        
        // Объединяем метрики от всех worker'ов
        for (const [workerId, workerData] of this.workerMetrics) {
            if (workerData.rawMetrics && typeof workerData.rawMetrics === 'string') {
                combinedMetrics += `# Metrics from ${workerId}\n`;
                combinedMetrics += workerData.rawMetrics + '\n';
            }
        }
        
        return combinedMetrics;
    }

    /**
     * Возвращает реестр метрик (для совместимости)
     * @returns {Object} реестр метрик Prometheus
     */
    getRegister() {
        return this.registry;
    }

    /**
     * Завершает работу агрегатора
     */
    destroy() {
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
        }
        this.registry.clear();
        this.workerMetrics.clear();
    }
}

module.exports = ClusterMetricsAggregator;