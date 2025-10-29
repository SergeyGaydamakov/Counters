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
     * Извлекает базовое имя метрики, удаляя стандартные суффиксы Prometheus
     * @param {string} metricName - полное имя метрики
     * @returns {string} базовое имя метрики
     */
    getBaseMetricName(metricName) {
        // Стандартные суффиксы Prometheus для гистограмм, summary и других
        // ВАЖНО: не удаляем '_total' — это имя счётчиков (Counter) и его
        // удаление приводит к слиянию разных метрик по общему префиксу.
        const suffixes = ['_bucket', '_count', '_sum', '_created'];
        for (const suffix of suffixes) {
            if (metricName.endsWith(suffix)) {
                return metricName.slice(0, -suffix.length);
            }
        }
        return metricName;
    }

    /**
     * Парсит метрики в формате Prometheus
     * @param {string} rawMetrics - сырые метрики
     * @returns {Object} объект с HELP, TYPE и data строками
     */
    parseMetrics(rawMetrics) {
        const lines = rawMetrics.split('\n');
        const parsed = {
            help: new Map(),   // Map<metricName, helpText>
            type: new Map(),   // Map<metricName, typeName>
            data: []           // Массив строк с данными метрик
        };

        for (const line of lines) {
            const trimmedLine = line.trim();
            
            // Пропускаем пустые строки
            if (!trimmedLine) continue;
            
            // Парсим HELP
            const helpMatch = trimmedLine.match(/^#\s*HELP\s+(\S+)\s+(.+)$/);
            if (helpMatch) {
                const [, metricName, helpText] = helpMatch;
                if (!parsed.help.has(metricName)) {
                    parsed.help.set(metricName, helpText);
                }
                continue;
            }
            
            // Парсим TYPE
            const typeMatch = trimmedLine.match(/^#\s*TYPE\s+(\S+)\s+(\S+)$/);
            if (typeMatch) {
                const [, metricName, typeName] = typeMatch;
                if (!parsed.type.has(metricName)) {
                    parsed.type.set(metricName, typeName);
                }
                continue;
            }
            
            // Остальное - это данные метрик
            if (trimmedLine.startsWith('#')) {
                // Игнорируем другие комментарии
                continue;
            }
            
            parsed.data.push(trimmedLine);
        }

        return parsed;
    }

    /**
     * Возвращает объединенные метрики от всех worker'ов
     * @returns {string} объединенные метрики в формате Prometheus
     */
    async getCombinedMetrics() {
        // Собираем данные от всех worker'ов
        const allMetrics = [];
        
        for (const [workerId, workerData] of this.workerMetrics) {
            if (workerData.rawMetrics && typeof workerData.rawMetrics === 'string') {
                // Проверяем, есть ли данные в метриках (строки кроме комментариев)
                const hasData = workerData.rawMetrics.match(/^[^#\s].*$/m);
                if (!hasData) {
                    logger.debug(`Worker ${workerId} не имеет данных метрик (только комментарии)`);
                    continue;
                }

                logger.debug(`Добавляем метрики от worker ${workerId}, размер: ${workerData.rawMetrics.length} символов`);
                
                const parsed = this.parseMetrics(workerData.rawMetrics);
                allMetrics.push(parsed);
            }
        }

        // Объединяем HELP и TYPE от всех worker'ов
        const combinedHelp = new Map();
        const combinedType = new Map();
        const combinedData = [];

        for (const metrics of allMetrics) {
            // Объединяем HELP
            for (const [metricName, helpText] of metrics.help) {
                if (!combinedHelp.has(metricName)) {
                    combinedHelp.set(metricName, helpText);
                }
            }

            // Объединяем TYPE
            for (const [metricName, typeName] of metrics.type) {
                if (!combinedType.has(metricName)) {
                    combinedType.set(metricName, typeName);
                }
            }

            // Объединяем данные метрик
            combinedData.push(...metrics.data);
        }

        // Группируем данные по базовому имени метрики
        const metricsByName = new Map();
        for (const dataLine of combinedData) {
            // Извлекаем имя метрики (первая часть до пробела или {)
            const metricNameMatch = dataLine.match(/^([^\s{]+)/);
            if (metricNameMatch) {
                const fullMetricName = metricNameMatch[1];
                // Используем базовое имя для группировки
                const baseMetricName = this.getBaseMetricName(fullMetricName);
                if (!metricsByName.has(baseMetricName)) {
                    metricsByName.set(baseMetricName, []);
                }
                metricsByName.get(baseMetricName).push(dataLine);
            }
        }

        // Формируем вывод в нужном формате
        let combinedMetrics = '';

        // Добавляем метрику активных worker'ов
        combinedMetrics += `# HELP active_workers Number of active workers\n`;
        combinedMetrics += `# TYPE active_workers gauge\n`;
        combinedMetrics += `active_workers ${this.workerMetrics.size}\n\n`;        
        
        // Выводим метрики отсортированными по имени
        const sortedMetricNames = Array.from(metricsByName.keys()).sort();
        
        for (const baseMetricName of sortedMetricNames) {
            // Выводим HELP, если есть (ищем по базовому имени)
            if (combinedHelp.has(baseMetricName)) {
                combinedMetrics += `# HELP ${baseMetricName} ${combinedHelp.get(baseMetricName)}\n`;
            }
            
            // Выводим TYPE, если есть (ищем по базовому имени)
            if (combinedType.has(baseMetricName)) {
                combinedMetrics += `# TYPE ${baseMetricName} ${combinedType.get(baseMetricName)}\n`;
            }
            
            // Выводим данные метрик
            for (const dataLine of metricsByName.get(baseMetricName)) {
                combinedMetrics += dataLine + '\n';
            }
            
            combinedMetrics += '\n';
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