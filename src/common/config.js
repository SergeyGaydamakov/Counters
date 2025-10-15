const dotenv = require('dotenv');
dotenv.config();

/**
 * Конфигурация Web сервиса
 */
const config = {
    // Порт сервера
    port: parseInt(process.env.WEB_PORT) || 3000,
    
    // Количество воркеров в кластере
    workers: parseInt(process.env.CLUSTER_WORKERS) || require('os').cpus().length,
    
    // Настройки фактов
    facts: {
        fieldConfigPath: process.env.MESSAGE_CONFIG_PATH || null,
        indexConfigPath: process.env.INDEX_CONFIG_PATH || null,
        counterConfigPath: process.env.COUNTER_CONFIG_PATH || null,
        targetSize: parseInt(process.env.FACT_TARGET_SIZE) || 500
    },
    
    // CORS настройки
    cors: {
        origin: process.env.CORS_ORIGIN || '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
    },
    
    // Rate limiting
    rateLimit: {
        windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS) || 15 * 60 * 1000, // 15 минут
        max: parseInt(process.env.RATE_LIMIT_MAX) || 1000, // максимум запросов за окно
        message: {
            error: 'Слишком много запросов с этого IP, попробуйте позже',
            retryAfter: '15 минут'
        }
    },
    
    // Лимиты запросов
    limits: {
        json: process.env.JSON_LIMIT || '10mb',
        urlencoded: process.env.URL_LIMIT || '10mb'
    },
    
    // Логирование
    logging: {
        enableRequestLogging: process.env.ENABLE_REQUEST_LOGGING !== 'false'
    },
    
    // Мониторинг
    monitoring: {
        healthCheckInterval: parseInt(process.env.HEALTH_CHECK_INTERVAL) || 30000, // 30 секунд
        enableMetrics: process.env.ENABLE_METRICS !== 'false'
    },

    // Строка подключения к MongoDB
    database: {
        connectionString: process.env.MONGODB_CONNECTION_STRING || 'mongodb://localhost:27017',
        databaseName: process.env.MONGODB_DATABASE_NAME || 'counters'
    },
    
    // Фильтрация типов сообщений
    messageTypes: {
        // Список разрешенных типов сообщений (через запятую)
        // Если не задан, обрабатываются все типы
        allowedTypes: process.env.ALLOWED_MESSAGE_TYPES ? 
            (() => {
                const types = process.env.ALLOWED_MESSAGE_TYPES.split(',').map(t => parseInt(t.trim())).filter(t => !isNaN(t));
                return types.length > 0 ? types : null;
            })() : 
            null
    },
};

module.exports = config;
