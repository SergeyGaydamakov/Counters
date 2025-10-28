const dotenv = require('dotenv');
dotenv.config();

/**
 * Безопасный парсинг JSON из переменной окружения
 * @param {string} envVar - значение переменной окружения
 * @param {*} defaultValue - значение по умолчанию
 * @param {string} varName - имя переменной для логирования ошибок
 * @returns {*} - распарсенное значение или значение по умолчанию
 */
function safeJsonParse(envVar, defaultValue, varName) {
    if (!envVar) {
        return defaultValue;
    }
    
    try {
        return JSON.parse(envVar.trim());
    } catch (error) {
        console.error(`Ошибка парсинга JSON для переменной ${varName}:`, error.message);
        console.error(`Некорректное значение: ${envVar}`);
        console.error(`Используется значение по умолчанию:`, defaultValue);
        
        // Дополнительная диагностика для типичных ошибок JSON
        if (error.message.includes('Expected property name')) {
            console.error(`💡 Подсказка: В JSON все ключи должны быть в кавычках.`);
            console.error(`   Пример: {"key": "value"} вместо {key: "value"}`);
        }
        
        return defaultValue;
    }
}

/**
 * Конфигурация Web сервиса
 */
const config = {
    //
    isDevelopment: process.env.NODE_ENV === "development",
    // Порт сервера
    port: parseInt(process.env.WEB_PORT) || 3000,
    
    // Количество воркеров в кластере
    workers: parseInt(process.env.CLUSTER_WORKERS) || require('os').cpus().length,
    
    // Настройки фактов
    facts: {
        fieldConfigPath: process.env.MESSAGE_CONFIG_PATH || null,
        indexConfigPath: process.env.INDEX_CONFIG_PATH || null,
        counterConfigPath: process.env.COUNTER_CONFIG_PATH || null,
        targetSize: parseInt(process.env.FACT_TARGET_SIZE) || 500,
        includeFactDataToIndex: process.env.INCLUDE_FACT_DATA_TO_INDEX === 'true',
        lookupFacts: process.env.LOOKUP_FACTS === 'true',
        indexBulkUpdate: process.env.INDEX_BULK_UPDATE === 'true',
        maxDepthLimit: parseInt(process.env.MAX_DEPTH_LIMIT) || 500,
        maxCountersProcessing: parseInt(process.env.MAX_COUNTERS_PROCESSING) || 0,
        maxCountersPerRequest: parseInt(process.env.MAX_COUNTERS_PER_REQUEST) || 0,
        allowedCountersNames: process.env.ALLOWED_COUNTERS_NAMES ? process.env.ALLOWED_COUNTERS_NAMES.split(',').map(t => t.trim()).filter(t => t !== '') : null,
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
        enableRequestLogging: process.env.ENABLE_REQUEST_LOGGING !== 'false',
        saveFrequency: parseInt(process.env.LOG_SAVE_FREQUENCY) || 100,
        debugMode: process.env.DEBUG_MODE === 'true',
        logLevel: process.env.LOG_LEVEL || 'INFO',
        writeErrorsToFile: process.env.WRITE_ERRORS_TO_FILE === 'true'
    },
    
    // Строка подключения к MongoDB
    database: {
        connectionString: process.env.MONGODB_CONNECTION_STRING || 'mongodb://localhost:27017',
        databaseName: process.env.MONGODB_DATABASE_NAME || 'counters',
        options: {
            individualProcessClient: process.env.INDIVIDUAL_PROCESS_CLIENT === 'true' || false,
            individualCollectionObject: process.env.INDIVIDUAL_COLLECTION_OBJECT === 'true' || false,
            disableSave: process.env.DISABLE_SAVE === 'true' || false,
            writeConcern: safeJsonParse(process.env.MONGODB_WRITE_CONCERN, { w: 1, j: false, wtimeout: 5000 }, 'MONGODB_WRITE_CONCERN'),
            readConcern: safeJsonParse(process.env.MONGODB_READ_CONCERN, { level: "local" }, 'MONGODB_READ_CONCERN'),
            aggregateReadPreference: safeJsonParse(process.env.MONGODB_AGGREGATE_READ_PREFERENCE, { "mode": "secondaryPreferred" }, 'MONGODB_AGGREGATE_READ_PREFERENCE'),
            minPoolSize: parseInt(process.env.MONGODB_MIN_POOL_SIZE) || 10,
            maxPoolSize: parseInt(process.env.MONGODB_MAX_POOL_SIZE) || 100,
            maxConnecting: parseInt(process.env.MONGODB_MAX_CONNECTING) || 10,
        },
        batchSize: parseInt(process.env.MONGODB_BATCH_SIZE) || 5000,
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
            null,
        
        // Коэффициент уменьшения трафика IRIS запросов (по умолчанию 1 - обрабатывать все)
        irisTrafficReductionFactor: parseInt(process.env.IRIS_TRAFFIC_REDUCTION_FACTOR) || 1
    },
};

module.exports = config;
