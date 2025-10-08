const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const Logger = require('../utils/logger');
const { MongoProvider, FactController } = require('../index');
const config = require('../utils/config');
const { createRoutes } = require('./routes');
const { 
    requestLogger, 
    jsonValidator, 
    errorHandler, 
    notFoundHandler, 
    responseMetadata 
} = require('./middleware');

// Загружаем переменные окружения
const dotenv = require('dotenv');
dotenv.config();

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
const app = express();

// Глобальная переменная для хранения провайдера и контроллера
let mongoProvider = null;
let factController = null;

// Middleware для безопасности
app.use(helmet({
    contentSecurityPolicy: false, // Отключаем CSP для API
    crossOriginEmbedderPolicy: false
}));

// CORS настройки
app.use(cors(config.cors));

// Сжатие ответов
app.use(compression());

// Rate limiting
const limiter = rateLimit(config.rateLimit);
app.use('/api/', limiter);

// Парсинг JSON с обработкой ошибок
app.use(express.json({ 
    limit: config.limits.json
}));

// Middleware для обработки ошибок парсинга JSON
app.use((error, req, res, next) => {
    if (error instanceof SyntaxError && error.status === 400 && 'body' in error) {
        logger.warn('JSON parse error:', error.message);
        return res.status(400).json({
            success: false,
            error: 'Неверный JSON формат',
            message: error.message,
            timestamp: new Date().toISOString(),
            worker: process.pid
        });
    }
    next(error);
});

// Парсинг URL-encoded данных
app.use(express.urlencoded({ 
    extended: true, 
    limit: config.limits.urlencoded 
}));

// Middleware для добавления метаданных к ответу
app.use(responseMetadata);

// Middleware для логирования запросов
if (config.logging.enableRequestLogging) {
    app.use(requestLogger);
}

// Middleware для валидации JSON
app.use(jsonValidator);

// Функция инициализации
async function initialize() {
    try {
        logger.info(`🔌 Воркер ${process.pid} инициализируется...`);
        logger.info(`📊 MongoDB: ${config.database.connectionString}/${config.database.databaseName}`);

        // Создаем провайдер данных
        mongoProvider = new MongoProvider(
            config.database.connectionString, 
            config.database.databaseName
        );
        await mongoProvider.connect();
        logger.info(`✅ MongoDB подключен в воркере ${process.pid}`);

        // Создаем контроллер фактов
        factController = new FactController(
            mongoProvider, 
            config.facts.fieldConfigPath, 
            config.facts.indexConfigPath, 
            config.facts.targetSize
        );
        logger.info(`✅ FactController инициализирован в воркере ${process.pid}`);

        // Подключаем API маршруты с инициализированным контроллером
        app.use(createRoutes(factController));

        // 404 handler
        app.use(notFoundHandler);

        // Error handler
        app.use(errorHandler);

        // Запускаем сервер
        const server = app.listen(config.port, () => {
            logger.info(`🚀 Воркер ${process.pid} запущен на порту ${config.port}`);
        });

        // Graceful shutdown
        const gracefulShutdown = async (signal) => {
            logger.info(`📡 Воркер ${process.pid} получил сигнал ${signal}, завершаю работу...`);
            
            server.close(async () => {
                if (mongoProvider) {
                    await mongoProvider.disconnect();
                    logger.info(`✅ MongoDB отключен в воркере ${process.pid}`);
                }
                logger.info(`✅ Воркер ${process.pid} завершен`);
                process.exit(0);
            });
        };

        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
        process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    } catch (error) {
        logger.error(`❌ Ошибка инициализации воркера ${process.pid}:`, error);
        console.error('Полная ошибка:', error);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    }
}

// Запуск если файл выполняется напрямую
if (require.main === module) {
    initialize();
}
