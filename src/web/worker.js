const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const Logger = require('../utils/logger');
const { MongoProvider, FactController, CounterProducer } = require('../index');
const config = require('../common/config');
const { createRoutes } = require('./routes');
const { 
    requestLogger, 
    jsonValidator, 
    irisXmlParser,
    errorHandler, 
    notFoundHandler, 
    responseMetadata 
} = require('./middleware');
const Diagnostics = require('../utils/diagnostics');
const { initializeMetricsCollector, getMetricsCollector, destroyMetricsCollector } = require('../common/metrics');

// Загружаем переменные окружения
const dotenv = require('dotenv');
dotenv.config();

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
const app = express();

// Логируем запуск воркера
logger.info(`🚀 Воркер ${process.pid} загружается...`);
logger.info(`📋 Режим запуска: ${require.main === module ? 'прямой' : 'кластер'}`);

// Глобальные обработчики ошибок для воркера
process.on('uncaughtException', (error) => {
    logger.error(`❌ Необработанная ошибка в воркере ${process.pid}:`, error.message);
    logger.error(`📋 Детали:`, error);
    logger.error(`🔧 Воркер будет завершен для предотвращения нестабильной работы`);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error(`❌ Необработанное отклонение Promise в воркере ${process.pid}:`, reason);
    logger.error(`📋 Promise:`, promise);
    logger.error(`🔧 Проверьте асинхронные операции и обработку ошибок`);
});

// Обработка предупреждений
process.on('warning', (warning) => {
    logger.warn(`⚠️  Предупреждение в воркере ${process.pid}:`, warning.message);
    logger.warn(`📋 Детали:`, warning);
});

// Переменные для хранения экземпляров провайдера и контроллера
// Каждый Worker имеет свои собственные экземпляры для изоляции
let mongoProvider = null;
let factController = null;
let mongoCounters = null;

// Переменные для управления задержкой запуска (прогрев сервиса)
let serverStartTime = null; // Время запуска сервера
let firstRequestProcessed = false; // Флаг первого запроса (для прогрева)

// Middleware для безопасности
app.use(helmet({
    contentSecurityPolicy: false, // Отключаем CSP для API
    crossOriginEmbedderPolicy: false
}));

// CORS настройки
app.use(cors(config.cors));

// Сжатие ответов
app.use(compression());

// Middleware для задержки запуска (прогрев сервиса)
// Размещен до парсинга данных, чтобы снизить нагрузку на сервис
// Пропускает первый запрос для прогрева, затем блокирует запросы на config.startDelay секунд
app.use((req, res, next) => {
    // Если config.startDelay == 0, пропускаем все запросы
    if (config.startDelay === 0) {
        return next();
    }

    // Если serverStartTime еще не установлен (не должно происходить, но на всякий случай)
    if (serverStartTime === null) {
        logger.warn(`⚠️ Воркер ${process.pid}: serverStartTime не установлен, пропускаю запрос`);
        return next();
    }

    const currentTime = Date.now();
    const elapsedSeconds = (currentTime - serverStartTime) / 1000;

    // Пропускаем первый запрос для прогрева
    if (!firstRequestProcessed) {
        firstRequestProcessed = true;
        logger.info(`🔥 Воркер ${process.pid}: пропускаю первый запрос для прогрева (${req.method} ${req.path})`);
        return next();
    }

    // Если прошло меньше config.startDelay секунд, блокируем запрос
    if (elapsedSeconds < config.startDelay) {
        const remainingSeconds = (config.startDelay - elapsedSeconds).toFixed(2);
        logger.info(`⏳ Воркер ${process.pid}: запрос заблокирован (осталось ${remainingSeconds} сек): ${req.method} ${req.path}`);
        return res.status(503).json({
            success: false,
            error: 'Сервис прогревается',
            message: `Сервис еще не готов. Осталось ${remainingSeconds} секунд.`,
            timestamp: new Date().toISOString(),
            worker: process.pid,
            retryAfter: Math.ceil(config.startDelay - elapsedSeconds)
        });
    }

    // Время задержки истекло, пропускаем все запросы
    next();
});

// Rate limiting
// const limiter = rateLimit(config.rateLimit);
// app.use('/api/', limiter);

// Middleware для парсинга XML для IRIS маршрутов (ДО express.json)
app.use(irisXmlParser);

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
// app.use(responseMetadata);

// Middleware для логирования запросов
if (config.logging.enableRequestLogging) {
    app.use(requestLogger);
}

// Middleware для валидации JSON
app.use(jsonValidator);

// Функция инициализации
// Каждый Worker создает свои собственные экземпляры для полной изоляции
async function initialize() {
    try {
        logger.info(`🔌 Воркер ${process.pid} инициализируется...`);
        logger.info(`📊 MongoDB: ${config.database.connectionString}/${config.database.databaseName}`);
        logger.info(`🌐 Порт: ${config.port}`);
        logger.info(`📁 Конфигурационные файлы:`);
        logger.info(`   - Счетчики: ${config.facts.counterConfigPath || 'не указан'}`);
        logger.info(`   - Поля: ${config.facts.fieldConfigPath || 'не указан'}`);
        logger.info(`   - Индексы: ${config.facts.indexConfigPath || 'не указан'}`);
        logger.info(`📊 Настройки индексирования:`);
        logger.info(`   - Включать данные факта в индекс: ${config.facts.includeFactDataToIndex}`);
        
        // Выводим информацию о задержке запуска
        if (config.startDelay > 0) {
            logger.info(`⏳ Задержка запуска: ${config.startDelay} секунд (первый запрос будет пропущен для прогрева)`);
        } else {
            logger.info(`⏳ Задержка запуска: отключена (config.startDelay=0)`);
        }
        
        // Выводим информацию о разрешенных типах сообщений
        logger.info(`📨 Обрабатываемые типы сообщений:`);
        if (config.messageTypes.allowedTypes && config.messageTypes.allowedTypes.length > 0) {
            logger.info(`   - Разрешенные типы: ${config.messageTypes.allowedTypes.join(', ')}`);
            logger.info(`   - Всего типов: ${config.messageTypes.allowedTypes.length}`);
        } else {
            logger.info(`   - Все типы сообщений разрешены (фильтрация отключена)`);
        }

        // Запускаем диагностику системы
        const diagnostics = new Diagnostics(logger);
        const diagnosticResults = await diagnostics.runFullDiagnostics(config);
        diagnostics.logDiagnostics(diagnosticResults);

        // Проверяем критические ошибки
        const criticalErrors = diagnosticResults.recommendations.filter(rec => rec.type === 'error');
        if (criticalErrors.length > 0) {
            logger.error(`❌ Обнаружены критические ошибки, инициализация прервана:`);
            criticalErrors.forEach(error => {
                logger.error(`   - ${error.message}: ${error.details}`);
            });
            throw new Error(`Критические ошибки конфигурации: ${criticalErrors.map(e => e.message).join(', ')}`);
        }

        // Создаем экземпляр счетчиков для этого Worker'а
        logger.info(`🔧 Создаю экземпляр счетчиков...`);
        mongoCounters = new CounterProducer(config.facts.counterConfigPath, config.facts.useShortNames, config.facts.fieldConfigPath);
        
        // Создаем собственный экземпляр провайдера данных для этого Worker'а
        // Это обеспечивает полную изоляцию между Worker'ами
        logger.info(`🔧 Подключаюсь к MongoDB...`);
        mongoProvider = new MongoProvider(
            config.database.connectionString, 
            config.database.databaseName,
            config.database.options,
            mongoCounters,
            config.facts.includeFactDataToIndex,
            config.facts.lookupFacts,
            config.facts.indexBulkUpdate
        );
        
        // Добавляем таймаут для подключения к MongoDB
        const connectPromise = mongoProvider.connect();
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Таймаут подключения к MongoDB (30 секунд)')), 30000);
        });
        
        await Promise.race([connectPromise, timeoutPromise]);
        logger.info(`✅ MongoDB подключен в воркере ${process.pid}`);

        // Инициализируем коллектор метрик перед настройкой мониторинга connection pool
        logger.info(`🔧 Инициализирую коллектор метрик...`);
        initializeMetricsCollector(`worker-${process.pid}`);
        const metricsCollector = getMetricsCollector();
        
        // Устанавливаем metricsCollector для мониторинга connection pool
        if (metricsCollector && mongoProvider && typeof mongoProvider.setMetricsCollector === 'function') {
            mongoProvider.setMetricsCollector(metricsCollector);
            logger.info(`✅ Мониторинг connection pool настроен в воркере ${process.pid}`);
        }

        // Создаем собственный экземпляр контроллера фактов для этого Worker'а
        logger.info(`🔧 Инициализирую FactController...`);
        factController = new FactController(
            mongoProvider, 
            config.facts.fieldConfigPath, 
            config.facts.indexConfigPath, 
            config.facts.targetSize,
            config.facts.includeFactDataToIndex,
            config.facts.maxDepthLimit
        );
        logger.info(`✅ FactController инициализирован в воркере ${process.pid}`);

        // Подключаем API маршруты с инициализированным контроллером
        logger.info(`🔧 Настраиваю API маршруты...`);
        app.use(createRoutes(factController));

        // 404 handler
        app.use(notFoundHandler);

        // Error handler
        app.use(errorHandler);

        // Запускаем сервер
        logger.info(`🔧 Запускаю HTTP сервер на порту ${config.port}...`);
        
        // Устанавливаем время запуска сервера ДО вызова listen, чтобы избежать гонки
        // Это гарантирует, что serverStartTime будет установлен до того, как сервер начнет принимать запросы
        serverStartTime = Date.now();
        firstRequestProcessed = false; // Сбрасываем флаг первого запроса
        
        const server = app.listen(config.port, () => {
            logger.info(`🚀 Воркер ${process.pid} запущен на порту ${config.port}`);
            logger.info(`🌐 API доступно по адресу: http://localhost:${config.port}/api/v1/health`);
            
            if (config.startDelay > 0) {
                logger.info(`⏳ Режим прогрева активен: первый запрос будет пропущен, затем ${config.startDelay} сек задержки`);
                logger.info(`⏳ serverStartTime установлен: ${new Date(serverStartTime).toISOString()}`);
            } else {
                logger.info(`⏳ Задержка запуска отключена (config.startDelay=${config.startDelay})`);
            }
            
            // Отправляем сообщение мастеру о готовности
            if (process.send) {
                process.send({ type: 'worker-ready', pid: process.pid, port: config.port });
            }
        });

        // Обработка ошибок сервера
        server.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                logger.error(`❌ Порт ${config.port} уже используется другим процессом`);
                logger.error(`🔧 Проверьте запущенные процессы: netstat -an | findstr :${config.port}`);
            } else {
                logger.error(`❌ Ошибка HTTP сервера:`, err.message);
            }
            process.exit(1);
        });

        // Graceful shutdown
        // Каждый Worker завершает работу со своим собственным экземпляром MongoProvider
        const gracefulShutdown = async (signal) => {
            logger.info(`📡 Воркер ${process.pid} получил сигнал ${signal}, завершаю работу...`);
            
            server.close(async () => {
                if (mongoProvider) {
                    try {
                        await mongoProvider.disconnect();
                        logger.info(`✅ MongoDB отключен в воркере ${process.pid}`);
                    } catch (error) {
                        logger.error(`❌ Ошибка при отключении от MongoDB:`, error.message);
                    }
                }
                
                // Завершаем коллектор метрик
                try {
                    destroyMetricsCollector();
                    logger.info(`✅ Коллектор метрик завершен в воркере ${process.pid}`);
                } catch (error) {
                    logger.error(`❌ Ошибка при завершении коллектора метрик:`, error.message);
                }
                
                logger.info(`✅ Воркер ${process.pid} завершен`);
                process.exit(0);
            });
        };

        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
        process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    } catch (error) {
        logger.error(`❌ Ошибка инициализации воркера ${process.pid}:`, error.message);
        logger.error(`📋 Детали ошибки:`);
        logger.error(`   - Тип: ${error.constructor.name}`);
        logger.error(`   - Сообщение: ${error.message}`);
        logger.error(`   - Код: ${error.code || 'не указан'}`);
        
        // Отправляем сообщение мастеру об ошибке
        if (process.send) {
            process.send({ 
                type: 'worker-error', 
                pid: process.pid, 
                error: error.message,
                code: error.code
            });
        }
        
        if (error.code === 'ECONNREFUSED') {
            logger.error(`🔧 MongoDB недоступен. Проверьте:`);
            logger.error(`   1. Запущен ли MongoDB: netstat -an | findstr :27020`);
            logger.error(`   2. Правильность строки подключения: ${config.database.connectionString}`);
            logger.error(`   3. Доступность хоста и порта MongoDB`);
        } else if (error.code === 'EADDRINUSE') {
            logger.error(`🔧 Порт ${config.port} уже используется. Проверьте:`);
            logger.error(`   1. Запущенные процессы: netstat -an | findstr :${config.port}`);
            logger.error(`   2. Остановите конфликтующие процессы`);
        } else if (error.message.includes('не найден')) {
            logger.error(`🔧 Конфигурационные файлы недоступны. Проверьте:`);
            logger.error(`   1. Существование файлов конфигурации`);
            logger.error(`   2. Правильность путей в переменных окружения`);
            logger.error(`   3. Права доступа к файлам`);
        }
        
        console.error('Полная ошибка:', error);
        console.error('Stack trace:', error.stack);
        process.exit(1);
    }
}

// Запуск если файл выполняется напрямую
if (require.main === module) {
    initialize();
} else {
    // Если файл загружается как модуль (в кластере), также запускаем инициализацию
    initialize();
}
