// Импортируем систему логирования
const Logger = require('./utils/logger');
const axios = require('axios');
const MessageGenerator = require('./generators/messageGenerator');

// Загружаем переменные окружения из .env файла
const dotenv = require('dotenv');
dotenv.config();

// Создаем глобальный логгер с уровнем из переменной окружения
const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Параметры подключения к сервису из .env
const serviceHost = process.env.SERVICE_HOST || 'http://localhost:3000';
const messageConfigPath = process.env.MESSAGE_CONFIG_PATH || 'messageConfig.json';

// Инициализируем MessageGenerator для получения доступных типов сообщений
let messageGenerator = null;
let availableMessageTypes = [];

// Функция для инициализации MessageGenerator
function initializeMessageGenerator() {
    try {
        messageGenerator = new MessageGenerator(messageConfigPath);
        availableMessageTypes = messageGenerator.getAvailableTypes();
        logger.info('✓ MessageGenerator успешно инициализирован');
        logger.info(`✓ Загружено ${availableMessageTypes.length} типов сообщений из ${messageConfigPath}`);
    } catch (error) {
        logger.error('✗ Ошибка инициализации MessageGenerator:', error.message);
        logger.error('✗ Используются типы сообщений по умолчанию: 1-10');
        availableMessageTypes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    }
}

// Логируем загруженные параметры
logger.info('=== Загруженные параметры из .env ===');
logger.info('Service Host:', serviceHost);
logger.info('Message Config Path:', messageConfigPath);
logger.info('=====================================\n');

// Функция для корректного завершения программы
async function gracefulShutdown(signal) {
    logger.info(`\n📡 Получен сигнал ${signal}. Завершение работы...`);
    logger.info('✓ Программа завершена');
    process.exit(0);
}

// Обработчики событий завершения программы
process.on('SIGINT', () => gracefulShutdown('SIGINT'));   // Ctrl+C
process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // kill команда
process.on('SIGUSR2', () => gracefulShutdown('SIGUSR2')); // nodemon restart
process.on('uncaughtException', (error) => {
    logger.error('✗ Необработанная ошибка:', error);
    gracefulShutdown('uncaughtException');
});
process.on('unhandledRejection', (reason, promise) => {
    logger.error('✗ Необработанное отклонение Promise:', reason);
    gracefulShutdown('unhandledRejection');
});

async function main() {
    function initProcessingTime() {
        return {
            total: {
                min: 1000000,
                max: 0,
                total: 0
            },
            generateMessage: {
                min: 1000000,
                max: 0,
                total: 0
            },
            processMessage: {
                min: 1000000,
                max: 0,
                total: 0
            },
            count: 0,
            errors: 0
        };
    }

    function updateProcessingTime(processingTime, resultProcessingTime) {
        if (!resultProcessingTime) {
            return processingTime;
        }
        
        processingTime.total.total += resultProcessingTime.total;
        processingTime.total.min = Math.min(processingTime.total.min, resultProcessingTime.total);
        processingTime.total.max = Math.max(processingTime.total.max, resultProcessingTime.total);
        
        processingTime.generateMessage.total += resultProcessingTime.generateMessage;
        processingTime.generateMessage.min = Math.min(processingTime.generateMessage.min, resultProcessingTime.generateMessage);
        processingTime.generateMessage.max = Math.max(processingTime.generateMessage.max, resultProcessingTime.generateMessage);
        
        processingTime.processMessage.total += resultProcessingTime.processMessage;
        processingTime.processMessage.min = Math.min(processingTime.processMessage.min, resultProcessingTime.processMessage);
        processingTime.processMessage.max = Math.max(processingTime.processMessage.max, resultProcessingTime.processMessage);
        
        processingTime.count++;
        return processingTime;
    }

    function printProcessingTime(processingTime) {
        logger.info(`✓ Время обработки ${processingTime.count} запросов (avg / min / max): ${Math.round(processingTime.total.total / processingTime.count)} мсек / ${processingTime.total.min} мсек / ${processingTime.total.max} мсек`);
        logger.info(`✓ Время генерации сообщения (avg / min / max): ${Math.round(processingTime.generateMessage.total / processingTime.count)} мсек / ${processingTime.generateMessage.min} мсек / ${processingTime.generateMessage.max} мсек`);
        logger.info(`✓ Время обработки сообщения (avg / min / max): ${Math.round(processingTime.processMessage.total / processingTime.count)} мсек / ${processingTime.processMessage.min} мсек / ${processingTime.processMessage.max} мсек`);
        if (processingTime.errors > 0) {
            logger.info(`✓ Количество ошибок: ${processingTime.errors}`);
        }
    }

    // Функция для выбора случайного типа сообщения
    function getRandomMessageType() {
        if (availableMessageTypes.length === 0) {
            logger.error('✗ Нет доступных типов сообщений');
            return 1; // Fallback к типу 1
        }
        return availableMessageTypes[Math.floor(Math.random() * availableMessageTypes.length)];
    }

    // Функция для генерации сообщения через API
    async function generateMessage(messageType) {
        const startTime = Date.now();
        try {
            const response = await axios.get(`${serviceHost}/api/v1/message/${messageType}/json`, {
                timeout: 10000 // 10 секунд таймаут
            });
            const endTime = Date.now();
            return {
                success: true,
                data: response.data,
                time: endTime - startTime
            };
        } catch (error) {
            const endTime = Date.now();
            logger.error(`Ошибка генерации сообщения типа ${messageType}:`, error.message);
            return {
                success: false,
                error: error.message,
                time: endTime - startTime
            };
        }
    }

    // Функция для обработки сообщения через API
    async function processMessage(messageType, messageData) {
        const startTime = Date.now();
        try {
            const response = await axios.post(`${serviceHost}/api/v1/message/${messageType}/json`, messageData, {
                headers: {
                    'Content-Type': 'application/json'
                },
                timeout: 30000 // 30 секунд таймаут для обработки
            });
            const endTime = Date.now();
            return {
                success: true,
                data: response.data,
                time: endTime - startTime
            };
        } catch (error) {
            const endTime = Date.now();
            logger.error(`Ошибка обработки сообщения типа ${messageType}:`, error.message);
            return {
                success: false,
                error: error.message,
                time: endTime - startTime
            };
        }
    }

    // Функция для выполнения одного цикла тестирования
    async function runTestCycle() {
        const cycleStartTime = Date.now();
        const messageType = getRandomMessageType();
        
        // 1. Генерируем сообщение
        const generateResult = await generateMessage(messageType);
        if (!generateResult.success) {
            return {
                success: false,
                error: `Ошибка генерации: ${generateResult.error}`,
                processingTime: {
                    total: Date.now() - cycleStartTime,
                    generateMessage: generateResult.time,
                    processMessage: 0
                }
            };
        }

        // 2. Обрабатываем сгенерированное сообщение
        const processResult = await processMessage(messageType, generateResult.data);
        const totalTime = Date.now() - cycleStartTime;

        return {
            success: processResult.success,
            messageType: messageType,
            factId: processResult.success ? processResult.data.factId : null,
            processingTime: {
                total: totalTime,
                generateMessage: generateResult.time,
                processMessage: processResult.time
            },
            error: processResult.success ? null : processResult.error
        };
    }

    try {
        // Инициализируем MessageGenerator
        initializeMessageGenerator();
        
        logger.info('=== Инициализация завершена ===');
        logger.info('Available Message Types:', availableMessageTypes.join(', '));
        logger.info('================================\n');

        let requestCount = 0;
        const CYCLE_OUTPUT = 100;
        let startCycleTime = Date.now();
        let processingTime = initProcessingTime();

        // Функция с бесконечным циклом запуска тестов
        async function run() {
            const result = await runTestCycle();
            
            // Обновляем метрики
            processingTime = updateProcessingTime(processingTime, result.processingTime);
            
            if (!result.success) {
                processingTime.errors++;
            }

            // Если время обработки не null, то увеличиваем счетчик запросов
            if (result.processingTime) {
                requestCount++;
            }

            // Выводим статистику каждые CYCLE_OUTPUT запросов
            if (requestCount % CYCLE_OUTPUT === 0) {
                logger.info(`✓ Выполнено ${requestCount} тестовых запросов`);
                logger.info(`✓ Скорость выполнения: ${Math.round(CYCLE_OUTPUT / (Date.now() - startCycleTime) * 1000)} запросов в секунду`);
                logger.info(`✓ Время выполнения 1 запроса: ${Math.round((Date.now() - startCycleTime) / CYCLE_OUTPUT)} миллисекунд`);
                logger.info("");
                printProcessingTime(processingTime);
                logger.info("");
                
                // Сбрасываем метрики для следующего цикла
                processingTime = initProcessingTime();
                startCycleTime = Date.now();
            }

            // Следующий запрос
            setTimeout(async () => {
                await run();
            }, 0); 
        }

        logger.info('🚀 Запуск тестирования сервиса...');
        logger.info(`📡 Подключение к сервису: ${serviceHost}`);
        logger.info(`📊 Типы сообщений для тестирования: ${availableMessageTypes.join(', ')}`);
        logger.info(`📁 Конфигурация сообщений: ${messageConfigPath}`);
        logger.info('');

        // Запускаем тестирование
        await run();

    } catch (error) {
        logger.error('✗ Ошибка выполнения программы:', error.message);
        process.exit(1);
    }
}

// Запуск если файл выполняется напрямую
if (require.main === module) {
    main();
}

module.exports = { main };
