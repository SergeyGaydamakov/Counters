// Импортируем систему логирования
const Logger = require('./utils/logger');
const { MongoProvider, FactController } = require('./index');

// Загружаем переменные окружения из .env файла
const dotenv = require('dotenv');
dotenv.config();

// Создаем глобальный логгер с уровнем из переменной окружения
const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Параметры подключения к MongoDB из .env
const connectionString = process.env.MONGODB_CONNECTION_STRING || 'mongodb://localhost:27017';
const databaseName = process.env.MONGODB_DATABASE_NAME || 'counters';

// Параметры генерации фактов из .env
const fieldConfigPath = process.env.FACT_FIELD_CONFIG_PATH || null;
const indexConfigPath = process.env.INDEX_CONFIG_PATH || null;
const targetSize = parseInt(process.env.FACT_TARGET_SIZE) || 500;

// Логируем загруженные параметры
logger.info('=== Загруженные параметры из .env ===');
logger.info('MongoDB Connection String:', connectionString);
logger.info('MongoDB Database Name:', databaseName);
logger.info('Field Config Path:', fieldConfigPath);
logger.info('Index Config Path:', indexConfigPath);
logger.info('Target Size:', targetSize);
logger.info('=====================================\n');

// Глобальная переменная для хранения провайдера
let mongoProvider = null;

// Функция для корректного завершения программы
async function gracefulShutdown(signal) {
    logger.info(`\n📡 Получен сигнал ${signal}. Завершение работы...`);
    
    if (mongoProvider) {
        try {
            await mongoProvider.disconnect();
            logger.info('✓ Соединение с MongoDB закрыто');
        } catch (error) {
            logger.error('✗ Ошибка при закрытии соединения с MongoDB:', error.message);
        }
    }
    
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

async function main(){
    function initProcessingTime(){
        return {
            total: {
                min: 1000000,
                max: 0,
                total: 0
            },
            relevantFacts: {
                min: 1000000,
                max: 0,
                total: 0
            },
            counters: {
                min: 1000000,
                max: 0,
                total: 0
            },
            saveFact: {
                min: 1000000,
                max: 0,
                total: 0
            },
            saveIndex: {
                min: 1000000,
                max: 0,
                total: 0
            },
            count: 0
        };
    }
    function updateProcessingTime(processingTime, result){
        processingTime.total.total += result.processingTime.total;
        processingTime.total.min = Math.min(processingTime.total.min, result.processingTime.total);
        processingTime.total.max = Math.max(processingTime.total.max, result.processingTime.total);
        processingTime.relevantFacts.total += result.processingTime.relevantFacts;
        processingTime.relevantFacts.min = Math.min(processingTime.relevantFacts.min, result.processingTime.relevantFacts);
        processingTime.relevantFacts.max = Math.max(processingTime.relevantFacts.max, result.processingTime.relevantFacts);
        processingTime.counters.total += result.processingTime.counters;
        processingTime.counters.min = Math.min(processingTime.counters.min, result.processingTime.counters);
        processingTime.counters.max = Math.max(processingTime.counters.max, result.processingTime.counters);
        processingTime.saveFact.total += result.processingTime.saveFact;
        processingTime.saveFact.min = Math.min(processingTime.saveFact.min, result.processingTime.saveFact);
        processingTime.saveFact.max = Math.max(processingTime.saveFact.max, result.processingTime.saveFact);
        processingTime.saveIndex.total += result.processingTime.saveIndex;
        processingTime.saveIndex.min = Math.min(processingTime.saveIndex.min, result.processingTime.saveIndex);
        processingTime.saveIndex.max = Math.max(processingTime.saveIndex.max, result.processingTime.saveIndex);
        processingTime.count++;
        return processingTime;
    }
    function printProcessingTime(processingTime){
        logger.info(`✓ Время обработки ${processingTime.count} фактов (avg / min / max): ${Math.round(processingTime.total.total/ processingTime.count)} мсек / ${processingTime.total.min} мсек / ${processingTime.total.max} мсек`);
        // logger.info(`✓ Время обработки релевантных фактов: ${Math.round(processingTime.relevantFacts.total/ processingTime.count)} миллисекунд`);
        logger.info(`✓ Время расчета счетчиков (avg / min / max): ${Math.round(processingTime.counters.total/ processingTime.count)} мсек / ${processingTime.counters.min} мсек / ${processingTime.counters.max} мсек`);
        logger.info(`✓ Время сохранения факта (avg / min / max): ${Math.round(processingTime.saveFact.total/ processingTime.count)} мсек / ${processingTime.saveFact.min} мсек / ${processingTime.saveFact.max} мсек`);
        logger.info(`✓ Время сохранения индекса (avg / min / max): ${Math.round(processingTime.saveIndex.total/ processingTime.count)} мсек / ${processingTime.saveIndex.min} мсек / ${processingTime.saveIndex.max} мсек`);
    }
    try {
        let factCount = 0;
        // Создаем провайдер данных
        mongoProvider = new MongoProvider(connectionString, databaseName);
        await mongoProvider.connect();
            
        // Создаем экземпляр контроллера с dbProvider
        const factController = new FactController(mongoProvider, fieldConfigPath, indexConfigPath, targetSize);
        const CYCLE_OUTPUT = 100;
        let startCycleTime = Date.now();
        let processingTime = initProcessingTime();
        // Функция с бесконечным циклом запуска run
        async function run(){
            const result = await factController.runWithCounters();
            // Подсчитываем минимальное, максимальное и среднее время обработки фактов
            processingTime = updateProcessingTime(processingTime, result);

            factCount++;
            if (factCount % CYCLE_OUTPUT === 0) {
                logger.info(`✓ Создано ${factCount} фактов`);
                logger.info(`✓ Скорость создания фактов: ${Math.round(CYCLE_OUTPUT / (Date.now() - startCycleTime) * 1000)} фактов в секунду`);
                logger.info(`✓ Время обработки 1 факта: ${Math.round((Date.now() - startCycleTime)/ CYCLE_OUTPUT)} миллисекунд`);
                logger.info("");
                printProcessingTime(processingTime);
                logger.info("");
                processingTime = initProcessingTime();
                startCycleTime = Date.now();
            }
            setTimeout(async () => {
                await run();
            }, 0);
        }
        
        // Запускаем контроллер
        await run();
        
    } catch (error) {
        logger.error('✗ Ошибка выполнения программы:', error.message);
        if (mongoProvider) {
            await mongoProvider.disconnect();
        }
        process.exit(1);
    }
};

// Запуск если файл выполняется напрямую
if (require.main === module) {
    main();
}
