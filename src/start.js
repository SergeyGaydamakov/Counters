// Импортируем систему логирования
const Logger = require('./logger');
const { MongoProvider, FactService, CounterProducer } = require('./index');
const config = require('./config');

// Загружаем переменные окружения из .env файла
const dotenv = require('dotenv');
dotenv.config();

// Создаем глобальный логгер с уровнем из переменной окружения
const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Логируем загруженные параметры
logger.info('=== Загруженные параметры из .env ===');
logger.info('MongoDB Connection String:', config.database.connectionString);
logger.info('MongoDB Database Name:', config.database.databaseName);
logger.info('Field Config Path:', config.facts.fieldConfigPath);
logger.info('Index Config Path:', config.facts.indexConfigPath);
logger.info('Target Size:', config.facts.targetSize);
logger.info('Counter Config Path:', config.facts.counterConfigPath);
logger.info('Include Fact Data To Index:', config.facts.includeFactDataToIndex);
logger.info('Lookup Facts:', config.facts.lookupFacts);
logger.info('Index Bulk Update:', config.facts.indexBulkUpdate);
logger.info('Max Depth Limit:', config.facts.maxDepthLimit);
logger.info('Max Counters Processing:', config.facts.maxCountersProcessing);
logger.info('Max Counters Per Request:', config.facts.maxCountersPerRequest);
logger.info('Allowed Counters Names:', config.facts.allowedCountersNames);

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
    function updateProcessingTime(processingTime, resultProcessingTime){
        if (!resultProcessingTime) {
            return processingTime;
        }
        processingTime.total.total += resultProcessingTime.total;
        processingTime.total.min = Math.min(processingTime.total.min, resultProcessingTime.total);
        processingTime.total.max = Math.max(processingTime.total.max, resultProcessingTime.total);
        processingTime.relevantFacts.total += resultProcessingTime.relevantFacts;
        processingTime.relevantFacts.min = Math.min(processingTime.relevantFacts.min, resultProcessingTime.relevantFacts);
        processingTime.relevantFacts.max = Math.max(processingTime.relevantFacts.max, resultProcessingTime.relevantFacts);
        processingTime.counters.total += resultProcessingTime.counters;
        processingTime.counters.min = Math.min(processingTime.counters.min, resultProcessingTime.counters);
        processingTime.counters.max = Math.max(processingTime.counters.max, resultProcessingTime.counters);
        processingTime.saveFact.total += resultProcessingTime.saveFact;
        processingTime.saveFact.min = Math.min(processingTime.saveFact.min, resultProcessingTime.saveFact);
        processingTime.saveFact.max = Math.max(processingTime.saveFact.max, resultProcessingTime.saveFact);
        processingTime.saveIndex.total += resultProcessingTime.saveIndex;
        processingTime.saveIndex.min = Math.min(processingTime.saveIndex.min, resultProcessingTime.saveIndex);
        processingTime.saveIndex.max = Math.max(processingTime.saveIndex.max, resultProcessingTime.saveIndex);
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
        const mongoCounters = new CounterProducer(config.facts.counterConfigPath, config.facts.useShortNames, config.facts.fieldConfigPath);
        // Создаем провайдер данных
        mongoProvider = new MongoProvider(config.database.connectionString, config.database.databaseName, config.database.options, mongoCounters, config.facts.includeFactDataToIndex, config.facts.lookupFacts, config.facts.indexBulkUpdate);
        await mongoProvider.connect();
            
        // Создаем экземпляр сервиса с dbProvider
        const factService = new FactService(mongoProvider, config.facts.fieldConfigPath, config.facts.indexConfigPath, config.facts.targetSize, config.facts.includeFactDataToIndex, config.facts.maxDepthLimit);
        const CYCLE_OUTPUT = 100;
        let startCycleTime = Date.now();
        let processingTime = initProcessingTime();
        // Функция с бесконечным циклом запуска run
        async function run(){
            const result = await factService.runWithCounters();
            // Подсчитываем минимальное, максимальное и среднее время обработки фактов
            processingTime = updateProcessingTime(processingTime, result.processingTime);

            if (result.processingTime) {
                // Если время обработки не null, то увеличиваем счетчик фактов
                factCount++;
            }
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
