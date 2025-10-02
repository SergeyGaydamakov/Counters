// Импортируем систему логирования
const Logger = require('./utils/logger');
const { MongoProvider, FactController } = require('./index');

// Загружаем переменные окружения из .env файла
require('dotenv').config();


// Создаем глобальный логгер с уровнем из переменной окружения
const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Параметры подключения к MongoDB из .env
const connectionString = process.env.MONGODB_CONNECTION_STRING || 'mongodb://localhost:27017';
const databaseName = process.env.MONGODB_DATABASE_NAME || 'CounterTest';

// Параметры генерации фактов из .env
const fieldCount = parseInt(process.env.FACT_FIELD_COUNT) || 23;
const typeCount = parseInt(process.env.FACT_TYPE_COUNT) || 10;
const fieldsPerType = parseInt(process.env.FACT_FIELDS_PER_TYPE) || 10;
const targetSize = parseInt(process.env.FACT_TARGET_SIZE) || 500;

// Даты из .env
const fromDate = process.env.FACT_FROM_DATE ? new Date(process.env.FACT_FROM_DATE) : undefined;
const toDate = process.env.FACT_TO_DATE ? new Date(process.env.FACT_TO_DATE) : undefined;

// Описание структуры полей для каждого типа факта
const typeFieldsConfig = {
    1: ['f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10'],
    2: ['f2', 'f4', 'f6', 'f8', 'f10', 'f12', 'f14', 'f16', 'f18', 'f20'],
    3: ['f3', 'f6', 'f9', 'f12', 'f15', 'f17', 'f19', 'f21', 'f22', 'f23'],
    4: ['f1', 'f5', 'f7', 'f11', 'f13', 'f15', 'f17', 'f19', 'f21', 'f23'],
    5: ['f2', 'f4', 'f8', 'f10', 'f14', 'f16', 'f18', 'f20', 'f22', 'f23'],
    6: ['f1', 'f3', 'f7', 'f9', 'f11', 'f13', 'f15', 'f17', 'f19', 'f21'],
    7: ['f2', 'f5', 'f8', 'f11', 'f14', 'f16', 'f18', 'f20', 'f22', 'f23'],
    8: ['f3', 'f6', 'f9', 'f12', 'f13', 'f15', 'f17', 'f19', 'f21', 'f22'],
    9: ['f1', 'f4', 'f7', 'f10', 'f12', 'f14', 'f16', 'f18', 'f20', 'f23'],
    10: ['f2', 'f5', 'f8', 'f11', 'f13', 'f15', 'f17', 'f19', 'f21', 'f22']
};

// Логируем загруженные параметры
logger.debug('=== Загруженные параметры из .env ===');
logger.debug('MongoDB Connection String:', connectionString);
logger.debug('MongoDB Database Name:', databaseName);
logger.debug('Field Count:', fieldCount);
logger.debug('Type Count:', typeCount);
logger.debug('Fields Per Type:', fieldsPerType);
logger.debug('Target Size:', targetSize);
logger.debug('From Date:', fromDate ? fromDate.toISOString() : 'default');
logger.debug('To Date:', toDate ? toDate.toISOString() : 'default');
logger.debug('=====================================\n');

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
    try {
        let factCount = 0;
        // Создаем провайдер данных
        mongoProvider = new MongoProvider(connectionString, databaseName);
        await mongoProvider.connect();
            
        // Создаем экземпляр контроллера с dbProvider
        const factController = new FactController(mongoProvider, fieldCount, typeCount, fieldsPerType, typeFieldsConfig, fromDate, toDate, targetSize);
        const CYCLE_OUTPUT = 100;
        let startCycleTime = Date.now();
        // Функция с бесконечным циклом запуска run
        async function run(){
            await factController.run();
            factCount++;
            if (factCount % CYCLE_OUTPUT === 0) {
                logger.info(`✓ Создано ${factCount} фактов`);
                logger.info(`✓ Скорость создания фактов: ${Math.round(CYCLE_OUTPUT / (Date.now() - startCycleTime) * 1000)} фактов в секунду`);
                logger.info(`✓ Время обработки 1 факта: ${Math.round((Date.now() - startCycleTime)/ CYCLE_OUTPUT)} миллисекунд`);
                logger.info("");
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
