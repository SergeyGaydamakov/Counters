const { FactController, MongoProvider } = require('../index');
const Logger = require('../utils/logger');

/**
 * Быстрый пример использования FactController
 */
async function quickExample() {
    const logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
    logger.info('=== Быстрый пример FactController ===\n');

    // Создаем провайдер данных
    const mongoProvider = new MongoProvider('mongodb://localhost:27017', 'CounterTest');
    
    // Создаем контроллер с dbProvider
    const factController = new FactController(mongoProvider);

    try {
        // Подключаемся к MongoDB
        logger.debug('Подключение к MongoDB...');
        const connected = await mongoProvider.connect();
        if (!connected) {
            logger.error('Не удалось подключиться к MongoDB');
            return;
        }
        logger.debug('✓ Подключено\n');

        logger.debug('Использование FactController...\n');

        // Создаем факт с индексными значениями
        logger.debug('Создание факта с индексными значениями...');
        const fact = {
            i: 'quick-example-001',
            t: 1,
            c: new Date(),
            f1: 'первое значение',
            f2: 'второе значение',
            f5: 'пятое значение'
        };

        const result = await factController.saveFact(fact);
        
        if (result.success) {
            logger.info('✓ Факт успешно создан');
            logger.debug(`  - ID: ${result.factId}`);
            logger.debug(`  - Создано индексных значений: ${result.indexesCreated}`);
            logger.debug(`  - Вставлено индексных значений: ${result.indexesInserted}`);
        } else {
            logger.error('✗ Ошибка создания факта:', result.error);
        }


    } finally {
        // Отключаемся от MongoDB
        logger.debug('\nОтключение от MongoDB...');
        await mongoProvider.disconnect();
        logger.debug('✓ Отключено');
    }
}

// Запуск примера
if (require.main === module) {
    quickExample()
        .then(() => {
            const logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
            logger.info('\n✓ Пример завершен');
            process.exit(0);
        })
        .catch((error) => {
            const logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
            logger.error('✗ Ошибка выполнения:', error.message);
            process.exit(1);
        });
}

module.exports = quickExample;
