const FactMapper = require('../generators/factMapper');
const Logger = require('../utils/logger');

/**
 * Пример использования параметра keepUnmappedFields в FactMapper
 * Демонстрирует разницу между keepUnmappedFields=true и keepUnmappedFields=false
 */
function factMapperKeepUnmappedExample() {
    const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
    
    try {
        logger.info('=== Пример использования параметра keepUnmappedFields ===');
        
        // Создаем тестовую конфигурацию с правилами для несуществующих полей
        const testConfig = [
            {
                src: 'user_id',
                dst: 'userId',
                types: ['user_event']
            },
            {
                src: 'event_type',
                dst: 'eventType',
                types: ['user_event']
            },
            {
                src: 'nonexistent_field',
                dst: 'mapped_nonexistent',
                types: ['user_event']
            }
        ];
        
        const mapper = new FactMapper(testConfig);
        
        // Создаем тестовый факт с некоторыми полями, которые есть в правилах, и некоторыми, которых нет
        const testFact = {
            user_id: '12345',
            event_type: 'login',
            timestamp: new Date(),
            session_id: 'sess_abc123',
            additional_data: 'some_value',
            // Отсутствуют: nonexistent_field (есть в правилах)
            // Отсутствуют: other_field (нет в правилах)
        };
        
        logger.info('Входной факт:');
        logger.info(JSON.stringify(testFact, null, 2));
        
        logger.info('\n--- Тестирование с keepUnmappedFields=true (по умолчанию) ---');
        const mappedWithKeep = mapper.mapFact(testFact, 'user_event', true);
        logger.info('Результат:');
        logger.info(JSON.stringify(mappedWithKeep, null, 2));
        
        logger.info('\n--- Тестирование с keepUnmappedFields=false ---');
        const mappedWithoutKeep = mapper.mapFact(testFact, 'user_event', false);
        logger.info('Результат:');
        logger.info(JSON.stringify(mappedWithoutKeep, null, 2));
        
        logger.info('\n--- Анализ различий ---');
        logger.info('При keepUnmappedFields=true:');
        logger.info('  - Поля, не найденные в правилах маппинга, сохраняются (timestamp, session_id, additional_data)');
        logger.info('  - Поля, найденные в правилах, маппятся (user_id -> userId, event_type -> eventType)');
        logger.info('  - Несуществующие поля из правил не добавляются в результат');
        
        logger.info('\nПри keepUnmappedFields=false:');
        logger.info('  - Поля, не найденные в правилах маппинга, удаляются');
        logger.info('  - Поля, найденные в правилах, маппятся (user_id -> userId, event_type -> eventType)');
        logger.info('  - Несуществующие поля из правил не добавляются в результат');
        
        // Демонстрация с фактом, содержащим несуществующие поля из правил
        logger.info('\n--- Тестирование с полем, которое есть в правилах, но отсутствует в факте ---');
        const testFactWithMissingField = {
            user_id: '67890',
            // event_type отсутствует (есть в правилах)
            timestamp: new Date(),
            // nonexistent_field отсутствует (есть в правилах)
        };
        
        logger.info('Входной факт с отсутствующими полями:');
        logger.info(JSON.stringify(testFactWithMissingField, null, 2));
        
        const mappedWithMissingFields = mapper.mapFact(testFactWithMissingField, 'user_event', false);
        logger.info('Результат с keepUnmappedFields=false:');
        logger.info(JSON.stringify(mappedWithMissingFields, null, 2));
        
        logger.info('\nОбратите внимание:');
        logger.info('- event_type отсутствует в результате (не было в исходном факте)');
        logger.info('- timestamp удален (не было в правилах маппинга)');
        logger.info('- user_id маппится в userId');
        
    } catch (error) {
        logger.error(`Ошибка в примере: ${error.message}`);
        logger.error(error.stack);
    }
}

// Запускаем пример, если файл выполняется напрямую
if (require.main === module) {
    factMapperKeepUnmappedExample();
}

module.exports = factMapperKeepUnmappedExample;
