const FactMapper = require('../generators/factMapper');
const Logger = require('../utils/logger');

/**
 * Пример использования FactMapper с инициализацией через массив конфигурации
 * Демонстрирует преимущества для тестирования и разработки
 */
function factMapperArrayExample() {
    const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
    
    try {
        logger.info('=== Пример FactMapper с массивом конфигурации ===');
        
        // Создаем тестовую конфигурацию
        const testConfig = [
            {
                src: 'user_id',
                dst: 'userId',
                types: ['user_event', 'user_action']
            },
            {
                src: 'event_type',
                dst: 'eventType',
                types: ['user_event', 'system_event']
            },
            {
                src: 'timestamp',
                dst: 'createdAt',
                types: ['user_event', 'user_action', 'system_event']
            },
            {
                src: 'session_id',
                dst: 'sessionId',
                types: ['user_action']
            }
        ];
        
        // Создаем маппер с тестовой конфигурацией
        const mapper = new FactMapper(testConfig);
        
        logger.info('Количество правил маппинга:', mapper._mappingConfig.length);
        
        // Тестируем маппинг для разных типов событий
        const testFacts = [
            {
                user_id: '12345',
                event_type: 'login',
                timestamp: new Date(),
                session_id: 'sess_abc123',
                additional_data: 'some_value'
            },
            {
                user_id: '67890',
                event_type: 'logout',
                timestamp: new Date(),
                additional_data: 'another_value'
            },
            {
                event_type: 'system_start',
                timestamp: new Date(),
                system_info: 'server_info'
            }
        ];
        
        logger.info('\n--- Тестирование маппинга для типа "user_event" ---');
        const userEventFacts = mapper.mapFacts(testFacts, 'user_event');
        userEventFacts.forEach((fact, index) => {
            logger.info(`Факт ${index + 1}:`);
            logger.info(JSON.stringify(fact, null, 2));
        });
        
        logger.info('\n--- Тестирование маппинга для типа "user_action" ---');
        const userActionFacts = mapper.mapFacts(testFacts, 'user_action');
        userActionFacts.forEach((fact, index) => {
            logger.info(`Факт ${index + 1}:`);
            logger.info(JSON.stringify(fact, null, 2));
        });
        
        logger.info('\n--- Тестирование маппинга для типа "system_event" ---');
        const systemEventFacts = mapper.mapFacts(testFacts, 'system_event');
        systemEventFacts.forEach((fact, index) => {
            logger.info(`Факт ${index + 1}:`);
            logger.info(JSON.stringify(fact, null, 2));
        });
        
        // Демонстрация правил маппинга для конкретного типа
        logger.info('\n--- Правила маппинга для типа "user_action" ---');
        const rules = mapper.getMappingRulesForType('user_action');
        rules.forEach((rule, index) => {
            logger.info(`Правило ${index + 1}: ${rule.src} -> ${rule.dst}`);
        });
        
        // Демонстрация завершена
        logger.info('\n--- Демонстрация завершена ---');
        
    } catch (error) {
        logger.error(`Ошибка в примере: ${error.message}`);
        logger.error(error.stack);
    }
}

// Запускаем пример, если файл выполняется напрямую
if (require.main === module) {
    factMapperArrayExample();
}

module.exports = factMapperArrayExample;
