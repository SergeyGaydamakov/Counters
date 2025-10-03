const FactMapper = require('../generators/factMapper');
const Logger = require('../utils/logger');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

async function factMapperF23Example() {
    try {
        logger.info('=== Пример FactMapper с полями f1-f23 ===');
        
        // Создаем экземпляр маппера с тестовой конфигурацией f1-f23
        const testConfig = [];
        for (let i = 1; i <= 23; i++) {
            testConfig.push({
                src: `f${i}`,
                dst: `f${i}`,
                types: ['type1', 'type2', 'type3', 'type4', 'type5', 'type6', 'type7', 'type8', 'type9', 'type10']
            });
        }
        const mapper = new FactMapper(testConfig);
        
        logger.info(`Загружено правил маппинга: ${mapper._mappingConfig.length}`);
        
        // Создаем тестовый факт с полями f1-f23
        const inputFact = {};
        for (let i = 1; i <= 23; i++) {
            inputFact[`f${i}`] = `value_${i}`;
        }
        
        // Добавляем дополнительные поля, которые не должны маппиться
        inputFact.otherField1 = 'ignored_value_1';
        inputFact.otherField2 = 'ignored_value_2';
        
        logger.info('Входной факт:');
        logger.info(JSON.stringify(inputFact, null, 2));
        
        // Тестируем маппинг с keepUnmappedFields=true
        const mappedFactKeep = mapper.mapFact(inputFact, 'type1', true);
        logger.info('\nРезультат с keepUnmappedFields=true:');
        logger.info(JSON.stringify(mappedFactKeep, null, 2));
        
        // Тестируем маппинг с keepUnmappedFields=false
        const mappedFactRemove = mapper.mapFact(inputFact, 'type1', false);
        logger.info('\nРезультат с keepUnmappedFields=false:');
        logger.info(JSON.stringify(mappedFactRemove, null, 2));
        
        // Проверяем, что все поля f1-f23 присутствуют в результате
        const allFieldsPresent = [];
        const missingFields = [];
        
        for (let i = 1; i <= 23; i++) {
            if (`f${i}` in mappedFactKeep) {
                allFieldsPresent.push(`f${i}`);
            } else {
                missingFields.push(`f${i}`);
            }
        }
        
        logger.info(`\nАнализ результатов:`);
        logger.info(`- Поля f1-f23 присутствуют: ${allFieldsPresent.length}/23`);
        logger.info(`- Отсутствующие поля: ${missingFields.length > 0 ? missingFields.join(', ') : 'нет'}`);
        logger.info(`- Дополнительные поля в keepUnmappedFields=true: ${Object.keys(mappedFactKeep).length - 23}`);
        logger.info(`- Дополнительные поля в keepUnmappedFields=false: ${Object.keys(mappedFactRemove).length - 23}`);
        
        // Тестируем с частичным набором полей
        const partialFact = {
            f1: 'partial_value_1',
            f5: 'partial_value_5',
            f10: 'partial_value_10',
            f15: 'partial_value_15',
            f20: 'partial_value_20',
            f23: 'partial_value_23',
            extraField: 'should_be_removed'
        };
        
        logger.info('\n--- Тестирование с частичным набором полей ---');
        logger.info('Входной факт с частичными полями:');
        logger.info(JSON.stringify(partialFact, null, 2));
        
        const partialMapped = mapper.mapFact(partialFact, 'type1', false);
        logger.info('Результат маппинга (keepUnmappedFields=false):');
        logger.info(JSON.stringify(partialMapped, null, 2));
        
        logger.info('\n--- Демонстрация завершена ---');
        
    } catch (error) {
        logger.error(`Ошибка в примере: ${error.message}`);
        logger.error(error.stack);
    }
}

// Запускаем пример
factMapperF23Example();
