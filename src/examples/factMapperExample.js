const FactMapper = require('../generators/factMapper');
const Logger = require('../utils/logger');

/**
 * Пример использования модуля FactMapper
 */
function factMapperExample() {
    const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
    
    try {
        logger.info('=== Пример использования FactMapper ===');
        
        // Создаем экземпляр маппера с тестовой конфигурацией
        const testConfig = [
            {
                src: 'f1',
                dst: 'f1',
                types: ['type1', 'type2', 'type3', 'type4', 'type5', 'type6', 'type7', 'type8', 'type9', 'type10']
            }
        ];
        const mapper = new FactMapper(testConfig);
        
        logger.info('1. Инициализация с массивом конфигурации:');
        
        // Создаем тестовый входной факт
        const inputFact = {
            f1: 'test_value_123',
            f2: 'another_value',
            f3: 'third_value',
            someOtherField: 'ignored_field'
        };
        
        logger.info('Входной факт:');
        logger.info(JSON.stringify(inputFact, null, 2));
        
        // Преобразуем факт для типа 'type1'
        const mappedFact = mapper.mapFact(inputFact, 'type1');
        
        logger.info('Преобразованный факт для типа "type1":');
        logger.info(JSON.stringify(mappedFact, null, 2));
        
        // Показываем правила маппинга для типа 'type1'
        const rules = mapper.getMappingRulesForType('type1');
        logger.info('Правила маппинга для типа "type1":');
        rules.forEach((rule, index) => {
            logger.info(`  ${index + 1}. ${rule.src} -> ${rule.dst} (типы: ${rule.types.join(', ')})`);
        });
        
        // Тестируем маппинг массива фактов
        const inputFacts = [
            { f1: 'value1', f2: 'value2' },
            { f1: 'value3', f2: 'value4' },
            { f1: 'value5', f2: 'value6' }
        ];
        
        logger.info('Массив входных фактов:');
        logger.info(JSON.stringify(inputFacts, null, 2));
        
        const mappedFacts = mapper.mapFacts(inputFacts, 'type2');
        
        logger.info('Преобразованный массив фактов для типа "type2":');
        logger.info(JSON.stringify(mappedFacts, null, 2));
        
        // Тестируем с неподдерживаемым типом
        logger.info('Тестирование с неподдерживаемым типом "unknown_type":');
        const unknownTypeFact = mapper.mapFact(inputFact, 'unknown_type');
        logger.info('Результат (должен быть без изменений):');
        logger.info(JSON.stringify(unknownTypeFact, null, 2));

        // Тестируем параметр keepUnmappedFields
        logger.info('\nТестирование параметра keepUnmappedFields:');
        const testFactForKeepUnmapped = {
            f1: 'test_value',
            f2: 'another_value',
            f3: 'third_value',
            otherField: 'ignored'
        };
        
        logger.info('Входной факт:');
        logger.info(JSON.stringify(testFactForKeepUnmapped, null, 2));
        
        const mappedWithKeep = mapper.mapFact(testFactForKeepUnmapped, 'type1', true);
        logger.info('Результат с keepUnmappedFields=true (по умолчанию):');
        logger.info(JSON.stringify(mappedWithKeep, null, 2));
        
        const mappedWithoutKeep = mapper.mapFact(testFactForKeepUnmapped, 'type1', false);
        logger.info('Результат с keepUnmappedFields=false:');
        logger.info(JSON.stringify(mappedWithoutKeep, null, 2));
        
        // Пример инициализации через массив конфигурации
        logger.info('\n2. Инициализация с кастомной конфигурацией:');
        const customConfig = [
            {
                src: 'custom_field1',
                dst: 'mapped_custom_field1',
                types: ['custom_type1', 'custom_type2']
            },
            {
                src: 'custom_field2',
                dst: 'mapped_custom_field2',
                types: ['custom_type1']
            }
        ];
        
        const customMapper = new FactMapper(customConfig);
        logger.info(`   Количество правил: ${customMapper._mappingConfig.length}`);
        
        const customInputFact = {
            custom_field1: 'custom_value_1',
            custom_field2: 'custom_value_2',
            other_field: 'ignored'
        };
        
        logger.info('Входной факт для кастомного маппера:');
        logger.info(JSON.stringify(customInputFact, null, 2));
        
        const customMappedFact = customMapper.mapFact(customInputFact, 'custom_type1');
        logger.info('Преобразованный факт:');
        logger.info(JSON.stringify(customMappedFact, null, 2));
        
        logger.info('Кастомный маппер готов к использованию');
        
    } catch (error) {
        logger.error(`Ошибка в примере: ${error.message}`);
        logger.error(error.stack);
    }
}

// Запускаем пример, если файл выполняется напрямую
if (require.main === module) {
    factMapperExample();
}

module.exports = factMapperExample;
