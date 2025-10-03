const FactMapper = require('../generators/factMapper');
const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger');

/**
 * Тесты для модуля FactMapper
 */
class FactMapperTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
    }

    /**
     * Запускает все тесты
     */
    runAllTests() {
        this.logger.info('=== Запуск тестов FactMapper ===');
        
        this.testConstructor('1. Тест конструктора с файлом конфигурации...');
        this.testConstructorWithArray('2. Тест конструктора с массивом конфигурации...');
        this.testConstructorWithoutConfig('3. Тест конструктора без конфигурации...');
        this.testMapFact('4. Тест маппинга одного факта...');
        this.testMapFactKeepUnmappedFieldsFalse('5. Тест маппинга с keepUnmappedFields=false...');
        this.testMapFactKeepUnmappedFieldsTrue('6. Тест маппинга с keepUnmappedFields=true...');
        this.testMapFactWithoutConfig('7. Тест маппинга без конфигурации...');
        this.testMapFacts('8. Тест маппинга массива фактов...');
        this.testMapFactWithMultipleFields('9. Тест маппинга факта с множественными полями...');
        this.testMapFactsWithMultipleFields('10. Тест маппинга массива фактов с множественными полями...');
        this.testGetMappingRulesForType('11. Тест получения правил маппинга для типа...');
        this.testErrorHandling('12. Тест обработки ошибок...');
        
        this.printResults();
    }

    /**
     * Проверяет результат теста
     */
    assert(condition, testName, errorMessage = '') {
        if (condition) {
            this.testResults.passed++;
            this.logger.info(`✓ ${testName}`);
        } else {
            this.testResults.failed++;
            this.testResults.errors.push(`${testName}: ${errorMessage}`);
            this.logger.error(`✗ ${testName}: ${errorMessage}`);
        }
    }

    /**
     * Тест конструктора
     */
    testConstructor(title) {
        this.logger.info(title);
        try {
            // Тест инициализации через массив конфигурации
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['type1', 'type2']
                }
            ];
            
            const mapper = new FactMapper(testConfig);
            this.assert(mapper instanceof FactMapper, 'Конструктор создает экземпляр FactMapper');
            this.assert(Array.isArray(mapper._mappingConfig), 'mappingConfig является массивом');
            this.assert(mapper._mappingConfig.length > 0, 'mappingConfig не пустой');
        } catch (error) {
            this.assert(false, 'Конструктор', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест инициализации через массив конфигурации
     */
    testConstructorWithArray(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type1', 'test_type2']
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    types: ['test_type1']
                }
            ];

            const mapper = new FactMapper(testConfig);
            this.assert(mapper instanceof FactMapper, 'Конструктор с массивом создает экземпляр FactMapper');
            this.assert(Array.isArray(mapper._mappingConfig), 'mappingConfig является массивом');
            this.assert(mapper._mappingConfig.length === 2, 'mappingConfig содержит 2 правила');
            this.assert(mapper._mappingConfig[0].src === 'field1', 'Первое правило корректно загружено');
            this.assert(mapper._mappingConfig[1].src === 'field2', 'Второе правило корректно загружено');
        } catch (error) {
            this.assert(false, 'Конструктор с массивом', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест инициализации без конфигурации
     */
    testConstructorWithoutConfig(title) {
        this.logger.info(title);
        try {
            const mapper = new FactMapper();
            this.assert(mapper instanceof FactMapper, 'Конструктор без конфигурации создает экземпляр FactMapper');
            this.assert(Array.isArray(mapper._mappingConfig), 'mappingConfig является массивом');
            this.assert(mapper._mappingConfig.length === 0, 'mappingConfig пустой');
        } catch (error) {
            this.assert(false, 'Конструктор без конфигурации', `Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест маппинга одного факта
     */
    testMapFact(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFact = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedFact = mapper.mapFact(inputFact, 'test_type');
            
            this.assert(typeof mappedFact === 'object', 'mapFact возвращает объект');
            this.assert(mappedFact.mapped_field1 === 'test_value', 'Поле field1 корректно маппится в mapped_field1');
            this.assert('field2' in mappedFact, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedFact, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedFact), 'Исходное поле field1 удалено после маппинга');
        } catch (error) {
            this.assert(false, 'Маппинг факта', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга с параметром keepUnmappedFields = false
     */
    testMapFactKeepUnmappedFieldsFalse(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                },
                {
                    src: 'nonexistent_field',
                    dst: 'mapped_nonexistent',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFact = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedFact = mapper.mapFact(inputFact, 'test_type', false);
            
            this.assert(typeof mappedFact === 'object', 'mapFact возвращает объект');
            this.assert(mappedFact.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert(!('field2' in mappedFact), 'Поле field2 удалено при keepUnmappedFields=false');
            this.assert(!('otherField' in mappedFact), 'Другие поля удалены при keepUnmappedFields=false');
            this.assert(!('field1' in mappedFact), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('nonexistent_field' in mappedFact), 'Несуществующее поле удалено при keepUnmappedFields=false');
        } catch (error) {
            this.assert(false, 'Маппинг факта с keepUnmappedFields=false', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга с параметром keepUnmappedFields = true (по умолчанию)
     */
    testMapFactKeepUnmappedFieldsTrue(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                },
                {
                    src: 'nonexistent_field',
                    dst: 'mapped_nonexistent',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFact = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedFact = mapper.mapFact(inputFact, 'test_type', true);
            
            this.assert(typeof mappedFact === 'object', 'mapFact возвращает объект');
            this.assert(mappedFact.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert('field2' in mappedFact, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedFact, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedFact), 'Исходное поле field1 удалено после маппинга');
            // При keepUnmappedFields=true несуществующие поля не добавляются в результат
            this.assert(!('nonexistent_field' in mappedFact), 'Несуществующее поле не добавляется в результат');
            this.assert(!('mapped_nonexistent' in mappedFact), 'Целевое поле для несуществующего поля не создается');
        } catch (error) {
            this.assert(false, 'Маппинг факта с keepUnmappedFields=true', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга без конфигурации
     */
    testMapFactWithoutConfig(title) {
        this.logger.info(title);
        try {
            const mapper = new FactMapper(); // Без конфигурации
            const inputFact = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedFact = mapper.mapFact(inputFact, 'any_type');
            
            this.assert(typeof mappedFact === 'object', 'mapFact возвращает объект');
            this.assert(mappedFact.field1 === 'test_value', 'Поле field1 сохраняется без изменений');
            this.assert(mappedFact.field2 === 'another_value', 'Поле field2 сохраняется без изменений');
            this.assert(mappedFact.otherField === 'ignored', 'Поле otherField сохраняется без изменений');
            // Факт должен вернуться без изменений, так как нет правил маппинга
            this.assert(JSON.stringify(mappedFact) === JSON.stringify(inputFact), 'Факт возвращается без изменений');
        } catch (error) {
            this.assert(false, 'Маппинг факта без конфигурации', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга массива фактов
     */
    testMapFacts(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFacts = [
                { field1: 'value1' },
                { field1: 'value2' },
                { field1: 'value3' }
            ];

            const mappedFacts = mapper.mapFacts(inputFacts, 'test_type');
            
            this.assert(Array.isArray(mappedFacts), 'mapFacts возвращает массив');
            this.assert(mappedFacts.length === 3, 'Количество фактов сохраняется');
            this.assert(mappedFacts[0].mapped_field1 === 'value1', 'Первый факт корректно маппится');
            this.assert(mappedFacts[1].mapped_field1 === 'value2', 'Второй факт корректно маппится');
            this.assert(mappedFacts[2].mapped_field1 === 'value3', 'Третий факт корректно маппится');
        } catch (error) {
            this.assert(false, 'Маппинг массива фактов', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга одного факта с несколькими полями
     */
    testMapFactWithMultipleFields(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    types: ['test_type']
                },
                {
                    src: 'field3',
                    dst: 'mapped_field3',
                    types: ['test_type']
                },
                {
                    src: 'field4',
                    dst: 'mapped_field4',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFact = {
                field1: 'value1',
                field2: 'value2',
                field3: 'value3',
                field4: 'value4',
                field5: 'unmapped_value',
                otherField: 'ignored'
            };

            const mappedFact = mapper.mapFact(inputFact, 'test_type');
            
            this.assert(typeof mappedFact === 'object', 'mapFact возвращает объект');
            this.assert(mappedFact.mapped_field1 === 'value1', 'Поле field1 корректно маппится в mapped_field1');
            this.assert(mappedFact.mapped_field2 === 'value2', 'Поле field2 корректно маппится в mapped_field2');
            this.assert(mappedFact.mapped_field3 === 'value3', 'Поле field3 корректно маппится в mapped_field3');
            this.assert(mappedFact.mapped_field4 === 'value4', 'Поле field4 корректно маппится в mapped_field4');
            this.assert('field5' in mappedFact, 'Немаппированное поле field5 сохраняется');
            this.assert('otherField' in mappedFact, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedFact), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('field2' in mappedFact), 'Исходное поле field2 удалено после маппинга');
            this.assert(!('field3' in mappedFact), 'Исходное поле field3 удалено после маппинга');
            this.assert(!('field4' in mappedFact), 'Исходное поле field4 удалено после маппинга');
        } catch (error) {
            this.assert(false, 'Маппинг факта с несколькими полями', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга массива фактов с несколькими полями
     */
    testMapFactsWithMultipleFields(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['test_type']
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    types: ['test_type']
                },
                {
                    src: 'field3',
                    dst: 'mapped_field3',
                    types: ['test_type']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputFacts = [
                { 
                    field1: 'value1_1', 
                    field2: 'value1_2', 
                    field3: 'value1_3',
                    field4: 'unmapped1'
                },
                { 
                    field1: 'value2_1', 
                    field2: 'value2_2', 
                    field3: 'value2_3',
                    field4: 'unmapped2'
                },
                { 
                    field1: 'value3_1', 
                    field2: 'value3_2', 
                    field3: 'value3_3',
                    field4: 'unmapped3'
                }
            ];

            const mappedFacts = mapper.mapFacts(inputFacts, 'test_type');
            
            this.assert(Array.isArray(mappedFacts), 'mapFacts возвращает массив');
            this.assert(mappedFacts.length === 3, 'Количество фактов сохраняется');
            
            // Проверяем первый факт
            this.assert(mappedFacts[0].mapped_field1 === 'value1_1', 'Первый факт: field1 корректно маппится');
            this.assert(mappedFacts[0].mapped_field2 === 'value1_2', 'Первый факт: field2 корректно маппится');
            this.assert(mappedFacts[0].mapped_field3 === 'value1_3', 'Первый факт: field3 корректно маппится');
            this.assert(mappedFacts[0].field4 === 'unmapped1', 'Первый факт: немаппированное поле сохраняется');
            
            // Проверяем второй факт
            this.assert(mappedFacts[1].mapped_field1 === 'value2_1', 'Второй факт: field1 корректно маппится');
            this.assert(mappedFacts[1].mapped_field2 === 'value2_2', 'Второй факт: field2 корректно маппится');
            this.assert(mappedFacts[1].mapped_field3 === 'value2_3', 'Второй факт: field3 корректно маппится');
            this.assert(mappedFacts[1].field4 === 'unmapped2', 'Второй факт: немаппированное поле сохраняется');
            
            // Проверяем третий факт
            this.assert(mappedFacts[2].mapped_field1 === 'value3_1', 'Третий факт: field1 корректно маппится');
            this.assert(mappedFacts[2].mapped_field2 === 'value3_2', 'Третий факт: field2 корректно маппится');
            this.assert(mappedFacts[2].mapped_field3 === 'value3_3', 'Третий факт: field3 корректно маппится');
            this.assert(mappedFacts[2].field4 === 'unmapped3', 'Третий факт: немаппированное поле сохраняется');
        } catch (error) {
            this.assert(false, 'Маппинг массива фактов с несколькими полями', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения правил маппинга для типа
     */
    testGetMappingRulesForType(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    types: ['type1', 'type2']
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    types: ['type1']
                },
                {
                    src: 'field3',
                    dst: 'mapped_field3',
                    types: ['type3']
                }
            ];

            const mapper = new FactMapper(testConfig);
            const rules = mapper.getMappingRulesForType('type1');
            
            this.assert(Array.isArray(rules), 'getMappingRulesForType возвращает массив');
            this.assert(rules.length === 2, 'Есть 2 правила для type1');
            
            if (rules.length > 0) {
                const rule = rules[0];
                this.assert(typeof rule.src === 'string', 'Правило содержит src');
                this.assert(typeof rule.dst === 'string', 'Правило содержит dst');
                this.assert(Array.isArray(rule.types), 'Правило содержит types');
            }
        } catch (error) {
            this.assert(false, 'Получение правил маппинга', `Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест обработки ошибок
     */
    testErrorHandling(title) {
        this.logger.info(title);
        try {
            const testConfig = [{ src: 'field1', dst: 'mapped_field1', types: ['type1'] }];
            const mapper = new FactMapper(testConfig);
            
            // Тест с невалидным входным фактом
            try {
                mapper.mapFact(null, 'type1');
                this.assert(false, 'Обработка null факта', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка null факта', 'Ошибка корректно обработана');
            }

            // Тест с невалидным типом факта
            try {
                mapper.mapFact({ field1: 'test' }, null);
                this.assert(false, 'Обработка null типа', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка null типа', 'Ошибка корректно обработана');
            }

            // Тест с невалидным массивом фактов
            try {
                mapper.mapFacts('not an array', 'type1');
                this.assert(false, 'Обработка невалидного массива', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка невалидного массива', 'Ошибка корректно обработана');
            }

            // Тест с невалидной конфигурацией при инициализации
            try {
                new FactMapper([{ src: 'field1' }]); // Отсутствует dst и types
                this.assert(false, 'Обработка невалидной конфигурации', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка невалидной конфигурации', 'Ошибка корректно обработана');
            }

        } catch (error) {
            this.assert(false, 'Обработка ошибок', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Выводит результаты тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования FactMapper ===');
        this.logger.info(`Пройдено: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
            this.logger.info('\nОшибки:');
            this.testResults.errors.forEach(error => {
                this.logger.error(`  - ${error}`);
            });
        }
        
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? (this.testResults.passed / total * 100).toFixed(2) : 0;
        this.logger.info(`\nПроцент успешности: ${successRate}%`);
        
        if (this.testResults.failed === 0) {
            this.logger.info('🎉 Все тесты прошли успешно!');
        } else {
            this.logger.error('❌ Некоторые тесты провалились');
        }
    }
}

// Запускаем тесты, если файл выполняется напрямую
if (require.main === module) {
    const test = new FactMapperTest();
    test.runAllTests();
}

module.exports = FactMapperTest;
