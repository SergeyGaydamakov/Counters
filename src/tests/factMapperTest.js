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
        this.testMapFactWithMultipleFields('8. Тест маппинга факта с множественными полями...');
        this.testGetMappingRulesForType('9. Тест получения правил маппинга для типа...');
        this.testErrorHandling('10. Тест обработки ошибок...');
        this.testMessageValidation('11. Тест валидации входящего сообщения для mapMessageToFact...');
        
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
                    message_types: [1, 2]
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
                    message_types: [101, 102]
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    message_types: [101]
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
                    message_types: [201]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputEvent = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedEventData = mapper.mapMessageData(inputEvent, 201);
            
            this.assert(typeof mappedEventData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedEventData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится в mapped_field1');
            this.assert('field2' in mappedEventData, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedEventData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedEventData), 'Исходное поле field1 удалено после маппинга');
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
                    message_types: [301]
                },
                {
                    src: 'nonexistent_field',
                    dst: 'mapped_nonexistent',
                    message_types: [301]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputEvent = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedEventData = mapper.mapMessageData(inputEvent, 301, false);
            
            this.assert(typeof mappedEventData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedEventData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert(!('field2' in mappedEventData), 'Поле field2 удалено при keepUnmappedFields=false');
            this.assert(!('otherField' in mappedEventData), 'Другие поля удалены при keepUnmappedFields=false');
            this.assert(!('field1' in mappedEventData), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('nonexistent_field' in mappedEventData), 'Несуществующее поле удалено при keepUnmappedFields=false');
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
                    message_types: [401]
                },
                {
                    src: 'nonexistent_field',
                    dst: 'mapped_nonexistent',
                    message_types: [401]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputEvent = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedEventData = mapper.mapMessageData(inputEvent, 401, true);
            
            this.assert(typeof mappedEventData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedEventData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert('field2' in mappedEventData, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedEventData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedEventData), 'Исходное поле field1 удалено после маппинга');
            // При keepUnmappedFields=true несуществующие поля не добавляются в результат
            this.assert(!('nonexistent_field' in mappedEventData), 'Несуществующее поле не добавляется в результат');
            this.assert(!('mapped_nonexistent' in mappedEventData), 'Целевое поле для несуществующего поля не создается');
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
            const inputEvent = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedEventData = mapper.mapMessageData(inputEvent, 999);
            
            this.assert(typeof mappedEventData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedEventData.field1 === 'test_value', 'Поле field1 сохраняется без изменений');
            this.assert(mappedEventData.field2 === 'another_value', 'Поле field2 сохраняется без изменений');
            this.assert(mappedEventData.otherField === 'ignored', 'Поле otherField сохраняется без изменений');
            // Факт должен вернуться без изменений, так как нет правил маппинга
            this.assert(JSON.stringify(mappedEventData) === JSON.stringify(inputEvent), 'Факт возвращается без изменений');
        } catch (error) {
            this.assert(false, 'Маппинг факта без конфигурации', `Ошибка: ${error.message}`);
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
                    message_types: [501]
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    message_types: [501]
                },
                {
                    src: 'field3',
                    dst: 'mapped_field3',
                    message_types: [501]
                },
                {
                    src: 'field4',
                    dst: 'mapped_field4',
                    message_types: [501]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputEvent = {
                field1: 'value1',
                field2: 'value2',
                field3: 'value3',
                field4: 'value4',
                field5: 'unmapped_value',
                otherField: 'ignored'
            };

            const mappedEventData = mapper.mapMessageData(inputEvent, 501);
            
            this.assert(typeof mappedEventData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedEventData.mapped_field1 === 'value1', 'Поле field1 корректно маппится в mapped_field1');
            this.assert(mappedEventData.mapped_field2 === 'value2', 'Поле field2 корректно маппится в mapped_field2');
            this.assert(mappedEventData.mapped_field3 === 'value3', 'Поле field3 корректно маппится в mapped_field3');
            this.assert(mappedEventData.mapped_field4 === 'value4', 'Поле field4 корректно маппится в mapped_field4');
            this.assert('field5' in mappedEventData, 'Немаппированное поле field5 сохраняется');
            this.assert('otherField' in mappedEventData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedEventData), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('field2' in mappedEventData), 'Исходное поле field2 удалено после маппинга');
            this.assert(!('field3' in mappedEventData), 'Исходное поле field3 удалено после маппинга');
            this.assert(!('field4' in mappedEventData), 'Исходное поле field4 удалено после маппинга');
        } catch (error) {
            this.assert(false, 'Маппинг факта с несколькими полями', `Ошибка: ${error.message}`);
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
                    message_types: [601, 602]
                },
                {
                    src: 'field2',
                    dst: 'mapped_field2',
                    message_types: [601]
                },
                {
                    src: 'field3',
                    dst: 'mapped_field3',
                    message_types: [603]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const rules = mapper.getMappingRulesForType(601);
            
            this.assert(Array.isArray(rules), 'getMappingRulesForType возвращает массив');
            this.assert(rules.length === 2, 'Есть 2 правила для типа 601');
            
            if (rules.length > 0) {
                const rule = rules[0];
                this.assert(typeof rule.src === 'string', 'Правило содержит src');
                this.assert(typeof rule.dst === 'string', 'Правило содержит dst');
                this.assert(Array.isArray(rule.message_types), 'Правило содержит message_types');
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
            const testConfig = [{ src: 'field1', dst: 'mapped_field1', message_types: [701] }];
            const mapper = new FactMapper(testConfig);
            
            // Тест с невалидным входным фактом
            try {
                mapper.mapMessageData(null, 701);
                this.assert(false, 'Обработка null факта', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка null факта', 'Ошибка корректно обработана');
            }

            // Тест с невалидным типом факта
            try {
                mapper.mapMessageData({ field1: 'test' }, null);
                this.assert(false, 'Обработка null типа', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка null типа', 'Ошибка корректно обработана');
            }

            // Тест с невалидным массивом фактов (теперь проверяем mapMessageData с невалидными данными)
            try {
                mapper.mapMessageData('not an object', 701);
                this.assert(false, 'Обработка невалидного объекта', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка невалидного объекта', 'Ошибка корректно обработана');
            }

            // Тест с невалидной конфигурацией при инициализации
            try {
                new FactMapper([{ src: 'field1' }]); // Отсутствует dst и message_types
                this.assert(false, 'Обработка невалидной конфигурации', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(true, 'Обработка невалидной конфигурации', 'Ошибка корректно обработана');
            }

        } catch (error) {
            this.assert(false, 'Обработка ошибок', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест валидации входящего сообщения для mapMessageToFact
     */
    testMessageValidation(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'id',
                    dst: 'fact_id',
                    message_types: [1001],
                    key_type: 1 // HASH key type
                },
                {
                    src: 'name',
                    dst: 'fact_name',
                    message_types: [1001]
                }
            ];

            const mapper = new FactMapper(testConfig);

            // Тест 1: null сообщение
            try {
                mapper.mapMessageToFact(null);
                this.assert(false, 'Валидация null сообщения', 'Должна была быть выброшена ошибка для null');
            } catch (error) {
                this.assert(error.message.includes('Входное сообщение должно быть объектом'), 
                    'Валидация null сообщения', 'Корректная ошибка для null сообщения');
            }

            // Тест 2: undefined сообщение
            try {
                mapper.mapMessageToFact(undefined);
                this.assert(false, 'Валидация undefined сообщения', 'Должна была быть выброшена ошибка для undefined');
            } catch (error) {
                this.assert(error.message.includes('Входное сообщение должно быть объектом'), 
                    'Валидация undefined сообщения', 'Корректная ошибка для undefined сообщения');
            }

            // Тест 3: не объект (строка)
            try {
                mapper.mapMessageToFact('not an object');
                this.assert(false, 'Валидация строки как сообщения', 'Должна была быть выброшена ошибка для строки');
            } catch (error) {
                this.assert(error.message.includes('Входное сообщение должно быть объектом'), 
                    'Валидация строки как сообщения', 'Корректная ошибка для строки');
            }

            // Тест 4: не объект (число)
            try {
                mapper.mapMessageToFact(123);
                this.assert(false, 'Валидация числа как сообщения', 'Должна была быть выброшена ошибка для числа');
            } catch (error) {
                this.assert(error.message.includes('Входное сообщение должно быть объектом'), 
                    'Валидация числа как сообщения', 'Корректная ошибка для числа');
            }

            // Тест 5: отсутствует поле t (тип сообщения)
            try {
                mapper.mapMessageToFact({ d: { id: 'test', name: 'test' } });
                this.assert(false, 'Валидация отсутствия поля t', 'Должна была быть выброшена ошибка для отсутствующего поля t');
            } catch (error) {
                this.assert(error.message.includes('Тип сообщения должен быть целым числом'), 
                    'Валидация отсутствия поля t', 'Корректная ошибка для отсутствующего поля t');
            }

            // Тест 6: поле t не является числом
            try {
                mapper.mapMessageToFact({ t: 'not a number', d: { id: 'test', name: 'test' } });
                this.assert(false, 'Валидация нечислового поля t', 'Должна была быть выброшена ошибка для нечислового поля t');
            } catch (error) {
                this.assert(error.message.includes('Тип сообщения должен быть целым числом'), 
                    'Валидация нечислового поля t', 'Корректная ошибка для нечислового поля t');
            }

            // Тест 7: поле t является null
            try {
                mapper.mapMessageToFact({ t: null, d: { id: 'test', name: 'test' } });
                this.assert(false, 'Валидация null поля t', 'Должна была быть выброшена ошибка для null поля t');
            } catch (error) {
                this.assert(error.message.includes('Тип сообщения должен быть целым числом'), 
                    'Валидация null поля t', 'Корректная ошибка для null поля t');
            }

            // Тест 8: отсутствует поле d (данные сообщения)
            try {
                mapper.mapMessageToFact({ t: 1001 });
                this.assert(false, 'Валидация отсутствия поля d', 'Должна была быть выброшена ошибка для отсутствующего поля d');
            } catch (error) {
                this.assert(error.message.includes('Данные сообщения должны быть объектом'), 
                    'Валидация отсутствия поля d', 'Корректная ошибка для отсутствующего поля d');
            }

            // Тест 9: поле d не является объектом
            try {
                mapper.mapMessageToFact({ t: 1001, d: 'not an object' });
                this.assert(false, 'Валидация необъектного поля d', 'Должна была быть выброшена ошибка для необъектного поля d');
            } catch (error) {
                this.assert(error.message.includes('Данные сообщения должны быть объектом'), 
                    'Валидация необъектного поля d', 'Корректная ошибка для необъектного поля d');
            }

            // Тест 10: поле d является null
            try {
                mapper.mapMessageToFact({ t: 1001, d: null });
                this.assert(false, 'Валидация null поля d', 'Должна была быть выброшена ошибка для null поля d');
            } catch (error) {
                this.assert(error.message.includes('Данные сообщения должны быть объектом'), 
                    'Валидация null поля d', 'Корректная ошибка для null поля d');
            }

            // Тест 11: валидное сообщение должно проходить валидацию
            try {
                const validMessage = { t: 1001, d: { id: 'test123', name: 'Test Name' } };
                const result = mapper.mapMessageToFact(validMessage);
                this.assert(result && typeof result === 'object', 'Валидация корректного сообщения', 'Корректное сообщение должно проходить валидацию');
                this.assert(result._id !== null, 'Генерация ID для корректного сообщения', 'ID должен быть сгенерирован для корректного сообщения');
                this.assert(result.t === 1001, 'Сохранение типа сообщения', 'Тип сообщения должен сохраняться');
                this.assert(result.d && typeof result.d === 'object', 'Сохранение данных сообщения', 'Данные сообщения должны сохраняться');
            } catch (error) {
                this.assert(false, 'Валидация корректного сообщения', `Ошибка при обработке корректного сообщения: ${error.message}`);
            }

            // Тест 12: сообщение без ключевого поля (отсутствует поле для генерации _id)
            try {
                const messageWithoutKey = { t: 1001, d: { name: 'Test Name' } }; // отсутствует поле 'id'
                mapper.mapMessageToFact(messageWithoutKey);
                this.assert(false, 'Валидация сообщения без ключевого поля', 'Должна была быть выброшена ошибка для сообщения без ключевого поля');
            } catch (error) {
                this.assert(error.message.includes('не указан ключ'), 
                    'Валидация сообщения без ключевого поля', 'Корректная ошибка для сообщения без ключевого поля');
            }

        } catch (error) {
            this.assert(false, 'Валидация входящего сообщения', `Ошибка: ${error.message}`);
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
