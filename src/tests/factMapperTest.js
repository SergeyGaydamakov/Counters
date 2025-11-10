const FactMapper = require('../domain/factMapper');
const fs = require('fs');
const path = require('path');
const Logger = require('../common/logger');
const { ERROR_MISSING_KEY_IN_MESSAGE, ERROR_MISSING_KEY_IN_CONFIG } = require('../common/errors');

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
/*        
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
        this.testDuplicateSrcDstValidation('12. Тест валидации дублирующихся комбинаций src->dst...');
        this.testDuplicateSrcDifferentDst('13. Тест маппинга с дублирующимися src полями и разными dst...');
        this.testConflictingDstValidation('14. Тест валидации конфликтующих dst полей...');
        this.testFileSearchInPaths('15. Тест поиска файла в разных директориях...');
        this.testMessageConfigValidation('16. Тест валидации messageConfig.json...');
        this.testTypeConversion('17. Тест конвертации типов...');
        this.testStringTypeConversion('18. Тест конвертации в string...');
        this.testIntegerTypeConversion('19. Тест конвертации в integer...');
        this.testFloatTypeConversion('20. Тест конвертации в float...');
        this.testDateTypeConversion('21. Тест конвертации в date...');
        this.testEnumTypeConversion('22. Тест конвертации в enum...');
        this.testObjectIdTypeConversion('23. Тест конвертации в objectId...');
        this.testBooleanTypeConversion('24. Тест конвертации в boolean...');
        this.testDefaultTypeConversion('25. Тест конвертации с типом по умолчанию...');
        this.testMultipleKeyFields('26. Тест нескольких ключевых полей в конфигурации...');
        this.testMultipleKeyFieldsOrder('27. Тест порядка ключевых полей...');
        this.testMultipleKeyFieldsMissing('28. Тест отсутствующих ключевых полей...');
*/        
        this.testJsonMessageToFactConversion('29. Тест конвертации JSON сообщения в JSON факт...');
        
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
            const inputMessage = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 201);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedMessageData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится в mapped_field1');
            this.assert('field2' in mappedMessageData, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedMessageData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedMessageData), 'Исходное поле field1 удалено после маппинга');
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
            const inputMessage = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 301, false);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedMessageData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert(!('field2' in mappedMessageData), 'Поле field2 удалено при keepUnmappedFields=false');
            this.assert(!('otherField' in mappedMessageData), 'Другие поля удалены при keepUnmappedFields=false');
            this.assert(!('field1' in mappedMessageData), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('nonexistent_field' in mappedMessageData), 'Несуществующее поле удалено при keepUnmappedFields=false');
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
            const inputMessage = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 401, true);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedMessageData.mapped_field1 === 'test_value', 'Поле field1 корректно маппится');
            this.assert('field2' in mappedMessageData, 'Поле field2 сохраняется');
            this.assert('otherField' in mappedMessageData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedMessageData), 'Исходное поле field1 удалено после маппинга');
            // При keepUnmappedFields=true несуществующие поля не добавляются в результат
            this.assert(!('nonexistent_field' in mappedMessageData), 'Несуществующее поле не добавляется в результат');
            this.assert(!('mapped_nonexistent' in mappedMessageData), 'Целевое поле для несуществующего поля не создается');
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
            const inputMessage = {
                field1: 'test_value',
                field2: 'another_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 999);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedMessageData.field1 === 'test_value', 'Поле field1 сохраняется без изменений');
            this.assert(mappedMessageData.field2 === 'another_value', 'Поле field2 сохраняется без изменений');
            this.assert(mappedMessageData.otherField === 'ignored', 'Поле otherField сохраняется без изменений');
            // Факт должен вернуться без изменений, так как нет правил маппинга
            this.assert(JSON.stringify(mappedMessageData) === JSON.stringify(inputMessage), 'Факт возвращается без изменений');
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
            const inputMessage = {
                field1: 'value1',
                field2: 'value2',
                field3: 'value3',
                field4: 'value4',
                field5: 'unmapped_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 501);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(mappedMessageData.mapped_field1 === 'value1', 'Поле field1 корректно маппится в mapped_field1');
            this.assert(mappedMessageData.mapped_field2 === 'value2', 'Поле field2 корректно маппится в mapped_field2');
            this.assert(mappedMessageData.mapped_field3 === 'value3', 'Поле field3 корректно маппится в mapped_field3');
            this.assert(mappedMessageData.mapped_field4 === 'value4', 'Поле field4 корректно маппится в mapped_field4');
            this.assert('field5' in mappedMessageData, 'Немаппированное поле field5 сохраняется');
            this.assert('otherField' in mappedMessageData, 'Другие поля сохраняются');
            this.assert(!('field1' in mappedMessageData), 'Исходное поле field1 удалено после маппинга');
            this.assert(!('field2' in mappedMessageData), 'Исходное поле field2 удалено после маппинга');
            this.assert(!('field3' in mappedMessageData), 'Исходное поле field3 удалено после маппинга');
            this.assert(!('field4' in mappedMessageData), 'Исходное поле field4 удалено после маппинга');
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
                    key_order: 1 // HASH key type
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
                this.assert(error.code == ERROR_MISSING_KEY_IN_MESSAGE, 
                    'Валидация сообщения без ключевого поля', 'Корректная ошибка для сообщения без ключевого поля');
            }

        } catch (error) {
            this.assert(false, 'Валидация входящего сообщения', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест валидации дублирующихся комбинаций src->dst
     */
    testDuplicateSrcDstValidation(title) {
        this.logger.info(title);
        try {
            // Тест с дублирующимися комбинациями src->dst при пересечении message_types
            const duplicateConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    message_types: [1001, 1002]
                },
                {
                    src: 'field1', // Дублирующееся src поле
                    dst: 'mapped_field1', // Дублирующееся dst поле - должна быть ошибка
                    message_types: [1002, 1003] // Пересекается с первым правилом (1002)
                }
            ];

            try {
                new FactMapper(duplicateConfig);
                this.assert(false, 'Валидация дублирующихся комбинаций src->dst при пересечении типов', 'Должна была быть выброшена ошибка для дублирующихся комбинаций при пересечении типов');
            } catch (error) {
                this.assert(error.message.includes('Найдены дублирующиеся комбинации src->dst при пересечении message_types'), 
                    'Валидация дублирующихся комбинаций src->dst при пересечении типов', 'Корректная ошибка для дублирующихся комбинаций при пересечении типов');
                this.assert(error.message.includes('пересекающиеся типы: [1002]'), 
                    'Информация о пересекающихся типах', 'Ошибка должна содержать информацию о пересекающихся типах');
            }

            // Тест с дублирующимися комбинациями src->dst БЕЗ пересечения message_types - должна быть корректна
            const validConfigNoIntersection = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    message_types: [1001]
                },
                {
                    src: 'field1', // Дублирующееся src поле
                    dst: 'mapped_field1', // Дублирующееся dst поле - НО типы не пересекаются
                    message_types: [1002] // НЕ пересекается с первым правилом
                }
            ];

            try {
                const mapper = new FactMapper(validConfigNoIntersection);
                this.assert(mapper instanceof FactMapper, 'Валидация дублирующихся комбинаций БЕЗ пересечения типов', 'Дублирующиеся комбинации БЕЗ пересечения типов должны быть корректны');
                this.assert(mapper._mappingConfig.length === 2, 'Количество правил в корректной конфигурации', 'Должно быть 2 правила');
            } catch (error) {
                this.assert(false, 'Валидация дублирующихся комбинаций БЕЗ пересечения типов', `Ошибка при валидации корректных комбинаций: ${error.message}`);
            }

            // Тест с корректными разными комбинациями src->dst
            const validConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    message_types: [1003]
                },
                {
                    src: 'field1', // Дублирующееся src поле
                    dst: 'mapped_field2', // Разное dst поле - должно быть корректно
                    message_types: [1004]
                },
                {
                    src: 'field2', // Разное src поле
                    dst: 'mapped_field3', // Разное dst поле - должно быть корректно
                    message_types: [1005]
                }
            ];

            try {
                const mapper = new FactMapper(validConfig);
                this.assert(mapper instanceof FactMapper, 'Валидация корректных комбинаций src->dst', 'Корректные комбинации должны проходить валидацию');
                this.assert(mapper._mappingConfig.length === 3, 'Количество правил в корректной конфигурации', 'Должно быть 3 правила');
            } catch (error) {
                this.assert(false, 'Валидация корректных комбинаций src->dst', `Ошибка при валидации корректных комбинаций: ${error.message}`);
            }

        } catch (error) {
            this.assert(false, 'Валидация дублирующихся комбинаций src->dst', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест маппинга с дублирующимися src полями и разными dst полями
     */
    testDuplicateSrcDifferentDst(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'commonField',
                    dst: 'dst1',
                    message_types: [2001]
                },
                {
                    src: 'commonField', // Дублирующееся src поле
                    dst: 'dst2', // Разное dst поле
                    message_types: [2001]
                },
                {
                    src: 'uniqueField',
                    dst: 'uniqueDst',
                    message_types: [2001]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputMessage = {
                commonField: 'test_value',
                uniqueField: 'unique_value',
                otherField: 'ignored'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 2001);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            
            // Проверяем, что оба dst поля созданы с одинаковым значением из src
            this.assert(mappedMessageData.dst1 === 'test_value', 'Поле dst1 корректно маппится из commonField');
            this.assert(mappedMessageData.dst2 === 'test_value', 'Поле dst2 корректно маппится из commonField');
            this.assert(mappedMessageData.uniqueDst === 'unique_value', 'Поле uniqueDst корректно маппится из uniqueField');
            
            // Проверяем, что исходное поле удалено
            this.assert(!('commonField' in mappedMessageData), 'Исходное поле commonField удалено после маппинга');
            this.assert(!('uniqueField' in mappedMessageData), 'Исходное поле uniqueField удалено после маппинга');
            
            // Проверяем, что другие поля сохраняются
            this.assert('otherField' in mappedMessageData, 'Другие поля сохраняются');

            // Проверяем, что оба dst поля имеют одинаковое значение
            this.assert(mappedMessageData.dst1 === mappedMessageData.dst2, 'Оба dst поля имеют одинаковое значение из одного src поля');

        } catch (error) {
            this.assert(false, 'Маппинг с дублирующимися src полями и разными dst', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Ищет файл в нескольких директориях
     * @param {string} filename - имя файла для поиска
     * @param {Array<string>} searchPaths - массив путей для поиска
     * @returns {string|null} путь к найденному файлу или null
     */
    findFileInPaths(filename, searchPaths) {
        for (const searchPath of searchPaths) {
            const fullPath = path.join(searchPath, filename);
            if (fs.existsSync(fullPath)) {
                return fullPath;
            }
        }
        return null;
    }

    /**
     * Тест валидации конфликтующих dst полей
     */
    testConflictingDstValidation(title) {
        this.logger.info(title);
        try {
            // Тест с конфликтующими dst полями при пересечении message_types (разные src маппятся на одно dst)
            const conflictingConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field',
                    message_types: [4001, 4002]
                },
                {
                    src: 'field2', // Разное src поле
                    dst: 'mapped_field', // То же dst поле - должна быть ошибка
                    message_types: [4002, 4003] // Пересекается с первым правилом (4002)
                }
            ];

            try {
                new FactMapper(conflictingConfig);
                this.assert(false, 'Валидация конфликтующих dst полей при пересечении типов', 'Должна была быть выброшена ошибка для конфликтующих dst полей при пересечении типов');
            } catch (error) {
                this.assert(error.message.includes('Найдены конфликтующие dst поля при пересечении message_types'), 
                    'Валидация конфликтующих dst полей при пересечении типов', 'Корректная ошибка для конфликтующих dst полей при пересечении типов');
                this.assert(error.message.includes('пересекающиеся типы: [4002]'), 
                    'Информация о пересекающихся типах', 'Ошибка должна содержать информацию о пересекающихся типах');
            }

            // Тест с конфликтующими dst полями БЕЗ пересечения message_types - должна быть корректна
            const validConfigNoIntersection = [
                {
                    src: 'field1',
                    dst: 'mapped_field',
                    message_types: [4001]
                },
                {
                    src: 'field2', // Разное src поле
                    dst: 'mapped_field', // То же dst поле - НО типы не пересекаются
                    message_types: [4002] // НЕ пересекается с первым правилом
                }
            ];

            try {
                const mapper = new FactMapper(validConfigNoIntersection);
                this.assert(mapper instanceof FactMapper, 'Валидация конфликтующих dst полей БЕЗ пересечения типов', 'Конфликтующие dst поля БЕЗ пересечения типов должны быть корректны');
                this.assert(mapper._mappingConfig.length === 2, 'Количество правил в корректной конфигурации', 'Должно быть 2 правила');
            } catch (error) {
                this.assert(false, 'Валидация конфликтующих dst полей БЕЗ пересечения типов', `Ошибка при валидации корректных dst полей: ${error.message}`);
            }

            // Тест с корректной конфигурацией (разные src маппятся на разные dst)
            const validConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    message_types: [4003]
                },
                {
                    src: 'field2', // Разное src поле
                    dst: 'mapped_field2', // Разное dst поле - должно быть корректно
                    message_types: [4004]
                },
                {
                    src: 'field1', // Дублирующееся src поле
                    dst: 'mapped_field3', // Разное dst поле - должно быть корректно
                    message_types: [4005]
                }
            ];

            try {
                const mapper = new FactMapper(validConfig);
                this.assert(mapper instanceof FactMapper, 'Валидация корректных dst полей', 'Корректные dst поля должны проходить валидацию');
                this.assert(mapper._mappingConfig.length === 3, 'Количество правил в корректной конфигурации', 'Должно быть 3 правила');
            } catch (error) {
                this.assert(false, 'Валидация корректных dst полей', `Ошибка при валидации корректных dst полей: ${error.message}`);
            }

            // Тест с множественными конфликтами при пересечении типов
            const multipleConflictsConfig = [
                {
                    src: 'field1',
                    dst: 'conflict_field',
                    message_types: [4006, 4007]
                },
                {
                    src: 'field2',
                    dst: 'conflict_field', // Первый конфликт
                    message_types: [4007, 4008] // Пересекается с первым правилом (4007)
                },
                {
                    src: 'field3',
                    dst: 'another_conflict',
                    message_types: [4008, 4009]
                },
                {
                    src: 'field4',
                    dst: 'another_conflict', // Второй конфликт
                    message_types: [4009, 4010] // Пересекается с третьим правилом (4009)
                }
            ];

            try {
                new FactMapper(multipleConflictsConfig);
                this.assert(false, 'Валидация множественных конфликтов dst при пересечении типов', 'Должна была быть выброшена ошибка для множественных конфликтов при пересечении типов');
            } catch (error) {
                this.assert(error.message.includes('Найдены конфликтующие dst поля при пересечении message_types'), 
                    'Валидация множественных конфликтов dst при пересечении типов', 'Корректная ошибка для множественных конфликтов при пересечении типов');
                this.assert(error.message.includes('conflict_field'), 
                    'Первый конфликт в ошибке', 'Ошибка должна содержать информацию о первом конфликте');
                this.assert(error.message.includes('another_conflict'), 
                    'Второй конфликт в ошибке', 'Ошибка должна содержать информацию о втором конфликте');
            }

        } catch (error) {
            this.assert(false, 'Валидация конфликтующих dst полей', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест поиска файла в разных директориях
     */
    testFileSearchInPaths(title) {
        this.logger.info(title);
        try {
            // Тест поиска файла по имени в разных директориях
            const testConfig = [
                {
                    src: 'testField',
                    dst: 'mappedTestField',
                    message_types: [3001]
                }
            ];

            // Создаем временный файл конфигурации в текущей директории
            const tempConfigPath = path.join(process.cwd(), 'tempConfig.json');
            fs.writeFileSync(tempConfigPath, JSON.stringify(testConfig, null, 2));

            try {
                // Тестируем поиск файла по имени
                const mapper = new FactMapper('tempConfig.json');
                this.assert(mapper instanceof FactMapper, 'Поиск файла по имени', 'FactMapper должен найти файл по имени');
                this.assert(mapper._mappingConfig.length === 1, 'Загрузка конфигурации из найденного файла', 'Конфигурация должна быть загружена корректно');
                this.assert(mapper._mappingConfig[0].src === 'testField', 'Корректность загруженной конфигурации', 'Правила должны быть загружены корректно');
            } finally {
                // Удаляем временный файл
                if (fs.existsSync(tempConfigPath)) {
                    fs.unlinkSync(tempConfigPath);
                }
            }

            // Тест с несуществующим файлом
            try {
                new FactMapper('nonexistentConfig.json');
                this.assert(false, 'Поиск несуществующего файла', 'Должна была быть выброшена ошибка для несуществующего файла');
            } catch (error) {
                this.assert(error.message.includes('Файл конфигурации не найден'), 
                    'Поиск несуществующего файла', 'Корректная ошибка для несуществующего файла');
            }

            // Тест с абсолютным путем (должен работать как раньше)
            const absolutePath = path.join(process.cwd(), 'messageConfig.json');
            if (fs.existsSync(absolutePath)) {
                try {
                    const mapper = new FactMapper(absolutePath);
                    this.assert(mapper instanceof FactMapper, 'Загрузка по абсолютному пути', 'FactMapper должен работать с абсолютными путями');
                } catch (error) {
                    this.assert(false, 'Загрузка по абсолютному пути', `Ошибка при загрузке по абсолютному пути: ${error.message}`);
                }
            }

        } catch (error) {
            this.assert(false, 'Поиск файла в разных директориях', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест валидации messageConfig.json
     */
    testMessageConfigValidation(title) {
        this.logger.info(title);
        try {
            // Тестируем поиск файла по имени (FactMapper сам найдет файл в стандартных директориях)
            try {
                const mapper = new FactMapper('messageConfig.json');
                this.assert(mapper instanceof FactMapper, 'Валидация messageConfig.json', 'messageConfig.json должен проходить валидацию');
                this.assert(Array.isArray(mapper._mappingConfig), 'mappingConfig является массивом');
                this.assert(mapper._mappingConfig.length > 0, 'mappingConfig не пустой');
                
                // Проверяем, что в конфигурации есть дублирующиеся src поля с разными dst
                const srcFields = mapper._mappingConfig.map(rule => rule.src);
                const uniqueSrcFields = [...new Set(srcFields)];
                
                if (srcFields.length > uniqueSrcFields.length) {
                    this.assert(true, 'Наличие дублирующихся src полей в messageConfig.json', 'В конфигурации есть дублирующиеся src поля');
                    
                    // Проверяем, что для дублирующихся src полей есть разные dst
                    const srcDstMap = {};
                    mapper._mappingConfig.forEach(rule => {
                        if (!srcDstMap[rule.src]) {
                            srcDstMap[rule.src] = [];
                        }
                        srcDstMap[rule.src].push(rule.dst);
                    });
                    
                    let hasDifferentDst = false;
                    for (const [src, dstArray] of Object.entries(srcDstMap)) {
                        if (dstArray.length > 1) {
                            const uniqueDst = [...new Set(dstArray)];
                            if (uniqueDst.length > 1) {
                                hasDifferentDst = true;
                                this.logger.info(`   Поле ${src} маппится в разные dst: [${uniqueDst.join(', ')}]`);
                            }
                        }
                    }
                    
                    this.assert(hasDifferentDst, 'Разные dst для дублирующихся src полей', 'Для дублирующихся src полей должны быть разные dst');
                } else {
                    this.assert(false, 'Наличие дублирующихся src полей в messageConfig.json', 'В конфигурации нет дублирующихся src полей');
                }
                
            } catch (error) {
                this.assert(false, 'Валидация messageConfig.json', `Ошибка при валидации messageConfig.json: ${error.message}`);
            }

        } catch (error) {
            this.assert(false, 'Валидация messageConfig.json', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации типов
     */
    testTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'stringField',
                    dst: 'convertedString',
                    message_types: [5001],
                    generator: { type: 'string' }
                },
                {
                    src: 'intField',
                    dst: 'convertedInt',
                    message_types: [5001],
                    generator: { type: 'integer' }
                },
                {
                    src: 'dateField',
                    dst: 'convertedDate',
                    message_types: [5001],
                    generator: { type: 'date' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputMessage = {
                stringField: 123,
                intField: '456',
                dateField: '2025-01-15T10:30:00Z'
            };

            const mappedMessageData = mapper.mapMessageData(inputMessage, 5001);
            
            this.assert(typeof mappedMessageData === 'object', 'mapMessageData возвращает объект');
            this.assert(typeof mappedMessageData.convertedString === 'string', 'stringField конвертирован в string');
            this.assert(mappedMessageData.convertedString === '123', 'stringField корректно конвертирован в string');
            this.assert(typeof mappedMessageData.convertedInt === 'number', 'intField конвертирован в integer');
            this.assert(mappedMessageData.convertedInt === 456, 'intField корректно конвертирован в integer');
            this.assert(mappedMessageData.convertedDate instanceof Date, 'dateField конвертирован в Date');
            
        } catch (error) {
            this.assert(false, 'Конвертация типов', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в string
     */
    testStringTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'stringField',
                    message_types: [5002],
                    generator: { type: 'string' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: 123, expected: '123' },
                { input: true, expected: 'true' },
                { input: null, expected: null },
                { input: undefined, expected: undefined },
                { input: { obj: 'test' }, expected: '[object Object]' },
                { input: [1, 2, 3], expected: '1,2,3' }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5002);
                
                this.assert(mappedMessageData.stringField === testCase.expected, 
                    `String конвертация тест ${index + 1}`, 
                    `Ожидалось '${testCase.expected}', получено '${mappedMessageData.stringField}'`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в string', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в integer
     */
    testIntegerTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'intField',
                    message_types: [5003],
                    generator: { type: 'integer' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: '123', expected: 123 },
                { input: '456.789', expected: 456 },
                { input: '0', expected: 0 },
                { input: '-100', expected: -100 },
                { input: 'abc', expected: 0 }, // невалидное значение
                { input: null, expected: null },
                { input: undefined, expected: undefined }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5003);
                
                this.assert(mappedMessageData.intField === testCase.expected, 
                    `Integer конвертация тест ${index + 1}`, 
                    `Ожидалось ${testCase.expected}, получено ${mappedMessageData.intField}`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в integer', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в float
     */
    testFloatTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'floatField',
                    message_types: [5009],
                    generator: { type: 'float' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: '123.45', expected: 123.45 },
                { input: '456', expected: 456.0 },
                { input: '0.123', expected: 0.123 },
                { input: '-100.5', expected: -100.5 },
                { input: 'abc', expected: 0.0 }, // невалидное значение
                { input: null, expected: null },
                { input: undefined, expected: undefined },
                { input: '3.14159', expected: 3.14159 },
                { input: Infinity, expected: 0.0 }, // Infinity как невалидное значение
                { input: -Infinity, expected: 0.0 } // -Infinity как невалидное значение
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5009);
                
                this.assert(mappedMessageData.floatField === testCase.expected, 
                    `Float конвертация тест ${index + 1}`, 
                    `Ожидалось ${testCase.expected}, получено ${mappedMessageData.floatField}`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в float', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в date
     */
    testDateTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'dateField',
                    message_types: [5004],
                    generator: { type: 'date' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: '2025-01-15T10:30:00Z', expectedType: 'Date' },
                { input: '2025-01-15', expectedType: 'Date' },
                { input: 'invalid-date', expectedType: 'Date' }, // невалидная дата
                { input: null, expected: null }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5004);
                
                if (testCase.expected !== undefined) {
                    // Для null ожидаем точное совпадение
                    this.assert(mappedMessageData.dateField === testCase.expected, 
                        `Date конвертация тест ${index + 1}`, 
                        `Ожидалось ${testCase.expected}, получено ${mappedMessageData.dateField}`);
                } else {
                    // Для остальных случаев ожидаем объект Date
                    this.assert(mappedMessageData.dateField instanceof Date, 
                        `Date конвертация тест ${index + 1}`, 
                        `Ожидался объект Date, получен ${typeof mappedMessageData.dateField}`);
                }
            });

            // Отдельный тест для undefined - когда поле отсутствует в объекте
            const inputMessageWithoutField = {}; // поле field1 отсутствует
            const mappedMessageDataWithoutField = mapper.mapMessageData(inputMessageWithoutField, 5004);
            this.assert(!('dateField' in mappedMessageDataWithoutField), 
                'Date конвертация тест 5', 
                'Поле не должно быть создано, если исходное поле отсутствует');
            
        } catch (error) {
            this.assert(false, 'Конвертация в date', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в enum (эквивалент string)
     */
    testEnumTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'enumField',
                    message_types: [5005],
                    generator: { type: 'enum' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: 123, expected: '123' },
                { input: true, expected: 'true' },
                { input: 'test', expected: 'test' },
                { input: null, expected: null },
                { input: undefined, expected: undefined }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5005);
                
                this.assert(mappedMessageData.enumField === testCase.expected, 
                    `Enum конвертация тест ${index + 1}`, 
                    `Ожидалось '${testCase.expected}', получено '${mappedMessageData.enumField}'`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в enum', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в objectId
     */
    testObjectIdTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'objectIdField',
                    message_types: [5006],
                    generator: { type: 'objectId' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: '507f1f77bcf86cd799439011', expected: '507f1f77bcf86cd799439011' },
                { input: 123, expected: '123' },
                { input: true, expected: 'true' },
                { input: null, expected: null },
                { input: undefined, expected: undefined }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5006);
                
                this.assert(mappedMessageData.objectIdField === testCase.expected, 
                    `ObjectId конвертация тест ${index + 1}`, 
                    `Ожидалось '${testCase.expected}', получено '${mappedMessageData.objectIdField}'`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в objectId', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации в boolean
     */
    testBooleanTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'boolField',
                    message_types: [5007],
                    generator: { type: 'boolean' }
                }
            ];

            const mapper = new FactMapper(testConfig);
            
            // Тест различных типов входных данных
            const testCases = [
                { input: true, expected: true },
                { input: false, expected: false },
                { input: 'true', expected: true },
                { input: 'false', expected: false },
                { input: '1', expected: true },
                { input: '0', expected: false },
                { input: 'yes', expected: true },
                { input: 'no', expected: false },
                { input: 1, expected: true },
                { input: 0, expected: false },
                { input: 'invalid', expected: false },
                { input: null, expected: null },
                { input: undefined, expected: undefined }
            ];

            testCases.forEach((testCase, index) => {
                const inputMessage = { field1: testCase.input };
                const mappedMessageData = mapper.mapMessageData(inputMessage, 5007);
                
                this.assert(mappedMessageData.boolField === testCase.expected, 
                    `Boolean конвертация тест ${index + 1}`, 
                    `Ожидалось ${testCase.expected}, получено ${mappedMessageData.boolField}`);
            });
            
        } catch (error) {
            this.assert(false, 'Конвертация в boolean', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации с типом по умолчанию (string)
     */
    testDefaultTypeConversion(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'field1',
                    dst: 'defaultField',
                    message_types: [5008]
                    // Нет generator.type - должен использоваться string по умолчанию
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputMessage = { field1: 123 };
            const mappedMessageData = mapper.mapMessageData(inputMessage, 5008);
            
            this.assert(typeof mappedMessageData.defaultField === 'string', 'Тип по умолчанию string');
            this.assert(mappedMessageData.defaultField === '123', 'Корректная конвертация в string по умолчанию');
            
        } catch (error) {
            this.assert(false, 'Конвертация с типом по умолчанию', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест нескольких ключевых полей в конфигурации
     */
    testMultipleKeyFields(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'id',
                    dst: 'fact_id',
                    message_types: [6001],
                    key_order: 1
                },
                {
                    src: 'type',
                    dst: 'fact_type',
                    message_types: [6001],
                    key_order: 2
                },
                {
                    src: 'name',
                    dst: 'fact_name',
                    message_types: [6001]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputMessage = {
                t: 6001,
                d: {
                    id: 'test123',
                    type: 'user',
                    name: 'Test User'
                }
            };

            const result = mapper.mapMessageToFact(inputMessage);
            
            this.assert(result && typeof result === 'object', 'mapMessageToFact возвращает объект');
            this.assert(result._id !== null, 'ID факта сгенерирован');
            this.assert(typeof result._id === 'string', 'ID факта является строкой');
            this.assert(result.t === 6001, 'Тип сообщения сохранен');
            this.assert(result.d.fact_id === 'test123', 'Первое ключевое поле корректно маппится');
            this.assert(result.d.fact_type === 'user', 'Второе ключевое поле корректно маппится');
            this.assert(result.d.fact_name === 'Test User', 'Обычное поле корректно маппится');

            // Проверяем, что ID генерируется одинаково для одинаковых ключевых полей
            const result2 = mapper.mapMessageToFact(inputMessage);
            this.assert(result._id === result2._id, 'ID факта одинаков для одинаковых ключевых полей');

            // Проверяем, что ID отличается при изменении ключевых полей
            const inputMessage2 = {
                t: 6001,
                d: {
                    id: 'test456', // изменили первое ключевое поле
                    type: 'user',
                    name: 'Test User'
                }
            };
            const result3 = mapper.mapMessageToFact(inputMessage2);
            this.assert(result._id !== result3._id, 'ID факта отличается при изменении ключевых полей');

        } catch (error) {
            this.assert(false, 'Тест нескольких ключевых полей', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест порядка ключевых полей
     */
    testMultipleKeyFieldsOrder(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'id',
                    dst: 'fact_id',
                    message_types: [6002],
                    key_order: 2 // второй порядок
                },
                {
                    src: 'type',
                    dst: 'fact_type',
                    message_types: [6002],
                    key_order: 1 // первый порядок
                },
                {
                    src: 'name',
                    dst: 'fact_name',
                    message_types: [6002]
                }
            ];

            const mapper = new FactMapper(testConfig);
            const inputMessage = {
                t: 6002,
                d: {
                    id: 'test123',
                    type: 'user',
                    name: 'Test User'
                }
            };

            const result = mapper.mapMessageToFact(inputMessage);
            
            this.assert(result && typeof result === 'object', 'mapMessageToFact возвращает объект');
            this.assert(result._id !== null, 'ID факта сгенерирован');
            this.assert(result.t === 6002, 'Тип сообщения сохранен');
            this.assert(result.d.fact_id === 'test123', 'Поле id корректно маппится');
            this.assert(result.d.fact_type === 'user', 'Поле type корректно маппится');
            this.assert(result.d.fact_name === 'Test User', 'Поле name корректно маппится');

            // Проверяем, что порядок ключевых полей влияет на генерацию ID
            // Создаем конфигурацию с обратным порядком
            const testConfigReverse = [
                {
                    src: 'id',
                    dst: 'fact_id',
                    message_types: [6003],
                    key_order: 1 // первый порядок
                },
                {
                    src: 'type',
                    dst: 'fact_type',
                    message_types: [6003],
                    key_order: 2 // второй порядок
                }
            ];

            const mapperReverse = new FactMapper(testConfigReverse);
            const inputMessageReverse = {
                t: 6003,
                d: {
                    id: 'test123',
                    type: 'user'
                }
            };

            const resultReverse = mapperReverse.mapMessageToFact(inputMessageReverse);
            
            // ID должны отличаться из-за разного порядка ключевых полей
            this.assert(result._id !== resultReverse._id, 'ID факта отличается при разном порядке ключевых полей');

        } catch (error) {
            this.assert(false, 'Тест порядка ключевых полей', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест отсутствующих ключевых полей
     */
    testMultipleKeyFieldsMissing(title) {
        this.logger.info(title);
        try {
            const testConfig = [
                {
                    src: 'id',
                    dst: 'fact_id',
                    message_types: [6004],
                    key_order: 1
                },
                {
                    src: 'type',
                    dst: 'fact_type',
                    message_types: [6004],
                    key_order: 2
                },
                {
                    src: 'name',
                    dst: 'fact_name',
                    message_types: [6004]
                }
            ];

            const mapper = new FactMapper(testConfig);

            // Тест 1: отсутствует одно из ключевых полей
            try {
                const inputMessageMissingOne = {
                    t: 6004,
                    d: {
                        id: 'test123',
                        name: 'Test User'
                        // отсутствует поле 'type'
                    }
                };
                mapper.mapMessageToFact(inputMessageMissingOne);
                this.assert(true, 'Отсутствие одного ключевого поля', 'Не должна быть ошибка');
            } catch (error) {
                this.assert(error.code === ERROR_MISSING_KEY_IN_MESSAGE, 
                    'Отсутствие одного из двух ключевых полей', 'Некорректная ошибка для одного отсутствующего ключевого поля');
            }

            // Тест 2: отсутствуют все ключевые поля
            try {
                const inputMessageMissingAll = {
                    t: 6004,
                    d: {
                        name: 'Test User'
                        // отсутствуют поля 'id' и 'type'
                    }
                };
                mapper.mapMessageToFact(inputMessageMissingAll);
                this.assert(false, 'Отсутствие всех ключевых полей', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(error.code === ERROR_MISSING_KEY_IN_MESSAGE, 
                    'Отсутствие всех ключевых полей', 'Корректная ошибка для отсутствующих ключевых полей');
            }

            // Тест 3: отсутствует конфигурация ключевых полей для типа сообщения
            try {
                const inputMessageNoConfig = {
                    t: 9999, // тип, для которого нет конфигурации
                    d: {
                        id: 'test123',
                        type: 'user',
                        name: 'Test User'
                    }
                };
                mapper.mapMessageToFact(inputMessageNoConfig);
                this.assert(false, 'Отсутствие конфигурации ключевых полей', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(error.code === ERROR_MISSING_KEY_IN_CONFIG, 
                    'Отсутствие конфигурации ключевых полей', 'Корректная ошибка для отсутствующей конфигурации');
            }

            // Тест 4: валидное сообщение с частично заполненными ключевыми полями
            try {
                const inputMessagePartial = {
                    t: 6004,
                    d: {
                        id: 'test123',
                        type: 'user',
                        name: 'Test User'
                    }
                };
                const result = mapper.mapMessageToFact(inputMessagePartial);
                this.assert(result && typeof result === 'object', 'Валидное сообщение с ключевыми полями', 'Сообщение должно быть обработано корректно');
                this.assert(result._id !== null, 'ID факта сгенерирован для валидного сообщения', 'ID должен быть сгенерирован');
            } catch (error) {
                this.assert(false, 'Валидное сообщение с ключевыми полями', `Ошибка при обработке валидного сообщения: ${error.message}`);
            }

        } catch (error) {
            this.assert(false, 'Тест отсутствующих ключевых полей', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конвертации JSON сообщения в JSON факт
     */
    testJsonMessageToFactConversion(title) {
        this.logger.info(title);
        try {
            // Входящее сообщение из test_xml.json с добавлением ключевых полей для типа 50
            const inputMessage = {
                "MessageId": "12345678901234567890",
                "MessageTypeID": 50,
                "custom": "test",
                "i000": "0100",
                "fmt": "A",
                "rul": "atm88.1",
                "ssnumber": "12345678",
                "i039": "00",
                "i022": "0710",
                "dpan": "1234567890123456",
                "agreement": "1234567890", // key_order: 1
                "i018": "4814",
                "i002": "1234567890123456", // key_order: 2
                "i042": "123456789012345",
                "oac": "5000",
                "p_prv_id": "1",
                "tstamp": "2025-10-19",
                "status": "00",
                "rsncd": "00",
                "i049": "643",
                "i043b": "Moscow",
                "receiverCardNum": "9876543210987654",
                "i043a": "Test Merchant LLC",
                "requestorID": "MobileApp",
                "receiverRequisites": "1234567890123456",
                "ops_source_client_id": "87654321",
                "eci": "05",
                "i003": "000000",
                "s_authenticationResult": "1",
                "authenticationType": "1",
                "i032": "123456",
                "organization_id": "123456789",
                "i052": "1234",
                "i041": "12345678",
                "otbAffGrp": "1000",
                "servrul": "rule1,rule2",
                "nspk_rulesSet": "Android 12",
                "cnt": "RU",
                "mtrtyp": "1",
                "cavv": "abc123def456",
                "bankacc": "40817810123456789012",
                "status_reverse": "00",
                "mtrmod": "1",
                "phone": "+79001234567",
                "email": "test@example.com",
                "IP": "192.168.1.100",
                "orderId": "ORD123456789",
                "deviceId": "device123456789",
                "userAgent": "Mozilla/5.0 (Android 12; Mobile; rv:91.0)",
                "s_origin": "Card",
                "p_cardtype": "1",
                "p_dstCardType": "1",
                "White_List": "0",
                "resolution": "1",
                "payType": "1",
                "p_PGinterface": "P2P_PRT",
                "md_issBIN_3NumCntCode": "643",
                "p_basicFieldKey_CRC32": "a1b2c3d4",
                "pe_tb_client_private_flag": "true",
                "pe_vip_flag_3": "0",
                "pe_casino_flag_2": "1",
                "md_casino_client_flag_2": "1",
                "Issuing_BIN_8": "12345678",
                "agg_uniqueMerchId": "unique123",
                "PAN_Listing": "1",
                "ap_source_ip_by_client": "192.168.1.100",
                "rebillId": "rebill123",
                "deviceId_CRC32": "device123",
                "p_prvCode": "PRV001",
                "ap_payment_after_tokenization_3h": "1",
                "ap_payment_after_tokenization_not_green_7d": "0",
                "ap_payments_by_token_30_2d_ind": "1",
                "atmRejectedItems": "0",
                "atm_after_activation_lk_3h": "0",
                "atm_after_activation_sr_3h": "0",
                "atm_after_pc_2h": "0",
                "atm_after_pn_2h": "0",
                "atm_attribute": "1",
                "c2c_from_PAN_15m_cnt": "1",
                "c2c_from_PAN_15m_dst": "1",
                "c2c_same_dst_card_and_basicFieldKey": "1",
                "c2c_same_dst_cardnum": "1",
                "c2c_same_recipient": "1",
                "c2c_type": "1",
                "case_from_iris_sme": "0",
                "clientType": "1",
                "confirmationType": "1",
                "confirmed_device_indicator": "1",
                "drop_output_list": "0",
                "exc_merch": "0",
                "exc_providers": "0",
                "exchange_broker": "0",
                "exchange_mobile": "0",
                "exchange_prv_groups": "0",
                "finalTimestamp": "2025-10-19T18:42:25.701Z",
                "i_3ds_fp": "fp123",
                "i_Drunk_places_fraud_merchant_30d": "0",
                "i_PG_same_PAN_dst_200d_2d": "0",
                "i_PG_same_recipient_200_to_2d": "0",
                "i_any_payment_after_bad_drinking_place_24h_indicator_cnt": "0",
                "i_any_payment_after_bad_drinking_place_24h_indicator_sum": "0",
                "i_any_payment_after_drinking_place_9h": "0",
                "i_any_trxn_after_PINCHNG_indicator_3h": "0",
                "i_atm_CO_05_in_kzn_5d_ind": "0",
                "i_atm_after_taxi_5h_ind": "0",
                "i_auths_many_devices_14d_cnt": "0",
                "i_auths_trusted_device_30d": "0",
                "i_auths_trusted_wuid_30d": "0",
                "i_diff_clients_1_device_30d": "0",
                "i_many_clients_from_1_device_30d": "0",
                "i_operation_after_night_bars_3h_cnt": "0",
                "i_operation_after_pc_bad_FP_4h": "0",
                "i_operations_with_PIN_1h": "0",
                "i_pg_auth_after_sr_wb_48h": "0",
                "i_stas_after_bad_drinking_place_24h_ind_cnt": "0",
                "i_stas_after_bad_drinking_place_24h_ind_sum": "0",
                "i_stas_any_trxn_after_PINCHNG_ind_3h": "0",
                "i_stas_payment_after_drinking_place_6h_indicator_sum": "0",
                "i_stas_payment_after_drinking_place_or_shop_8h": "0",
                "i_test_cancel_selfie_by_device_70_trxn_24h": "0",
                "iss_black_listed": "0",
                "kpp": "123456789",
                "localTimestamp": "2025-10-19T18:42:25.701Z",
                "mass3_softDate": "2024-01-01",
                "md_ATM_city": "Moscow",
                "md_ATM_regionCode": "77",
                "md_IPGEO_3NumCntCode": "643",
                "md_IP_proxy_type": "0",
                "md_casino_client_flag": "1",
                "md_new_IPGEO_city_crc32": "city123",
                "md_suspectCall_date": "2024-01-01",
                "md_userAgent": "Mozilla/5.0",
                "message_CRC32": "msg123",
                "model_phone": "iPhone 13",
                "non_rur_exch": "0",
                "one_month_attribute": "1",
                "p_depEndDate_sys": "2025-12-31",
                "p_dstBankBik": "044525225",
                "p_dstOrganisationID": "123456789",
                "p_prv_grp_id": "1",
                "payCheckResult": "1",
                "pe_actual_residence_place": "Moscow",
                "pe_control_question_flag": "0",
                "pe_deposit_active_flag": "1",
                "pe_inv_value_client": "1000000",
                "pe_vip_flag": "0",
                "pe_vip_flag_2": "0",
                "pe_vip_flag_2_dst": "0",
                "pe_vip_flag_dst": "0",
                "prec_authType_temp": "1",
                "respCq": "0",
                "respSms": "0",
                "ri_dn": "ri123",
                "ri_i160_i177_179_2": "0",
                "risk_mcc": "4814",
                "s_gpu_vendor": "NVIDIA",
                "s_screen_heigth": "1920",
                "s_screen_width": "1080",
                "s_web_user_id_CRC32": "user123",
                "same_currency": "1",
                "same_mn_in_hist_cnt": "1",
                "score": "100",
                "screensharing_flag_dttm": "0",
                "sme_confirmed_device_indicator": "1",
                "sme_orgId_c2c_to_same_recip_200_1d": "0",
                "sme_orgId_norm_200days_to_1acc": "0",
                "sme_organizationId_norm_200days_to_1_INN": "0",
                "sme_organizationId_trxn_200days_to_1acc": "0",
                "sme_trans_dt_week_ago": "2024-01-01",
                "ssoId": "sso123",
                "termType": "1",
                "SystemTime": "2025-10-19T18:42:25.701Z",
                "ukbo_flag": "0",
                "userAgent_md5": "md5hash123",
                "INN_recepient_CRC32": "inn123",
                "s_customer_session_id": "session123",
                "baml": "0",
                "acsMLresult_temp": "1",
                // Добавляем недостающие ключевые поля для типа 50
                "i011": "123456", // key_order: 3
                "i037": "123456789012", // key_order: 4
                "aserno": "789012", // key_order: 5
                "payment_id": "1234567890" // key_order: 6
            };

            // Ожидаемый результат из test_fact_json.json
            const expectedResult = {
                "_id": "eXzTFMqCwue/1CnKKhYMaEqyT3k=",
                "t": 50,
                "c": "2025-10-23T12:01:16.540Z",
                "d": {
                    "MessageId": "12345678901234567890",
                    "MessageTypeID": 50,
                    "agreement": "1234567890",
                    "PAN": "1234567890123456",
                    "p_paymentId": 12345678901234567000,
                    "Timestamp": "2025-10-19T18:42:25.701Z",
                    "authenticationType": 1,
                    "paymentAccount_25": "40817810123456789012",
                    "DE48_add_data_CRC32": "abc123def456",
                    "Merch_country": "RU",
                    "dPan": "1234567890123456",
                    "eci": "05",
                    "msgMode": "A",
                    "DE0_msg_type": "0100",
                    "DE3_proc_code": "000000",
                    "mcc": "4814",
                    "DE22_pos_entry": "0710",
                    "DE32_acq_inst_code": "123456",
                    "srcClientId_R": "123456",
                    "authRC": "00",
                    "terminalId": "12345678",
                    "MerchantId": "123456789012345",
                    "Agg_submerchant_new": "123456789012345",
                    "Agg_submerchant_mcc": "123456789012345",
                    "fullMerchantName": "Test Merchant LLC",
                    "DE43b_merch_city": "Moscow",
                    "GEO_citycnt": "Moscow",
                    "DE49_cur_trxn": "643",
                    "currency": "643",
                    "DE52_PIN": "1234",
                    "trxn_mode": 1,
                    "trxn_type": 1,
                    "TrxnType": "1",
                    "s_mobile_device_os": "Android 12",
                    "avia_passenger4": "5000",
                    "Amount": 5000,
                    "p_dstClientId": "87654321",
                    "s_client_id_dst": "87654321",
                    "s_organization_id": "123456789",
                    "OTB": 1000,
                    "p_prv_id": 1,
                    "phone": "+79001234567",
                    "p_dstCardNumber": "9876543210987654",
                    "PAN_dst": "9876543210987654",
                    "p_basicFieldKey_CRC32": "a1b2c3d4",
                    "p_basicFieldKey": "1234567890123456",
                    "s_requested_application": "MobileApp",
                    "reason_code": 0,
                    "resptype": "00",
                    "rules": "atm88.1",
                    "score_rules": "atm88.1",
                    "s_authenticationResult": 1,
                    "wq_marks": "rule1,rule2",
                    "servrules": "rule1,rule2",
                    "s_client_id": "12345678",
                    "status": "00",
                    "status_reverse": "00",
                    "c2c_ml_last_pin_change_date": "2025-10-19T00:00:00.000Z",
                    "trxnDate": "2025-10-19",
                    "MessageTypeID": 100,
                    "email": "test@example.com",
                    "IP": "192.168.1.100",
                    "orderId": "ORD123456789",
                    "deviceId": "device123456789",
                    "userAgent": "Mozilla/5.0 (Android 12; Mobile; rv:91.0)",
                    "s_origin": "Card",
                    "p_cardtype": "1",
                    "p_dstCardType": "1",
                    "White_List": "0",
                    "Resolution": 1,
                    "payType": "1",
                    "p_PGinterface": "P2P_PRT",
                    "md_issBIN_3NumCntCode": "643",
                    "pe_tb_client_private_flag": "true",
                    "pe_vip_flag_3": "0",
                    "pe_casino_flag_2": "1",
                    "md_casino_client_flag_2": "1",
                    "Issuing_BIN_8": "12345678",
                    "agg_uniqueMerchId": "unique123",
                    "PAN_Listing": "1",
                    "ap_source_ip_by_client": "192.168.1.100",
                    "rebillId": "rebill123",
                    "deviceId_CRC32": "device123",
                    "p_prvCode": "PRV001",
                    "ap_payment_after_tokenization_3h": "1",
                    "ap_payment_after_tokenization_not_green_7d": "0",
                    "ap_payments_by_token_30_2d_ind": "1",
                    "atmRejectedItems": "0",
                    "atm_after_activation_lk_3h": "0",
                    "atm_after_activation_sr_3h": "0",
                    "atm_after_pc_2h": "0",
                    "atm_after_pn_2h": "0",
                    "atm_attribute": "1",
                    "c2c_from_PAN_15m_cnt": "1",
                    "c2c_from_PAN_15m_dst": "1",
                    "c2c_same_dst_card_and_basicFieldKey": "1",
                    "c2c_same_dst_cardnum": "1",
                    "c2c_same_recipient": "1",
                    "c2c_type": "1",
                    "case_from_iris_sme": "0",
                    "clientType": "1",
                    "confirmationType": "1",
                    "confirmed_device_indicator": "1",
                    "drop_output_list": "0",
                    "exc_merch": "0",
                    "exc_providers": "0",
                    "exchange_broker": "0",
                    "exchange_mobile": "0",
                    "exchange_prv_groups": "0",
                    "finalTimestamp": "2025-10-19T18:42:25.701Z",
                    "i_3ds_fp": "fp123",
                    "i_Drunk_places_fraud_merchant_30d": "0",
                    "i_PG_same_PAN_dst_200d_2d": "0",
                    "i_PG_same_recipient_200_to_2d": "0",
                    "i_any_payment_after_bad_drinking_place_24h_indicator_cnt": "0",
                    "i_any_payment_after_bad_drinking_place_24h_indicator_sum": "0",
                    "i_any_payment_after_drinking_place_9h": "0",
                    "i_any_trxn_after_PINCHNG_indicator_3h": "0",
                    "i_atm_CO_05_in_kzn_5d_ind": "0",
                    "i_atm_after_taxi_5h_ind": "0",
                    "i_auths_many_devices_14d_cnt": "0",
                    "i_auths_trusted_device_30d": "0",
                    "i_auths_trusted_wuid_30d": "0",
                    "i_diff_clients_1_device_30d": "0",
                    "i_many_clients_from_1_device_30d": "0",
                    "i_operation_after_night_bars_3h_cnt": "0",
                    "i_operation_after_pc_bad_FP_4h": "0",
                    "i_operations_with_PIN_1h": "0",
                    "i_pg_auth_after_sr_wb_48h": "0",
                    "i_stas_after_bad_drinking_place_24h_ind_cnt": "0",
                    "i_stas_after_bad_drinking_place_24h_ind_sum": "0",
                    "i_stas_any_trxn_after_PINCHNG_ind_3h": "0",
                    "i_stas_payment_after_drinking_place_6h_indicator_sum": "0",
                    "i_stas_payment_after_drinking_place_or_shop_8h": "0",
                    "i_test_cancel_selfie_by_device_70_trxn_24h": "0",
                    "iss_black_listed": "0",
                    "kpp": "123456789",
                    "localTimestamp": "2025-10-19T18:42:25.701Z",
                    "mass3_softDate": "2024-01-01",
                    "md_ATM_city": "Moscow",
                    "md_ATM_regionCode": "77",
                    "md_IPGEO_3NumCntCode": "643",
                    "md_IP_proxy_type": "0",
                    "md_casino_client_flag": "1",
                    "md_new_IPGEO_city_crc32": "city123",
                    "md_suspectCall_date": "2024-01-01",
                    "md_userAgent": "Mozilla/5.0",
                    "message_CRC32": "msg123",
                    "model_phone": "iPhone 13",
                    "non_rur_exch": "0",
                    "one_month_attribute": "1",
                    "p_depEndDate_sys": "2025-12-31",
                    "p_dstBankBik": "044525225",
                    "p_dstOrganisationID": "123456789",
                    "p_prv_grp_id": "1",
                    "payCheckResult": "1",
                    "pe_actual_residence_place": "Moscow",
                    "pe_control_question_flag": "0",
                    "pe_deposit_active_flag": "1",
                    "pe_inv_value_client": "1000000",
                    "pe_vip_flag": "0",
                    "pe_vip_flag_2": "0",
                    "pe_vip_flag_2_dst": "0",
                    "pe_vip_flag_dst": "0",
                    "prec_authType_temp": "1",
                    "respCq": "0",
                    "respSms": "0",
                    "ri_dn": "ri123",
                    "ri_i160_i177_179_2": "0",
                    "risk_mcc": "4814",
                    "s_gpu_vendor": "NVIDIA",
                    "s_screen_heigth": "1920",
                    "s_screen_width": "1080",
                    "s_web_user_id_CRC32": "user123",
                    "same_currency": "1",
                    "same_mn_in_hist_cnt": "1",
                    "score": "100",
                    "screensharing_flag_dttm": "0",
                    "sme_confirmed_device_indicator": "1",
                    "sme_orgId_c2c_to_same_recip_200_1d": "0",
                    "sme_orgId_norm_200days_to_1acc": "0",
                    "sme_organizationId_norm_200days_to_1_INN": "0",
                    "sme_organizationId_trxn_200days_to_1acc": "0",
                    "sme_trans_dt_week_ago": "2024-01-01",
                    "ssoId": "sso123",
                    "termType": "1",
                    "SystemTime": "2025-10-19T18:42:25.701Z",
                    "ukbo_flag": "0",
                    "userAgent_md5": "md5hash123",
                    "INN_recepient_CRC32": "inn123",
                    "s_customer_session_id": "session123",
                    "baml": "0",
                    "acsMLresult_temp": "1"
                }
            };

            // Загружаем реальную конфигурацию для типа 50 из messageConfig.json
            const fs = require('fs');
            const config = JSON.parse(fs.readFileSync('prod/messageConfig.json', 'utf8'));
            const testConfig = config.filter(rule => rule.message_types.includes(50));

            const mapper = new FactMapper(testConfig);
            
            // Создаем сообщение в правильном формате для mapMessageToFact
            const message = {
                t: 50,
                d: inputMessage
            };

            const result = mapper.mapMessageToFact(message);
            
            // Проверяем основные поля факта
            this.assert(result && typeof result === 'object', 'mapMessageToFact возвращает объект');
            this.assert(result._id !== null, 'ID факта сгенерирован');
            this.assert(typeof result._id === 'string', 'ID факта является строкой');
            this.assert(result.t === 50, 'Тип сообщения сохранен');
            this.assert(result.d.MessageTypeID === 50, 'MessageTypeID сохранен', 'Ожидалось: 50, получено: '+result.d.MessageTypeID);
            this.assert(result.c instanceof Date, 'Дата создания является объектом Date');
            this.assert(result.d && typeof result.d === 'object', 'Данные факта являются объектом');

            // Проверяем конкретные поля данных факта на основе реальных правил маппинга
            const testFields = [
                { src: 'agreement', dst: 'agreement', expected: '1234567890' },
                { src: 'i002', dst: 'PAN', expected: '1234567890123456' },
                { src: 'i011', dst: 'DE11_trace_num', expected: '123456' },
                { src: 'i037', dst: 'DE37_RRN', expected: '123456789012' },
                { src: 'aserno', dst: 'authlog_serno', expected: 789012, type: 'integer' },
                { src: 'payment_id', dst: 'p_paymentId', expected: 1234567890, type: 'integer' },
                { src: 'tstamp', dst: 'Timestamp', expected: '2025-10-19T00:00:00.000Z', type: 'date' },
                { src: 'MessageId', dst: 'MessageId', expected: '12345678901234567890' },
                { src: 'authenticationType', dst: 'authenticationType', expected: 1, type: 'integer' },
                { src: 'bankacc', dst: 'paymentAccount_25', expected: '40817810123456789012' },
                { src: 'cavv', dst: 'DE48_add_data_CRC32', expected: 'abc123def456' },
                { src: 'cnt', dst: 'Merch_country', expected: 'RU' },
                { src: 'dpan', dst: 'dPan', expected: '1234567890123456' },
                { src: 'eci', dst: 'eci', expected: '05' },
                { src: 'fmt', dst: 'msgMode', expected: 'A' },
                { src: 'i000', dst: 'DE0_msg_type', expected: '0100' },
                { src: 'i003', dst: 'DE3_proc_code', expected: '000000' },
                { src: 'i018', dst: 'mcc', expected: '4814' },
                { src: 'i022', dst: 'DE22_pos_entry', expected: '0710' },
                { src: 'i032', dst: 'DE32_acq_inst_code', expected: '123456' },
                { src: 'i032', dst: 'srcClientId_R', expected: '123456' },
                { src: 'i039', dst: 'authRC', expected: '00' },
                { src: 'i041', dst: 'terminalId', expected: '12345678' },
                { src: 'i042', dst: 'MerchantId', expected: '123456789012345' },
                { src: 'i043a', dst: 'fullMerchantName', expected: 'Test Merchant LLC' },
                { src: 'i043b', dst: 'DE43b_merch_city', expected: 'Moscow' },
                { src: 'i043b', dst: 'GEO_citycnt', expected: 'Moscow' },
                { src: 'i049', dst: 'DE49_cur_trxn', expected: '643' },
                { src: 'i049', dst: 'currency', expected: '643' },
                { src: 'i052', dst: 'DE52_PIN', expected: '1234' },
                { src: 'mtrtyp', dst: 'trxn_mode', expected: 1, type: 'integer' },
                { src: 'mtrtyp', dst: 'trxn_type', expected: 1, type: 'integer' },
                { src: 'mtrtyp', dst: 'TrxnType', expected: '1' },
                { src: 'nspk_rulesSet', dst: 's_mobile_device_os', expected: 'Android 12' },
                { src: 'oac', dst: 'avia_passenger4', expected: '5000' },
                { src: 'oac', dst: 'Amount', expected: 5000, type: 'integer' },
                { src: 'ops_source_client_id', dst: 'p_dstClientId', expected: '87654321' },
                { src: 'ops_source_client_id', dst: 's_client_id_dst', expected: '87654321' },
                { src: 'organization_id', dst: 's_organization_id', expected: '123456789' },
                { src: 'otbAffGrp', dst: 'OTB', expected: 1000, type: 'integer' },
                { src: 'p_prv_id', dst: 'p_prv_id', expected: 1, type: 'integer' },
                { src: 'phone', dst: 'phone', expected: '+79001234567' },
                { src: 'receiverCardNum', dst: 'p_dstCardNumber', expected: '9876543210987654' },
                { src: 'receiverCardNum', dst: 'PAN_dst', expected: '9876543210987654' },
                { src: 'p_basicFieldKey_CRC32', dst: 'p_basicFieldKey_CRC32', expected: 'a1b2c3d4' },
                { src: 'i002', dst: 'p_basicFieldKey', expected: '1234567890123456' },
                { src: 'requestorID', dst: 's_requested_application', expected: 'MobileApp' },
                { src: 'rsncd', dst: 'reason_code', expected: 0, type: 'integer' },
                { src: 'i039', dst: 'resptype', expected: '00' },
                { src: 'rul', dst: 'rules', expected: 'atm88.1' },
                { src: 'rul', dst: 'score_rules', expected: 'atm88.1' },
                { src: 's_authenticationResult', dst: 's_authenticationResult', expected: 1, type: 'integer' },
                { src: 'servrul', dst: 'wq_marks', expected: 'rule1,rule2' },
                { src: 'servrul', dst: 'servrules', expected: 'rule1,rule2' },
                { src: 'ssnumber', dst: 's_client_id', expected: '12345678' },
                { src: 'status', dst: 'status', expected: '00' },
                { src: 'status_reverse', dst: 'status_reverse', expected: '00' },
                { src: 'tstamp', dst: 'trxnDate', expected: '2025-10-19' },
                { src: 'email', dst: 'email', expected: 'test@example.com' },
                { src: 'IP', dst: 'IP', expected: '192.168.1.100' },
                { src: 'orderId', dst: 'orderId', expected: 'ORD123456789' },
                { src: 'deviceId', dst: 'deviceId', expected: 'device123456789' },
                { src: 'userAgent', dst: 'userAgent', expected: 'Mozilla/5.0 (Android 12; Mobile; rv:91.0)' },
                { src: 's_origin', dst: 's_origin', expected: 'Card' },
                { src: 'p_cardtype', dst: 'p_cardtype', expected: '1' },
                { src: 'p_dstCardType', dst: 'p_dstCardType', expected: '1' },
                { src: 'White_List', dst: 'White_List', expected: '0' },
                { src: 'payType', dst: 'payType', expected: '1' },
                { src: 'p_PGinterface', dst: 'p_PGinterface', expected: 'P2P_PRT' },
                { src: 'md_issBIN_3NumCntCode', dst: 'md_issBIN_3NumCntCode', expected: '643' },
                { src: 'pe_vip_flag_3', dst: 'pe_vip_flag_3', expected: '0' },
                { src: 'pe_casino_flag_2', dst: 'pe_casino_flag_2', expected: '1' },
                { src: 'md_casino_client_flag_2', dst: 'md_casino_client_flag_2', expected: '1' },
                { src: 'Issuing_BIN_8', dst: 'Issuing_BIN_8', expected: '12345678' },
                { src: 'agg_uniqueMerchId', dst: 'agg_uniqueMerchId', expected: 'unique123' },
                { src: 'PAN_Listing', dst: 'PAN_Listing', expected: '1' },
                { src: 'ap_source_ip_by_client', dst: 'ap_source_ip_by_client', expected: '192.168.1.100' },
                { src: 'rebillId', dst: 'rebillId', expected: 'rebill123' },
                { src: 'deviceId_CRC32', dst: 'deviceId_CRC32', expected: 'device123' },
                { src: 'p_prvCode', dst: 'p_prvCode', expected: 'PRV001' },
                { src: 'pe_tb_client_private_flag', dst: 'pe_tb_client_private_flag', expected: "true"},
                { src: 'resolution', dst: 'Resolution', expected: 1}
            ];

            // Проверяем каждое поле
            testFields.forEach((field, index) => {
                const actualValue = result.d[field.dst];
                const expectedValue = field.expected;
                
                if (field.type === 'date') {
                    // Для дат проверяем, что это объект Date
                    this.assert(actualValue instanceof Date, 
                        `Поле ${field.dst} (${field.src} -> ${field.dst})`, 
                        `Ожидался объект Date, получен: ${typeof actualValue}`);
                } else {
                    this.assert(actualValue === expectedValue, 
                        `Поле ${field.dst} (${field.src} -> ${field.dst})`, 
                        `Ожидалось: ${expectedValue}, получено: ${actualValue} (тип: ${typeof actualValue})`);
                }
            });

            // Проверяем, что ID факта генерируется корректно
            this.assert(result._id === expectedResult._id, 
                'ID факта соответствует ожидаемому', 
                `Ожидался: ${expectedResult._id}, получен: ${result._id}`);

        } catch (error) {
            this.assert(false, 'Конвертация JSON сообщения в JSON факт', `Ошибка: ${error.message}`);
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
