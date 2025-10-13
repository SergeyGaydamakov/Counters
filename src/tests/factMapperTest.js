const FactMapper = require('../generators/factMapper');
const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger');
const { ERROR_MISSING_KEY_IN_MESSAGE } = require('../common/errors');

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
        this.testDuplicateSrcDstValidation('12. Тест валидации дублирующихся комбинаций src->dst...');
        this.testDuplicateSrcDifferentDst('13. Тест маппинга с дублирующимися src полями и разными dst...');
        this.testConflictingDstValidation('14. Тест валидации конфликтующих dst полей...');
        this.testFileSearchInPaths('15. Тест поиска файла в разных директориях...');
        this.testMessageConfigValidation('16. Тест валидации messageConfig.json...');
        
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
            // Тест с дублирующимися комбинациями src->dst
            const duplicateConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field1',
                    message_types: [1001]
                },
                {
                    src: 'field1', // Дублирующееся src поле
                    dst: 'mapped_field1', // Дублирующееся dst поле - должна быть ошибка
                    message_types: [1002]
                }
            ];

            try {
                new FactMapper(duplicateConfig);
                this.assert(false, 'Валидация дублирующихся комбинаций src->dst', 'Должна была быть выброшена ошибка для дублирующихся комбинаций');
            } catch (error) {
                this.assert(error.message.includes('Найдены дублирующиеся комбинации src->dst'), 
                    'Валидация дублирующихся комбинаций src->dst', 'Корректная ошибка для дублирующихся комбинаций');
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
            // Тест с конфликтующими dst полями (разные src маппятся на одно dst)
            const conflictingConfig = [
                {
                    src: 'field1',
                    dst: 'mapped_field',
                    message_types: [4001]
                },
                {
                    src: 'field2', // Разное src поле
                    dst: 'mapped_field', // То же dst поле - должна быть ошибка
                    message_types: [4002]
                }
            ];

            try {
                new FactMapper(conflictingConfig);
                this.assert(false, 'Валидация конфликтующих dst полей', 'Должна была быть выброшена ошибка для конфликтующих dst полей');
            } catch (error) {
                this.assert(error.message.includes('Найдены конфликтующие dst поля'), 
                    'Валидация конфликтующих dst полей', 'Корректная ошибка для конфликтующих dst полей');
                this.assert(error.message.includes('field1 и field2'), 
                    'Детали конфликта в ошибке', 'Ошибка должна содержать информацию о конфликтующих src полях');
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

            // Тест с множественными конфликтами
            const multipleConflictsConfig = [
                {
                    src: 'field1',
                    dst: 'conflict_field',
                    message_types: [4006]
                },
                {
                    src: 'field2',
                    dst: 'conflict_field', // Первый конфликт
                    message_types: [4007]
                },
                {
                    src: 'field3',
                    dst: 'another_conflict',
                    message_types: [4008]
                },
                {
                    src: 'field4',
                    dst: 'another_conflict', // Второй конфликт
                    message_types: [4009]
                }
            ];

            try {
                new FactMapper(multipleConflictsConfig);
                this.assert(false, 'Валидация множественных конфликтов dst', 'Должна была быть выброшена ошибка для множественных конфликтов');
            } catch (error) {
                this.assert(error.message.includes('Найдены конфликтующие dst поля'), 
                    'Валидация множественных конфликтов dst', 'Корректная ошибка для множественных конфликтов');
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
