const CounterProducer = require('../generators/counterProducer');
const Logger = require('../utils/logger');

/**
 * Тесты для класса CounterProducer
 */
class CounterProducerTest {
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
        this.logger.info('=== Запуск тестов CounterProducer ===');
        
        this.testConstructorWithArray('1. Тест конструктора с массивом конфигурации...');
        this.testConstructorWithFile('2. Тест конструктора с файлом конфигурации...');
        this.testConstructorWithoutConfig('3. Тест конструктора без конфигурации...');
        this.testConditionMatching('4. Тест сопоставления условий...');
        this.testMongoOperators('5. Тест MongoDB операторов...');
        this.testMakeMethod('6. Тест метода make...');
        this.testHelperMethods('7. Тест вспомогательных методов...');
        this.testNowOperator('8. Тест оператора $$NOW...');
        this.testErrorHandling('10. Тест обработки ошибок...');
        
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
     * Тест конструктора с массивом конфигурации
     */
    testConstructorWithArray(title) {
        this.logger.info(title);
        try {
            const config = [
                {
                    name: 'test_counter_1',
                    comment: 'Тестовый счетчик 1',
                    indexTypeName: 'test_index_1',
                    computationConditions: { messageTypeId: [50, 70] },
                    evaluationConditions: null,
                    attributes: {
                        cnt: { $sum: 1 },
                        sum: { $sum: '$amount' }
                    }
                },
                {
                    name: 'test_counter_2',
                    comment: 'Тестовый счетчик 2',
                    indexTypeName: 'test_index_2',
                    computationConditions: { messageTypeId: [60] },
                    evaluationConditions: {
                        status: 'R'
                    },
                    attributes: {
                        cnt: { $sum: 1 },
                        rejected: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);
            
            this.assert(mongoCounters instanceof CounterProducer, 'Конструктор создает экземпляр CounterProducer');
            this.assert(mongoCounters.getCounterCount() === 2, 'Количество счетчиков корректно');
            
            this.assert(mongoCounters.getCounterDescription('test_counter_1') !== null, 'Счетчик test_counter_1 найден');
            this.assert(mongoCounters.getCounterDescription('test_counter_2') !== null, 'Счетчик test_counter_2 найден');
        } catch (error) {
            this.assert(false, 'Конструктор с массивом', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конструктора с файлом конфигурации
     */
    testConstructorWithFile(title) {
        this.logger.info(title);
        try {
            let mongoCounters = null;
            // В зависимости от способа запуска разные пути к файлу, не исправлять и не удалять!
            try {
                mongoCounters = new CounterProducer('../../countersConfig.json');
            } catch (error) {
                mongoCounters = new CounterProducer('./countersConfig.json');
            }
            
            this.assert(mongoCounters instanceof CounterProducer, 'Конструктор создает экземпляр CounterProducer');
            this.assert(mongoCounters.getCounterCount() > 0, 'Количество счетчиков корректно');
        } catch (error) {
            this.assert(false, 'Конструктор с файлом', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест конструктора без конфигурации
     */
    testConstructorWithoutConfig(title) {
        this.logger.info(title);
        try {
            const mongoCounters = new CounterProducer();
            
            this.assert(mongoCounters instanceof CounterProducer, 'Конструктор создает экземпляр CounterProducer');
            this.assert(mongoCounters.getCounterCount() === 0, 'Количество счетчиков равно 0');            
        } catch (error) {
            this.assert(false, 'Конструктор без конфигурации', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест сопоставления условий
     */
    testConditionMatching(title) {
        this.logger.info(title);
        try {
            const config = [
                {
                    name: 'counter_50_70',
                    comment: 'Счетчик для типов 50 и 70',
                    indexTypeName: 'counter_50_70_index',
                    computationConditions: { t: [50, 70] },
                    evaluationConditions: null,
                    attributes: { 
                        cnt: { $sum: 1 },
                        sum: { $sum: '$d.a' }
                    }
                },
                {
                    name: 'counter_status_a',
                    comment: 'Счетчик для статуса A',
                    indexTypeName: 'counter_status_a_index',
                    computationConditions: { "d.status": 'A' },
                    evaluationConditions: {
                        "d.a": { $gte: 1000 }
                    },
                    attributes: { 
                        cnt: { $sum: 1 },
                        approved: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);

            // Тест факта с messageTypeId = 50
            const fact1 = {
                _id: 'test1',
                t: 50,
                c: new Date(),
                d: { status: 'A' }
            };

            const result1 = mongoCounters.make(fact1);
            this.assert(typeof result1 === 'object', 'make возвращает объект');
            this.assert(result1.facetStages !== undefined, 'make возвращает объект с полем facetStages');
            this.assert(result1.facetStages?.counter_50_70 !== undefined, 'Счетчик counter_50_70 применен к факту с messageTypeId = 50');
            this.assert(result1.facetStages?.counter_status_a !== undefined, 'Счетчик counter_status_a применен к факту со статусом A');

            // Тест факта с messageTypeId = 60 (не подходит)
            const fact2 = {
                _id: 'test2',
                t: 60,
                c: new Date(),
                d: { messageTypeId: 60, status: 'A' }
            };

            const result2 = mongoCounters.make(fact2);
            this.assert(typeof result2 === 'object', 'make возвращает объект 2');
            this.assert(result2.facetStages !== undefined, 'make возвращает объект 2 с полем facetStages');
            this.assert(result2.facetStages?.counter_50_70 === undefined, 'Счетчик counter_50_70 не применен к факту с messageTypeId = 60');
            this.assert(result2.facetStages?.counter_status_a !== undefined, 'Счетчик counter_status_a применен к факту со статусом A');
        } catch (error) {
            this.assert(false, 'Сопоставление условий', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест MongoDB операторов
     */
    testMongoOperators(title) {
        this.logger.info(title);
        try {
            const config = [
                {
                    name: 'counter_nin',
                    comment: 'Счетчик с оператором $nin',
                    indexTypeName: 'counter_nin_index',
                    computationConditions: { 
                        mti: { $nin: ['0400', '0410'] },
                        status: { $ne: 'R' }
                    },
                    evaluationConditions: null,
                    attributes: { 
                        cnt: { $sum: 1 },
                        total: { $sum: 1 }
                    }
                },
                {
                    name: 'counter_regex',
                    comment: 'Счетчик с оператором $regex',
                    indexTypeName: 'counter_regex_index',
                    computationConditions: { 
                        de22: { $not: { $regex: '^(01|81)' } }
                    },
                    evaluationConditions: {
                        "d.a": { $gte: 1000 }
                    },
                    attributes: { 
                        cnt: { $sum: 1 },
                        total: { $sum: 1 }
                    }
                },
                {
                    name: 'counter_or',
                    comment: 'Счетчик с оператором $or',
                    indexTypeName: 'counter_or_index',
                    computationConditions: { 
                        status: { 
                            $or: [
                                { $ne: 'R' },
                                null,
                                { $exists: false }
                            ]
                        }
                    },
                    evaluationConditions: {
                        "d.f2": { $nin: ['value2', 'value4'] }
                    },
                    attributes: { 
                        cnt: { $sum: 1 },
                        total: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);

            // Тест с оператором $nin
            const fact1 = {
                _id: 'test1',
                t: 50,
                c: new Date(),
                d: { mti: '0200', status: 'A' }
            };

            const result1 = mongoCounters.make(fact1);
            this.assert(typeof result1 === 'object', 'make возвращает объект');
            this.assert(result1.facetStages.counter_nin !== undefined, 'Счетчик с $nin применен');
            this.assert(result1.facetStages.counter_regex !== undefined, 'Счетчик с $regex применен');
            this.assert(result1.facetStages.counter_or !== undefined, 'Счетчик с $or применен');

            // Тест с оператором $regex
            const fact2 = {
                _id: 'test2',
                t: 50,
                c: new Date(),
                d: { de22: '051234' }
            };

            const result2 = mongoCounters.make(fact2);
            this.assert(result2.facetStages.counter_regex !== undefined, 'Счетчик с $regex применен для корректного значения');

            // Тест с оператором $or
            const fact3 = {
                _id: 'test3',
                t: 50,
                c: new Date(),
                d: { status: 'A' }
            };

            const result3 = mongoCounters.make(fact3);
            this.assert(result3.facetStages.counter_or !== undefined, 'Счетчик с $or применен');
        } catch (error) {
            this.assert(false, 'MongoDB операторы', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест метода make
     */
    testMakeMethod(title) {
        this.logger.info(title);
        try {
            const config = [
                {
                    name: 'test_counter',
                    comment: 'Тестовый счетчик',
                    indexTypeName: 'test_counter_index',
                    computationConditions: { t: [50] },
                    evaluationConditions: {
                        status: 'A'
                    },
                    attributes: { 
                        cnt: { $sum: 1 },
                        count: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);

            // Тест с подходящим фактом
            const fact = {
                _id: 'test_fact',
                t: 50,
                c: new Date(),
                d: { messageTypeId: 50, status: 'A' }
            };

            const result = mongoCounters.make(fact);
            
            this.assert(typeof result === 'object', 'make возвращает объект');
            this.assert(result.facetStages !== undefined, 'make возвращает объект с полем facetStages');
            this.assert(result.facetStages?.test_counter !== undefined, 'Счетчик создан для подходящего факта');
            this.assert(Array.isArray(result.facetStages?.test_counter), 'Результат счетчика является массивом');
            this.assert(result.facetStages?.test_counter?.length === 2, 'Количество этапов aggregate корректно');

            // Тест с неподходящим фактом
            const unsuitableFact = {
                _id: 'unsuitable_fact',
                t: 60,
                c: new Date(),
                d: { messageTypeId: 60, status: 'A' }
            };

            const unsuitableResult = mongoCounters.make(unsuitableFact);
            
            this.assert(unsuitableResult === null, 'make возвращает null для неподходящего факта');
        } catch (error) {
            this.assert(false, 'Метод make', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест вспомогательных методов
     */
    testHelperMethods(title) {
        this.logger.info(title);
        try {
            const config = [
                {
                    name: 'counter1',
                    comment: 'Счетчик 1',
                    indexTypeName: 'counter1_index',
                    computationConditions: { type: 1 },
                    evaluationConditions: null,
                    attributes: { 
                        cnt: { $sum: 1 },
                        total: { $sum: 1 }
                    }
                },
                {
                    name: 'counter2',
                    comment: 'Счетчик 2',
                    indexTypeName: 'counter2_index',
                    computationConditions: { type: 2 },
                    evaluationConditions: {
                        "d.a": { $gte: 1000 }
                    },
                    attributes: { 
                        cnt: { $sum: 1 },
                        total: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);

            // Тест getCounterConfig
            const config1 = mongoCounters.getCounterDescription('counter1');
            this.assert(config1 !== null, 'getCounterDescription возвращает конфигурацию для существующего счетчика');
            this.assert(config1.name === 'counter1', 'getCounterDescription возвращает правильную конфигурацию');

            const configNotFound = mongoCounters.getCounterDescription('nonexistent');
            this.assert(configNotFound === null, 'getCounterDescription возвращает null для несуществующего счетчика');

            // Тест getCounterCount
            const count = mongoCounters.getCounterCount();
            this.assert(count === 2, 'getCounterCount возвращает правильное количество счетчиков');
        } catch (error) {
            this.assert(false, 'Вспомогательные методы', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест оператора $$NOW
     */
    testNowOperator(title) {
        this.logger.info(title);
        try {
            const config = [];
            const counterProducer = new CounterProducer(config);

            // Тест 1: $$NOW в простом сравнении $eq
            const fact1 = { d: { dt: new Date(Date.now() - 100) } }; // 100ms назад
            const condition1 = { 'd.dt': { '$eq': '$$NOW' } };
            const result1 = counterProducer._matchesCondition(fact1, condition1);
            this.assert(result1 === false, '$$NOW в простом сравнении $eq', 
                `Ожидалось false, получено ${result1}`);

            // Тест 2: $$NOW в сравнении $gte с прошлой датой
            const fact2 = { d: { dt: new Date(Date.now() - 100) } }; // 100ms назад
            const condition2 = { 'd.dt': { '$gte': '$$NOW' } };
            const result2 = counterProducer._matchesCondition(fact2, condition2);
            this.assert(result2 === false, '$$NOW в сравнении $gte с прошлой датой', 
                `Ожидалось false, получено ${result2}`);

            // Тест 3: $$NOW в сравнении $lte с прошлой датой
            const fact3 = { d: { dt: new Date(Date.now() - 100) } }; // 100ms назад
            const condition3 = { 'd.dt': { '$lte': '$$NOW' } };
            const result3 = counterProducer._matchesCondition(fact3, condition3);
            this.assert(result3 === true, '$$NOW в сравнении $lte с прошлой датой', 
                `Ожидалось true, получено ${result3}`);

            // Тест 4: $$NOW в сравнении $gt с прошлой датой
            const fact4 = { d: { dt: new Date(Date.now() - 1000) } }; // 1 секунда назад
            const condition4 = { 'd.dt': { '$gt': '$$NOW' } };
            const result4 = counterProducer._matchesCondition(fact4, condition4);
            this.assert(result4 === false, '$$NOW в сравнении $gt с прошлой датой', 
                `Ожидалось false, получено ${result4}`);

            // Тест 5: $$NOW в сравнении $lt с будущей датой
            const fact5 = { d: { dt: new Date(Date.now() + 1000) } }; // 1 секунда в будущем
            const condition5 = { 'd.dt': { '$lt': '$$NOW' } };
            const result5 = counterProducer._matchesCondition(fact5, condition5);
            this.assert(result5 === false, '$$NOW в сравнении $lt с будущей датой', 
                `Ожидалось false, получено ${result5}`);

            // Тест 6: $$NOW в $dateAdd - час назад
            const fact6 = { d: { dt: new Date(Date.now() - 30 * 60 * 1000) } }; // 30 минут назад
            const condition6 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'hour', 
                            'amount': -1 
                        } 
                    } 
                } 
            };
            const result6 = counterProducer._matchesCondition(fact6, condition6);
            this.assert(result6 === true, '$$NOW в $dateAdd - час назад', 
                `Ожидалось true, получено ${result6}`);

            // Тест 7: $$NOW в $dateAdd - день назад
            const fact7 = { d: { dt: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000) } }; // 2 дня назад
            const condition7 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'day', 
                            'amount': -1 
                        } 
                    } 
                } 
            };
            const result7 = counterProducer._matchesCondition(fact7, condition7);
            this.assert(result7 === false, '$$NOW в $dateAdd - день назад', 
                `Ожидалось false, получено ${result7}`);

            // Тест 8: $$NOW в $dateAdd - минута назад
            const fact8 = { d: { dt: new Date(Date.now() - 30 * 1000) } }; // 30 секунд назад
            const condition8 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'minute', 
                            'amount': -1 
                        } 
                    } 
                } 
            };
            const result8 = counterProducer._matchesCondition(fact8, condition8);
            this.assert(result8 === true, '$$NOW в $dateAdd - минута назад', 
                `Ожидалось true, получено ${result8}`);

            // Тест 9: $$NOW в комбинированном условии
            const fact9 = { d: { dt: new Date(Date.now() - 2 * 60 * 60 * 1000) } }; // 2 часа назад
            const condition9 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'hour', 
                            'amount': -3 
                        } 
                    },
                    '$lte': '$$NOW'
                } 
            };
            const result9 = counterProducer._matchesCondition(fact9, condition9);
            this.assert(result9 === true, '$$NOW в комбинированном условии', 
                `Ожидалось true, получено ${result9}`);

            // Тест 10: $$NOW в $dateAdd - невалидная единица времени
            const fact10 = { d: { dt: new Date() } };
            const condition10 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'invalid', 
                            'amount': -1 
                        } 
                    } 
                } 
            };
            const result10 = counterProducer._matchesCondition(fact10, condition10);
            this.assert(result10 === false, '$$NOW в $dateAdd - невалидная единица времени', 
                `Ожидалось false, получено ${result10}`);

            // Тест 11: $$NOW в $dateAdd - невалидная дата
            const fact11 = { d: { dt: new Date() } };
            const condition11 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': 'invalid-date', 
                            'unit': 'hour', 
                            'amount': -1 
                        } 
                    } 
                } 
            };
            const result11 = counterProducer._matchesCondition(fact11, condition11);
            this.assert(result11 === false, '$$NOW в $dateAdd - невалидная дата', 
                `Ожидалось false, получено ${result11}`);

            // Тест 12: $$NOW в $dateAdd - отсутствующие параметры
            const fact12 = { d: { dt: new Date() } };
            const condition12 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW'
                        } 
                    } 
                } 
            };
            const result12 = counterProducer._matchesCondition(fact12, condition12);
            this.assert(result12 === false, '$$NOW в $dateAdd - отсутствующие параметры', 
                `Ожидалось false, получено ${result12}`);

            // Тест 13: $$NOW в $dateAdd - нулевое количество
            const fact13 = { d: { dt: new Date(Date.now() - 100) } }; // 100ms назад
            const condition13 = { 
                'd.dt': { 
                    '$gte': { 
                        '$dateAdd': { 
                            'startDate': '$$NOW', 
                            'unit': 'hour', 
                            'amount': 0 
                        } 
                    } 
                } 
            };
            const result13 = counterProducer._matchesCondition(fact13, condition13);
            this.assert(result13 === false, '$$NOW в $dateAdd - нулевое количество', 
                `Ожидалось false, получено ${result13}`);

        } catch (error) {
            this.assert(false, 'Оператор $$NOW', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест обработки ошибок
     */
    testErrorHandling(title) {
        this.logger.info(title);
        try {
            // Тест с некорректной конфигурацией
            const invalidConfigs = [
                [{ name: 'test' }], // отсутствует computationConditions
                [{ name: 'test', computationConditions: {} }], // отсутствует evaluationConditions
                [{ name: 'test', computationConditions: {}, evaluationConditions: 'not_valid' }] // evaluationConditions не массив, объект или null
            ];

            for (const invalidConfig of invalidConfigs) {
                try {
                    new CounterProducer(invalidConfig);
                    this.assert(false, 'Обработка некорректной конфигурации', 'Должна была быть выброшена ошибка валидации');
                } catch (error) {
                    this.assert(error.message.includes('должен') || error.message.includes('не найден'), 
                        'Обработка некорректной конфигурации', 'Корректная ошибка валидации');
                }
            }

            // Тест с некорректным типом конфигурации (строка)
            try {
                new CounterProducer('not_an_array');
                this.assert(false, 'Обработка строки как конфигурации', 'Должна была быть выброшена ошибка');
            } catch (error) {
                this.assert(error.message.includes('должен') || error.message.includes('не найден'), 
                    'Обработка строки как конфигурации', 'Корректная ошибка валидации');
            }

            // Тест с некорректным фактом
            const config = [
                {
                    name: 'test_counter',
                    comment: 'Тестовый счетчик',
                    indexTypeName: 'test_counter_index',
                    computationConditions: { messageTypeId: [50] },
                    evaluationConditions: null,
                    attributes: { 
                        cnt: { $sum: 1 },
                        count: { $sum: 1 }
                    }
                }
            ];

            const mongoCounters = new CounterProducer(config);

            // Тест с null фактом
            try {
                const result = mongoCounters.make(null);
                this.assert(result === null, 'make возвращает null для null факта');
            } catch (error) {
                this.assert(false, 'Обработка null факта', `Неожиданная ошибка: ${error.message}`);
            }

            // Тест с фактом без поля d
            try {
                const factWithoutD = { _id: 'test', t: 50, c: new Date() };
                const result = mongoCounters.make(factWithoutD);
                this.assert(result === null, 'make возвращает null для факта без поля d');
            } catch (error) {
                this.assert(false, 'Обработка факта без поля d', `Неожиданная ошибка: ${error.message}`);
            }

        } catch (error) {
            this.assert(false, 'Обработка ошибок', `Ошибка: ${error.message}`);
        }
    }

    /**
     * Выводит результаты тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования CounterProducer ===');
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

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new CounterProducerTest();
    test.runAllTests();
}

module.exports = CounterProducerTest;
