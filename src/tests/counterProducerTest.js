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
        // testNowOperator перенесен в computationConditionsTest
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
