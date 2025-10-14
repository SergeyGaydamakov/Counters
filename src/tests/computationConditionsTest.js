const CounterProducer = require('../generators/counterProducer');

/**
 * Модуль тестирования для computationConditions в CounterProducer
 * Проверяет корректность обработки всех вариантов условий из countersCfg.json
 */
class ComputationConditionsTest {
    constructor() {
        this.logger = require('../utils/logger').fromEnv('LOG_LEVEL', 'INFO');
        this.testResults = [];
    }

    /**
     * Запускает все тесты для computationConditions
     */
    runAllTests() {
        this.logger.info('Запуск тестов для computationConditions в CounterProducer...');
        
        this.testBasicConditions();
        this.testComparisonOperators();
        this.testArrayOperators();
        this.testStringOperators();
        this.testLogicalOperators();
        this.testExprOperators();
        this.testComplexConditions();
        this.testEdgeCases();
        
        this.printResults();
    }

    /**
     * Тестирует базовые условия
     */
    testBasicConditions() {
        const testCases = [
            {
                name: 'Простое равенство',
                fact: { d: { typeId: 61 } },
                condition: { 'd.typeId': 61 },
                expected: true
            },
            {
                name: 'Простое неравенство',
                fact: { d: { typeId: 50 } },
                condition: { 'd.typeId': 61 },
                expected: false
            },
            {
                name: 'Строковое равенство',
                fact: { d: { mode: 'CI' } },
                condition: { 'd.mode': 'CI' },
                expected: true
            },
            {
                name: 'Null значение',
                fact: { d: { cli: null } },
                condition: { 'd.cli': null },
                expected: true
            },
            {
                name: 'Undefined значение',
                fact: { d: { missingField: undefined } },
                condition: { 'd.missingField': undefined },
                expected: true
            }
        ];

        this.runTestGroup('Базовые условия', testCases);
    }

    /**
     * Тестирует операторы сравнения
     */
    testComparisonOperators() {
        const testCases = [
            {
                name: '$eq оператор',
                fact: { d: { Amount: 100 } },
                condition: { 'd.Amount': { $eq: 100 } },
                expected: true
            },
            {
                name: '$ne оператор',
                fact: { d: { src: 'Card' } },
                condition: { 'd.src': { $ne: 'ATM' } },
                expected: true
            },
            {
                name: '$gt оператор',
                fact: { d: { Amount: 500 } },
                condition: { 'd.Amount': { $gt: 100 } },
                expected: true
            },
            {
                name: '$gte оператор',
                fact: { d: { Amount: 100 } },
                condition: { 'd.Amount': { $gte: 100 } },
                expected: true
            },
            {
                name: '$lt оператор',
                fact: { d: { Amount: 50 } },
                condition: { 'd.Amount': { $lt: 100 } },
                expected: true
            },
            {
                name: '$lte оператор',
                fact: { d: { Amount: 100 } },
                condition: { 'd.Amount': { $lte: 100 } },
                expected: true
            },
            {
                name: 'Строковые числа в $gte',
                fact: { d: { Amount: '600,000' } },
                condition: { 'd.Amount': { $gte: '500,000' } },
                expected: true
            }
        ];

        this.runTestGroup('Операторы сравнения', testCases);
    }

    /**
     * Тестирует операторы для массивов
     */
    testArrayOperators() {
        const testCases = [
            {
                name: '$in оператор',
                fact: { d: { typeId: 61 } },
                condition: { 'd.typeId': { $in: [61, 50] } },
                expected: true
            },
            {
                name: '$nin оператор',
                fact: { d: { typeId: 30 } },
                condition: { 'd.typeId': { $nin: [61, 50] } },
                expected: true
            },
            {
                name: '$in с пустым массивом',
                fact: { d: { cli: 'test' } },
                condition: { 'd.cli': { $in: [] } },
                expected: false
            },
            {
                name: '$nin с пустым массивом',
                fact: { d: { cli: 'test' } },
                condition: { 'd.cli': { $nin: [] } },
                expected: true
            },
            {
                name: '$all оператор',
                fact: { d: { tags: ['tag1', 'tag2', 'tag3'] } },
                condition: { 'd.tags': { $all: ['tag1', 'tag2'] } },
                expected: true
            },
            {
                name: '$size оператор',
                fact: { d: { items: [1, 2, 3] } },
                condition: { 'd.items': { $size: 3 } },
                expected: true
            }
        ];

        this.runTestGroup('Операторы для массивов', testCases);
    }

    /**
     * Тестирует операторы для строк
     */
    testStringOperators() {
        const testCases = [
            {
                name: '$regex оператор',
                fact: { d: { something: 'atm88.1 triggered' } },
                condition: { 'd.something': { $regex: 'atm88.1', $options: 'i' } },
                expected: true
            },
            {
                name: '$regex с флагами',
                fact: { d: { something: 'ATM88.1 TRIGGERED' } },
                condition: { 'd.something': { $regex: 'atm88.1', $options: 'i' } },
                expected: true
            },
            {
                name: '$regex с якорем начала',
                fact: { d: { anyField: '22007001' } },
                condition: { 'd.anyField': { $regex: '^22007001', $options: 'i' } },
                expected: true
            },
            {
                name: '$regex с множественными вариантами',
                fact: { d: { anyField: '553691' } },
                condition: { 'd.anyField': { $regex: '^(22007001|553691|521324)', $options: 'i' } },
                expected: true
            },
            {
                name: '$not с $regex',
                fact: { d: { doc: '123456' } },
                condition: { 'd.doc': { $not: { $regex: '^7', $options: 'i' } } },
                expected: true
            }
        ];

        this.runTestGroup('Операторы для строк', testCases);
    }

    /**
     * Тестирует логические операторы
     */
    testLogicalOperators() {
        const testCases = [
            {
                name: '$not оператор',
                fact: { d: { field: 'value' } },
                condition: { 'd.field': { $not: { $eq: 'other' } } },
                expected: true
            },
            {
                name: '$and оператор',
                fact: { d: { Amount: 500, typeId: 61 } },
                condition: { 'd.Amount': { $and: [{ $gt: 100 }, { $lt: 1000 }] } },
                expected: true
            },
            {
                name: '$or оператор',
                fact: { d: { typeId: 50 } },
                condition: { 'd.typeId': { $or: [{ $eq: 61 }, { $eq: 50 }] } },
                expected: true
            },
            {
                name: 'Комбинированные условия',
                fact: { d: { typeId: 61, mode: 'CI', src: 'ATM' } },
                condition: { 
                    'd.typeId': 61,
                    'd.mode': 'CI',
                    'd.src': { $ne: 'Card' }
                },
                expected: true
            }
        ];

        this.runTestGroup('Логические операторы', testCases);
    }

    /**
     * Тестирует операторы $expr
     */
    testExprOperators() {
        const testCases = [
            {
                name: '$expr с $ne',
                fact: { d: { num: '1234567890', anyField: '0987654321' } },
                condition: { 
                    '$expr': { 
                        '$ne': ['$d.num', '$d.anyField'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с $eq',
                fact: { d: { num: '1234567890', anyField: '1234567890' } },
                condition: { 
                    '$expr': { 
                        '$eq': ['$d.num', '$d.anyField'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с $gt',
                fact: { d: { Amount: 500, Threshold: 100 } },
                condition: { 
                    '$expr': { 
                        '$gt': ['$d.Amount', '$d.Threshold'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с $gte',
                fact: { d: { Amount: 100, Threshold: 100 } },
                condition: { 
                    '$expr': { 
                        '$gte': ['$d.Amount', '$d.Threshold'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с $lt',
                fact: { d: { Amount: 50, Threshold: 100 } },
                condition: { 
                    '$expr': { 
                        '$lt': ['$d.Amount', '$d.Threshold'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с $lte',
                fact: { d: { Amount: 100, Threshold: 100 } },
                condition: { 
                    '$expr': { 
                        '$lte': ['$d.Amount', '$d.Threshold'] 
                    } 
                },
                expected: true
            },
            {
                name: '$expr с вложенными полями',
                fact: { d: { client: { id: '123' }, user: { id: '456' } } },
                condition: { 
                    '$expr': { 
                        '$ne': ['$d.client.id', '$d.user.id'] 
                    } 
                },
                expected: true
            }
        ];

        this.runTestGroup('Операторы $expr', testCases);
    }

    /**
     * Тестирует сложные комбинированные условия
     */
    testComplexConditions() {
        const testCases = [
            {
                name: 'Комбинация базовых условий и $expr',
                fact: { 
                    d: { 
                        typeId: 61, 
                        mode: 'CI', 
                        num: '1234567890', 
                        anyField: '0987654321' 
                    } 
                },
                condition: { 
                    'd.typeId': 61,
                    'd.mode': 'CI',
                    '$expr': { 
                        '$ne': ['$d.num', '$d.anyField'] 
                    } 
                },
                expected: true
            },
            {
                name: 'Комбинация операторов и $expr',
                fact: { 
                    d: { 
                        typeId: 61, 
                        mode: 'CI',
                        src: 'ATM',
                        ptype: 2,
                        num: '1234567890', 
                        anyField: '0987654321' 
                    } 
                },
                condition: { 
                    'd.typeId': 61,
                    'd.mode': 'CI',
                    'd.src': { $ne: 'Card' },
                    'd.ptype': { $ne: 1 },
                    '$expr': { 
                        '$ne': ['$d.num', '$d.anyField'] 
                    } 
                },
                expected: true
            },
            {
                name: 'Комбинация с regex и $expr',
                fact: { 
                    d: { 
                        typeId: 61, 
                        mode: 'CI',
                        anyField: '22007001',
                        num: '1234567890'
                    } 
                },
                condition: { 
                    'd.typeId': 61,
                    'd.mode': 'CI',
                    'd.anyField': { 
                        $regex: '^(22007001|553691|521324)', 
                        $options: 'i' 
                    },
                    '$expr': { 
                        '$ne': ['$d.num', '$d.anyField'] 
                    } 
                },
                expected: true
            }
        ];

        this.runTestGroup('Сложные комбинированные условия', testCases);
    }

    /**
     * Тестирует граничные случаи
     */
    testEdgeCases() {
        const testCases = [
            {
                name: 'Отсутствующие поля в $expr',
                fact: { d: { num: '1234567890' } },
                condition: { 
                    '$expr': { 
                        '$ne': ['$d.num', '$d.missingField'] 
                    } 
                },
                expected: true // '1234567890' !== undefined = true
            },
            {
                name: 'Оба поля отсутствуют в $expr',
                fact: { d: {} },
                condition: { 
                    '$expr': { 
                        '$eq': ['$d.missingField1', '$d.missingField2'] 
                    } 
                },
                expected: true // undefined === undefined = true
            },
            {
                name: 'Пустые условия',
                fact: { d: { field: 'value' } },
                condition: {},
                expected: true
            },
            {
                name: 'Null условия',
                fact: { d: { field: 'value' } },
                condition: null,
                expected: true
            },
            {
                name: 'Факт без поля d',
                fact: { otherField: 'value' },
                condition: { 'd.field': 'value' },
                expected: false
            },
            {
                name: 'Null факт',
                fact: null,
                condition: { 'd.field': 'value' },
                expected: false
            },
            {
                name: 'Строковые числа в $expr',
                fact: { d: { Amount: '600,000', Threshold: '500,000' } },
                condition: { 
                    '$expr': { 
                        '$gt': ['$d.Amount', '$d.Threshold'] 
                    } 
                },
                expected: true
            }
        ];

        this.runTestGroup('Граничные случаи', testCases);
    }

    /**
     * Запускает группу тестов
     */
    runTestGroup(groupName, testCases) {
        this.logger.info(`Тестирование: ${groupName}`);
        
        const counterProducer = new CounterProducer([]);
        let passed = 0;
        let failed = 0;

        for (const testCase of testCases) {
            try {
                const result = counterProducer._matchesCondition(testCase.fact, testCase.condition);
                if (result === testCase.expected) {
                    passed++;
                    this.logger.debug(`✅ ${testCase.name}: ${result}`);
                } else {
                    failed++;
                    this.logger.error(`❌ ${testCase.name}: ожидалось ${testCase.expected}, получено ${result}`);
                    this.logger.debug(`   Факт: ${JSON.stringify(testCase.fact)}`);
                    this.logger.debug(`   Условие: ${JSON.stringify(testCase.condition)}`);
                }
            } catch (error) {
                failed++;
                this.logger.error(`💥 Ошибка в тесте "${testCase.name}": ${error.message}`);
                this.logger.debug(`   Факт: ${JSON.stringify(testCase.fact)}`);
                this.logger.debug(`   Условие: ${JSON.stringify(testCase.condition)}`);
            }
        }

        this.testResults.push({
            group: groupName,
            passed,
            failed,
            total: testCases.length
        });

        this.logger.info(`${groupName}: ${passed}/${testCases.length} тестов прошли успешно`);
    }

    /**
     * Выводит результаты тестирования
     */
    printResults() {
        this.logger.info('\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ COMPUTATION CONDITIONS ===');
        
        let totalPassed = 0;
        let totalFailed = 0;
        let totalTests = 0;

        for (const result of this.testResults) {
            this.logger.info(`${result.group}: ${result.passed}/${result.total} (${result.failed} неудачных)`);
            totalPassed += result.passed;
            totalFailed += result.failed;
            totalTests += result.total;
        }

        this.logger.info(`\nОбщий результат: ${totalPassed}/${totalTests} тестов прошли успешно`);
        
        if (totalFailed === 0) {
            this.logger.info('✅ Все тесты computationConditions прошли успешно!');
        } else {
            this.logger.warn(`❌ ${totalFailed} тестов computationConditions не прошли`);
        }

        // Дополнительная статистика
        const successRate = ((totalPassed / totalTests) * 100).toFixed(2);
        this.logger.info(`📊 Процент успешности: ${successRate}%`);
    }

    /**
     * Запускает тест для конкретного случая (для отладки)
     */
    debugTestCase(fact, condition, expected) {
        this.logger.info('=== ОТЛАДОЧНЫЙ ТЕСТ ===');
        this.logger.info(`Факт: ${JSON.stringify(fact)}`);
        this.logger.info(`Условие: ${JSON.stringify(condition)}`);
        this.logger.info(`Ожидаемый результат: ${expected}`);
        
        const counterProducer = new CounterProducer([]);
        const result = counterProducer._matchesCondition(fact, condition);
        
        this.logger.info(`Полученный результат: ${result}`);
        this.logger.info(`Тест ${result === expected ? 'ПРОШЕЛ' : 'НЕ ПРОШЕЛ'}`);
        
        return result === expected;
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new ComputationConditionsTest();
    test.runAllTests();
}

module.exports = ComputationConditionsTest;
