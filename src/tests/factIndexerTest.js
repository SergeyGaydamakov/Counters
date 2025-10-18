const { FactIndexer, MessageGenerator, FactMapper } = require('../index');
const Logger = require('../utils/logger');

/**
 * Тесты для класса FactIndexer (создание индексных значений)
 */
class FactIndexerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');

        // Локальная конфигурация для тестирования
        this.testIndexConfig = [
            {
                fieldName: "f1",
                dateName: "dt",
                indexTypeName: "test_type_1",
                indexType: 1,
                indexValue: 1
            },
            {
                fieldName: "f2",
                dateName: "dt",
                indexTypeName: "test_type_2",
                indexType: 2,
                indexValue: 2
            },
            {
                fieldName: "f3",
                dateName: "dt",
                indexTypeName: "test_type_3",
                indexType: 3,
                indexValue: 1
            },
            {
                fieldName: "f4",
                dateName: "dt",
                indexTypeName: "test_type_4",
                indexType: 4,
                indexValue: 1
            },
            {
                fieldName: "f5",
                dateName: "dt",
                indexTypeName: "test_type_5",
                indexType: 5,
                indexValue: 2
            },
            {
                fieldName: "f6",
                dateName: "dt",
                indexTypeName: "test_type_6",
                indexType: 6,
                indexValue: 2
            },
            {
                fieldName: "f7",
                dateName: "dt",
                indexTypeName: "test_type_7",
                indexType: 7,
                indexValue: 1
            }
        ];

        this.indexer = new FactIndexer(this.testIndexConfig, true); // includeFactData = true для тестов

        // Тестовая конфигурация полей
        this.testFieldConfig = [
            {
                "src": "dt",
                "dst": "dt",
                "message_types": [1, 2, 3], // user_action, system_message, payment
                "generator": {
                    "type": "date",
                    "min": "2024-01-01",
                    "max": "2024-06-30"
                }
            },
            {
                "src": "f1",
                "dst": "f1",
                "message_types": [1, 2, 3], // user_action, system_message, payment
                "key_type": 1
            },
            {
                "src": "f2",
                "dst": "f2",
                "message_types": [1, 3] // user_action, payment
            },
            {
                "src": "f3",
                "dst": "f3",
                "message_types": [2, 3] // system_message, payment
            },
            {
                "src": "f4",
                "dst": "f4",
                "message_types": [1] // user_action
            },
            {
                "src": "f5",
                "dst": "f5",
                "message_types": [2] // system_message
            }
        ];

        this.generator = new MessageGenerator(this.testFieldConfig);
        this.mapper = new FactMapper(this.testFieldConfig);
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
    }

    /**
     * Запуск всех тестов
     */
    async runAllTests() {
        this.logger.info('Starting FactIndexer tests...');
        this.logger.debug('=== Тестирование FactIndexer ===\n');

        this.testConfigValidation('1. Тест валидации конфигурации...');
        this.testBasicIndexing('2. Тест базового создания индексных значений...');
        this.testMultipleFields('3. Тест с множественными полями...');
        this.testNoFields('4. Тест без полей fN...');
        this.testInvalidInput('5. Тест с неверными входными данными...');
        this.testMultipleFacts('6. Тест с множественными фактами...');
        this.testStatistics('7. Тест структуры индексных значений...');
        this.testWithGeneratedFacts('8. Тест с сгенерированными фактами...');
        this.testDateNameField('9. Тест с полем dateName...');

        this.printResults();
    }

    /**
     * Тест базового создания индексных значений
     */
    testBasicIndexing(title) {
        this.logger.debug(title);

        const fact = {
            _id: 'test-id-123',
            t: 1,
            c: new Date('2024-01-01'),
            d: {
                dt: new Date('2024-01-15'),
                f1: 'value1',
                f2: 'value2',
                f5: 'value5'
            }
        };

        try {
            const indexValues = this.indexer.index(fact);

            // Проверяем количество индексных значений (должно быть 3 для f1, f2, f5)
            if (indexValues.length !== 3) {
                throw new Error(`Ожидалось 3 индексных значения, получено ${indexValues.length}`);
            }

            // Проверяем структуру индексного значения
            const requiredFields = ['_id', 'd', 'c', 'it', 'v', 't'];
            for (const field of requiredFields) {
                if (!(field in indexValues[0])) {
                    throw new Error(`Отсутствует поле ${field} в индексном значении`);
                }
            }

            // Проверяем, что it содержит правильные типы индексов
            const indexTypes = indexValues.map(iv => iv.it).sort();
            const expectedTypes = [1, 2, 5].sort();
            if (JSON.stringify(indexTypes) !== JSON.stringify(expectedTypes)) {
                throw new Error(`Неправильные типы индексов: ожидалось [1,2,5], получено [${indexTypes.join(',')}]`);
            }

            // Проверяем, что v содержит правильные значения полей
            const fieldValues = indexValues.map(iv => iv.v).sort();
            const expectedValues = ['value1', 'value2', 'value5'].sort();
            if (JSON.stringify(fieldValues) !== JSON.stringify(expectedValues)) {
                throw new Error(`Неправильные значения полей: ожидалось [value1,value2,value5], получено [${fieldValues.join(',')}]`);
            }

            // Проверяем, что h содержит правильные значения (хеш для f1 и f5, само значение для f2)
            const f1Index = indexValues.find(iv => iv.v === 'value1');
            const f2Index = indexValues.find(iv => iv.v === 'value2');
            const f5Index = indexValues.find(iv => iv.v === 'value5');

            // f1 должен иметь хеш (indexValue = 1), f5 должен иметь само значение (indexValue = 2)
            if (f1Index._id.h.length !==28) {
                throw new Error(`f1 должен иметь хеш длиной 28 символов (Base64), получено ${f1Index._id.h.length}`);
            }
            if (f5Index._id.h !== '5:value5') {
                throw new Error(`f5 должен иметь само значение поля, получено ${f5Index._id.h}`);
            }

            // f2 должен иметь само значение поля (indexValue = 2)
            if (f2Index._id.h !== '2:value2') {
                throw new Error(`f2 должен иметь само значение поля, получено ${f2Index._id.h}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testBasicIndexing: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест с множественными полями для создания индексных значений
     */
    testMultipleFields(title) {
        this.logger.debug(title);

        const fact = {
            _id: 'multi-field-test',
            t: 2,
            c: new Date('2024-02-01'),
            d: {
                dt: new Date('2024-02-15'),
                f1: 'val1',
                f2: 'val2',
                f3: 'val3',
                f4: 'val4',
                f6: 'val6',
                f7: 'val7',
                otherField: 'should be ignored',
                notF: 'should be ignored'
            }
        };

        try {
            const indexValues = this.indexer.index(fact);

            if (indexValues.length !== 6) {
                throw new Error(`Ожидалось 6 индексных значений, получено ${indexValues.length}`);
            }

            // Проверяем, что все индексные значения имеют правильные поля
            indexValues.forEach(indexValue => {
                if (indexValue._id.f !== 'multi-field-test') {
                    throw new Error('Неправильный ID факта в индексном значении');
                }

                // Проверяем структуру
                const requiredFields = ['_id', 'd', 'c', 'it', 'v', 't'];
                for (const field of requiredFields) {
                    if (!(field in indexValue)) {
                        throw new Error(`Отсутствует поле ${field} в индексном значении`);
                    }
                }
            });

            // Проверяем типы индексов
            const indexTypes = indexValues.map(iv => iv.it).sort();
            const expectedTypes = [1, 2, 3, 4, 6, 7].sort();
            if (JSON.stringify(indexTypes) !== JSON.stringify(expectedTypes)) {
                throw new Error(`Неправильные типы индексов: ожидалось [1,2,3,4,6,7], получено [${indexTypes.join(',')}]`);
            }

            // Проверяем значения полей
            const fieldValues = indexValues.map(iv => iv.v).sort();
            const expectedValues = ['val1', 'val2', 'val3', 'val4', 'val6', 'val7'].sort();
            if (JSON.stringify(fieldValues) !== JSON.stringify(expectedValues)) {
                throw new Error(`Неправильные значения полей: ожидалось [val1,val2,val3,val4,val6,val7], получено [${fieldValues.join(',')}]`);
            }

            // Проверяем правильность вычисления h (хеш для f1, f3, f10, f23; само значение для f2, f15)
            const hashFields = ['val1', 'val3', 'val4', 'val7'];
            const valueFields = ['val2', 'val6'];

            hashFields.forEach(val => {
                const indexValue = indexValues.find(iv => iv.v === val);
                if (indexValue._id.h.length !== 28) {
                    throw new Error(`Поле ${val} должно иметь хеш длиной 28 символов (Base64), получено ${indexValue._id.h.length}`);
                }
            });

            valueFields.forEach(val => {
                const indexValue = indexValues.find(iv => iv.v === val);
                if (indexValue._id.h !== `${indexValue.it}:${val}`) {
                    throw new Error(`Поле ${val} должно иметь само значение, получено ${indexValue._id.h}`);
                }
            });

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testMultipleFields: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест без полей fN (не создаются индексные значения)
     */
    testNoFields(title) {
        this.logger.debug(title);

        const fact = {
            _id: 'no-fields-test',
            t: 3,
            c: new Date('2024-03-01'),
            d: {
                dt: new Date('2024-03-15'),
                otherField: 'value',
                anotherField: 'another value'
            }
        };

        try {
            const indexValues = this.indexer.index(fact);

            if (indexValues.length !== 0) {
                throw new Error(`Ожидалось 0 индексных значений, получено ${indexValues.length}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testNoFields: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест с неверными входными данными
     */
    testInvalidInput(title) {
        this.logger.debug(title);

        const testCases = [
            { input: null, description: 'null' },
            { input: undefined, description: 'undefined' },
            { input: 'string', description: 'строка' },
            { input: 123, description: 'число' },
            { input: [], description: 'массив' },
            { input: { i: 'test' }, description: 'объект без обязательных полей' }
        ];

        let errorCount = 0;
        testCases.forEach(testCase => {
            try {
                this.indexer.index(testCase.input);
                errorCount++;
                this.logger.error(`   ✗ ${testCase.description}: должен был выбросить ошибку`);
            } catch (error) {
                // Ожидаемая ошибка
            }
        });

        if (errorCount === 0) {
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } else {
            this.testResults.failed++;
            this.testResults.errors.push(`testInvalidInput: ${errorCount} тестов не прошли валидацию`);
            this.logger.error(`   ✗ ${errorCount} тестов не прошли валидацию`);
        }
    }

    /**
     * Тест создания индексных значений для множественных фактов
     */
    testMultipleFacts(title) {
        this.logger.debug(title);

        const facts = [
            {
                _id: 'fact1',
                t: 1,
                c: new Date('2024-01-01'),
                d: {
                    dt: new Date('2024-01-15'),
                    f1: 'val1',
                    f2: 'val2'
                }
            },
            {
                _id: 'fact2',
                t: 2,
                c: new Date('2024-02-01'),
                d: {
                    dt: new Date('2024-02-15'),
                    f3: 'val3',
                    f5: 'val5'
                }
            }
        ];

        try {
            const indexValues = this.indexer.indexFacts(facts);

            if (indexValues.length !== 4) {
                throw new Error(`Ожидалось 4 индексных значения, получено ${indexValues.length}`);
            }

            // Проверяем, что индексные значения содержат правильные ID фактов
            const factIds = indexValues.map(idx => idx._id.f);
            if (!factIds.includes('fact1') || !factIds.includes('fact2')) {
                throw new Error('Неправильные ID фактов в индексных значениях');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testMultipleFacts: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест структуры индексных значений
     */
    testStatistics(title) {
        this.logger.debug(title);

        const fact = {
            _id: 'statistics-test',
            t: 1,
            c: new Date('2024-01-01'),
            d: {
                dt: new Date('2024-01-15'),
                f1: 'test_value_1',
                f2: 'test_value_2'
            }
        };

        try {
            const indexValues = this.indexer.index(fact);

            // Проверяем базовую структуру индексных значений
            if (indexValues.length !== 2) {
                throw new Error(`Неправильное количество индексных значений: ${indexValues.length}`);
            }

            // Проверяем, что все индексные значения имеют правильную структуру
            indexValues.forEach((indexValue, i) => {
                const requiredFields = ['_id', 'd', 'c', 'it', 'v', 't'];
                for (const field of requiredFields) {
                    if (!(field in indexValue)) {
                        throw new Error(`Отсутствует поле ${field} в индексном значении ${i}`);
                    }
                }
            });

            // Проверяем типы данных
            indexValues.forEach((indexValue, i) => {
                if (typeof indexValue._id.h !== 'string') {
                    throw new Error(`Поле h должно быть строкой в индексном значении ${i}`);
                }
                if (typeof indexValue._id.f !== 'string') {
                    throw new Error(`Поле f должно быть строкой в индексном значении ${i}`);
                }
                if (typeof indexValue.it !== 'number') {
                    throw new Error(`Поле it должно быть числом в индексном значении ${i}`);
                }
                if (typeof indexValue.v !== 'string') {
                    throw new Error(`Поле v должно быть строкой в индексном значении ${i}`);
                }
                if (typeof indexValue.t !== 'number') {
                    throw new Error(`Поле t должно быть числом в индексном значении ${i}`);
                }
                if (typeof indexValue.d !== 'object' || indexValue.d === null) {
                    throw new Error(`Поле d должно быть объектом в индексном значении ${i}`);
                }
                if (!(indexValue.c instanceof Date)) {
                    throw new Error(`Поле c должно быть датой в индексном значении ${i}`);
                }
            });

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testStatistics: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест создания индексных значений для сгенерированных фактов
     */
    testWithGeneratedFacts(title) {
        this.logger.debug(title);

        try {
            // Генерируем несколько фактов
            const facts = [];
            for (let i = 0; i < 5; i++) {
                const message = this.generator.generateRandomTypeMessage();
                const fact = this.mapper.mapMessageToFact(message);
                facts.push(fact);
            }

            const indexValues = this.indexer.indexFacts(facts);

            if (indexValues.length === 0) {
                throw new Error('Не создано ни одного индексного значения');
            }

            // Проверяем, что все индексные значения имеют правильную структуру
            indexValues.forEach(indexValue => {
                const requiredFields = ['_id', 'dt', 'c'];
                for (const field of requiredFields) {
                    if (!(field in indexValue)) {
                        throw new Error(`Отсутствует поле ${field} в индексном значении`);
                    }
                }
            });

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testWithGeneratedFacts: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест с полем dateName
     */
    testDateNameField(title) {
        this.logger.debug(title);

        try {
            // Создаем конфигурацию с разными полями дат
            const indexConfig = [
                {
                    fieldName: "f1",
                    dateName: "customDate",
                    indexTypeName: "test_type_1",
                    indexType: 1,
                    indexValue: 1
                },
                {
                    fieldName: "f2",
                    dateName: "dt",
                    indexTypeName: "test_type_2",
                    indexType: 2,
                    indexValue: 2
                }
            ];

            const indexer = new FactIndexer(indexConfig);

            // Тестовый факт с разными полями дат
            const fact = {
                _id: "test_id_123",
                t: 1,
                c: new Date('2024-01-02'),
                d: {
                    f1: "test_value_1",
                    f2: "test_value_2",
                    customDate: new Date('2024-03-15'),
                    dt: new Date('2024-04-20')
                }
            };

            const indexValues = indexer.index(fact);

            // Проверяем, что созданы индексные значения
            if (indexValues.length !== 2) {
                throw new Error(`Ожидалось 2 индексных значения, получено: ${indexValues.length}`);
            }

            // Проверяем первое индексное значение (использует customDate)
            const index1 = indexValues.find(iv => iv.it === 1);
            if (!index1) {
                throw new Error('Не найдено индексное значение для f1');
            }
            if (index1.dt.getTime() !== fact.d.customDate.getTime()) {
                throw new Error('Дата в первом индексном значении не соответствует customDate');
            }

            // Проверяем второе индексное значение (использует dt)
            const index2 = indexValues.find(iv => iv.it === 2);
            if (!index2) {
                throw new Error('Не найдено индексное значение для f2');
            }
            if (index2.dt.getTime() !== fact.d.dt.getTime()) {
                throw new Error('Дата во втором индексном значении не соответствует dt');
            }

            // Тест с невалидной датой
            const factWithInvalidDate = {
                _id: "test_id_456",
                t: 1,
                c: new Date('2024-01-02'),
                d: {
                    dt: new Date('2024-01-01'),
                    f1: "test_value_1",
                    customDate: "invalid_date_string" // невалидная дата
                }
            };

            const indexValuesInvalid = indexer.index(factWithInvalidDate);
            const indexInvalid = indexValuesInvalid.find(iv => iv.it === 1);
            if (indexInvalid) {
                throw new Error('Найдено индексное значение для факта с невалидной датой.');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testDateNameField: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест валидации конфигурации
     */
    testConfigValidation(title) {
        this.logger.debug(title);

        const testCases = [
            {
                config: 'string',
                description: 'строка вместо массива',
                shouldThrow: true
            },
            {
                config: [],
                description: 'пустой массив',
                shouldThrow: true
            },
            {
                config: [{}],
                description: 'объект без обязательных полей',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 1,
                    indexValue: 1
                }],
                description: 'валидная конфигурация',
                shouldThrow: false
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 1,
                    indexValue: 1
                }, {
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 2,
                    indexValue: 2
                }],
                description: 'дублирующаяся комбинация fieldName + indexTypeName',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 1,
                    indexValue: 1
                }, {
                    fieldName: 'f2',
                    dateName: 'dt',
                    indexTypeName: 'test2',
                    indexType: 1,
                    indexValue: 2
                }],
                description: 'дублирующийся indexType',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'invalid_field',
                    indexTypeName: 'test',
                    indexType: 1,
                    indexValue: 1
                }],
                description: 'неправильное название поля',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 123,
                    indexType: 1,
                    indexValue: 1
                }],
                description: 'indexTypeName не строка',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 'invalid',
                    indexValue: 1
                }],
                description: 'indexType не число',
                shouldThrow: true
            },
            {
                config: [{
                    fieldName: 'f1',
                    dateName: 'dt',
                    indexTypeName: 'test',
                    indexType: 1,
                    indexValue: 3
                }],
                description: 'неправильное значение indexValue',
                shouldThrow: true
            }
        ];

        let errorCount = 0;
        testCases.forEach((testCase, index) => {
            try {
                new FactIndexer(testCase.config);
                if (testCase.shouldThrow) {
                    errorCount++;
                    this.logger.error(`   ✗ Тест ${index + 1} (${testCase.description}): должен был выбросить ошибку`);
                }
            } catch (error) {
                if (!testCase.shouldThrow) {
                    errorCount++;
                    this.logger.error(`   ✗ Тест ${index + 1} (${testCase.description}): не должен был выбросить ошибку: ${error.message}`);
                }
            }
        });

        if (errorCount === 0) {
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } else {
            this.testResults.failed++;
            this.testResults.errors.push(`testConfigValidation: ${errorCount} тестов не прошли валидацию`);
            this.logger.error(`   ✗ ${errorCount} тестов не прошли валидацию`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования FactIndexer ===');
        this.logger.info(`Пройдено: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);

        if (this.testResults.errors.length > 0) {
            this.logger.error('\nОшибки:');
            this.testResults.errors.forEach(error => {
                this.logger.error(`  - ${error}`);
            });
        }

        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? (this.testResults.passed / total * 100).toFixed(1) : 0;
        this.logger.info(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new FactIndexerTest();
    test.runAllTests()
        .then(() => {
            process.exit(0);
        })
        .catch((error) => {
            this.logger.error('Критическая ошибка:', error.message);
            process.exit(1);
        });
}

module.exports = FactIndexerTest;
