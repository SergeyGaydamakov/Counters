const { MongoProvider, FactController} = require('../index');
const Logger = require('../utils/logger');
const EnvConfig = require('../utils/envConfig');

/**
 * Тесты для всех методов FactController
 */
class FactControllerTest {
    // Тестовая конфигурация полей фактов
    _testFieldConfig = [
        {
            "src": "dt",
            "dst": "dt",
            "event_types": [1, 2, 3],
            "generator": {
                "type": "date",
                "min": "2025-01-01",
                "max": "2025-10-01"
            }
        },
        {
            "src": "a",
            "dst": "a",
            "event_types": [1, 2, 3],
            "generator": {
                "type": "integer",
                "min": 1,
                "max": 10000000,
                "default_value": 1000,
                "default_random": 0.1
            }
        },
        {
            "src": "f1",
            "dst": "f1",
            "event_types": [1, 2, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            },
            "key_type": 1
        },
        {
            "src": "f2",
            "dst": "f2",
            "event_types": [1, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            }
        },
        {
            "src": "f3",
            "dst": "f3",
            "event_types": [2, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            }
        }
    ];

    // Локальная конфигурация индексных значений
    _testIndexConfig = [
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
        }
    ];
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        const mongoConfig = EnvConfig.getMongoConfig();
        this.provider = new MongoProvider(
            mongoConfig.connectionString,
            'factControllerTestDB'
        );


        this.controller = new FactController(this.provider, this._testFieldConfig, this._testIndexConfig, 500);
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
        this.logger.debug('=== Тестирование всех методов FactController (7 тестов) ===\n');

        try {
            // Подключение к базе данных
            await this.provider.connect();
            this.logger.debug('✓ Подключен к базе данных для тестирования FactController\n');

            // Запуск тестов
            await this.testRunBasic('1. Тест базового выполнения метода run...');
            await this.testRunWithExistingFacts('2. Тест выполнения с существующими фактами...');
            await this.testProcessEvent('3. Тест метода processEvent...');
            await this.testProcessEventWithCounters('6. Тест метода processEventWithCounters...');
            await this.testRunMultipleTimes('4. Тест многократного выполнения...');
            await this.testRunWithEmptyDatabase('5. Тест выполнения с пустой базой данных...');
            await this.testRunErrorHandling('6. Тест обработки ошибок...');

            // Отключение от базы данных
            await this.provider.disconnect();
            this.logger.debug('✓ Отключен от базы данных\n');

        } catch (error) {
            this.logger.error('✗ Ошибка при выполнении тестов FactController:', error.message);
            await this.provider.disconnect();
        }

        // Вывод результатов
        this.printResults();
    }

    /**
     * Тест базового выполнения метода run
     */
    async testRunBasic(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Выполняем метод run
            const result = await this.controller.run();

            // Проверяем, что результат содержит ожидаемые поля
            if (!result || typeof result !== 'object') {
                throw new Error('Метод run должен возвращать объект');
            }

            if (!result.fact) {
                throw new Error('Результат должен содержать поле fact');
            }

            if (!result.relevantFacts) {
                throw new Error('Результат должен содержать поле relevantFacts');
            }

            if (!result.factResult) {
                throw new Error('Результат должен содержать поле factResult');
            }

            if (!result.indexResult) {
                throw new Error('Результат должен содержать поле indexResult');
            }

            // Проверяем структуру сгенерированного факта
            const fact = result.fact;
            const requiredFields = ['_id', 't', 'c', 'd'];
            for (const field of requiredFields) {
                if (!(field in fact)) {
                    throw new Error(`Сгенерированный факт должен содержать поле ${field}`);
                }
            }

            // Проверяем, что relevantFacts является массивом
            if (!Array.isArray(result.relevantFacts)) {
                throw new Error('relevantFacts должен быть массивом');
            }

            // Проверяем, что факт был сохранен в базе данных
            const savedFacts = await this.provider.findFacts({ _id: fact._id });
            if (savedFacts.length === 0) {
                throw new Error('Факт должен быть сохранен в базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunBasic', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест выполнения метода run с существующими фактами
     */
    async testRunWithExistingFacts(title) {
        this.logger.debug(title);

        try {
            // Создаем несколько тестовых фактов с общими значениями полей
            const testFacts = [
                {
                    _id: 'test-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        a: 100,
                        dt: new Date(),
                        f1: 'shared-value',
                        f2: 'unique1'
                    }
                },
                {
                    _id: 'test-fact-002',
                    t: 1,
                    c: new Date(),
                    d: {
                        a: 200,
                        dt: new Date(),
                        f1: 'shared-value',
                        f2: 'unique2'
                    }
                }
            ];

            // Сохраняем тестовые факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                const indexValues = this.controller.factIndexer.index(fact);
                await this.provider.saveFactIndexList(indexValues);
            }

            // Создаем факт с теми же значениями полей, что и тестовые факты
            const testFact = {
                _id: 'test-fact-query',
                t: 1,
                c: new Date(),
                d: {
                    a: 300,
                    dt: new Date(),
                    f1: 'shared-value',
                    f2: 'unique3'
                }
            };

            // Тестируем getRelevantFacts напрямую
            const testFactIndexValues = this.controller.factIndexer.index(testFact);
            const testFactIndexHashValues = testFactIndexValues.map(index => index._id.h);
            const excludedFact = testFacts[1];
            const relevantFacts = await this.provider.getRelevantFacts(testFactIndexHashValues, excludedFact._id);

            // Проверяем, что relevantFacts содержит существующие факты
            if (relevantFacts.length === 0) {
                throw new Error('relevantFacts должен содержать существующие факты');
            }

            // Проверяем, что найденные факты содержат ожидаемые ID
            // relevantFacts содержит объекты с полем fact, поэтому извлекаем факты
            const foundIds = relevantFacts.map(f => f._id).filter(id => id); // Фильтруем пустые ID
            const expectedIds = testFacts.map(f => f._id);
            const hasExpectedFact = expectedIds.some(id => foundIds.includes(id));

            if (!hasExpectedFact) {
                this.logger.debug(`Найденные ID: ${foundIds.join(', ')}`);
                this.logger.debug(`Ожидаемые ID: ${expectedIds.join(', ')}`);
                this.logger.debug(`Полные найденные факты:`, JSON.stringify(relevantFacts, null, 2));
                throw new Error('relevantFacts должен содержать хотя бы один из тестовых фактов');
            }

            // Теперь выполняем обычный метод run
            const result = await this.controller.run();

            // Проверяем, что новый факт был сохранен
            const newFact = result.fact;
            const savedFacts = await this.provider.findFacts({ i: newFact.i });
            if (savedFacts.length === 0) {
                throw new Error('Новый факт должен быть сохранен в базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunWithExistingFacts', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест многократного выполнения метода run
     */
    async testRunMultipleTimes(title) {
        this.logger.debug(title);

        try {
            const runCount = 3;
            const results = [];

            // Выполняем метод run несколько раз
            for (let i = 0; i < runCount; i++) {
                const result = await this.controller.run();
                results.push(result);
            }

            // Проверяем, что все результаты корректны
            for (let i = 0; i < results.length; i++) {
                const result = results[i];
                if (!result.fact || !result.relevantFacts || !result.factResult || !result.indexResult) {
                    throw new Error(`Результат ${i + 1} должен содержать все обязательные поля`);
                }
            }

            // Проверяем, что все факты уникальны
            const factIds = results.map(r => r.fact._id);
            const uniqueIds = new Set(factIds);
            if (uniqueIds.size !== factIds.length) {
                throw new Error('Все сгенерированные факты должны иметь уникальные ID');
            }

            // Проверяем, что все факты сохранены в базе данных
            for (const result of results) {
                const savedFacts = await this.provider.findFacts({ _id: result.fact._id });
                if (savedFacts.length === 0) {
                    throw new Error(`Факт ${result.fact.i} должен быть сохранен в базе данных`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunMultipleTimes', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест выполнения метода run с пустой базой данных
     */
    async testRunWithEmptyDatabase(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Выполняем метод run
            const result = await this.controller.run();

            // Проверяем, что relevantFacts пустой массив
            if (!Array.isArray(result.relevantFacts)) {
                throw new Error('relevantFacts должен быть массивом');
            }

            if (result.relevantFacts.length !== 0) {
                throw new Error('relevantFacts должен быть пустым массивом при пустой базе данных');
            }

            // Проверяем, что факт был сохранен
            const savedFacts = await this.provider.findFacts({ i: result.fact.i });
            if (savedFacts.length === 0) {
                throw new Error('Факт должен быть сохранен даже при пустой базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunWithEmptyDatabase', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 5: Метод processEvent
     */
    async testProcessEvent(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Создаем тестовое событие
            const testEvent = {
                t: 1,
                d: {
                    dt: new Date('2024-01-01T00:00:00.000Z'),
                    a: 100,
                    f1: 'test-fact-001',
                    f2: 'value1'
                }
            };

            // Вызываем метод processEvent
            const result = await this.controller.processEvent(testEvent);

            // Проверяем структуру результата
            if (!result || typeof result !== 'object') {
                throw new Error('processEvent должен возвращать объект');
            }

            // Проверяем наличие обязательных полей в результате
            const requiredFields = ['fact', 'relevantFacts', 'factResult', 'indexResult'];
            for (const field of requiredFields) {
                if (!(field in result)) {
                    throw new Error(`Отсутствует обязательное поле: ${field}`);
                }
            }

            // Проверяем структуру факта
            if (!result.fact || typeof result.fact !== 'object') {
                throw new Error('Поле fact должно быть объектом');
            }

            // Проверяем обязательные поля факта
            const factRequiredFields = ['_id', 't', 'c', 'd'];
            for (const field of factRequiredFields) {
                if (!(field in result.fact)) {
                    throw new Error(`Отсутствует обязательное поле в факте: ${field}`);
                }
            }

            // Проверяем, что relevantFacts является массивом
            if (!Array.isArray(result.relevantFacts)) {
                throw new Error('Поле relevantFacts должно быть массивом');
            }

            // Проверяем, что factResult содержит информацию о сохранении
            if (!result.factResult || typeof result.factResult !== 'object') {
                throw new Error('Поле factResult должно быть объектом');
            }

            // Проверяем, что indexResult содержит информацию о сохранении индексов
            if (!result.indexResult || typeof result.indexResult !== 'object') {
                throw new Error('Поле indexResult должно быть объектом');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testProcessEvent', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 6: Метод processEventWithCounters
     */
    async testProcessEventWithCounters(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Создаем тестовое событие
            const testEvent = {
                t: 1,
                d: {
                    dt: new Date('2024-01-01T00:00:00.000Z'),
                    a: 100,
                    f1: 'test-fact-001',
                    f2: 'value1'
                }
            };

            // Вызываем метод processEventWithCounters
            const result = await this.controller.processEventWithCounters(testEvent);

            // Проверяем структуру результата
            if (!result || typeof result !== 'object') {
                throw new Error('processEventWithCounters должен возвращать объект');
            }

            // Проверяем наличие обязательных полей в результате
            const requiredFields = ['fact', 'factCounters', 'factResult', 'indexResult'];
            for (const field of requiredFields) {
                if (!(field in result)) {
                    throw new Error(`Отсутствует обязательное поле: ${field}`);
                }
            }

            // Проверяем структуру факта
            if (!result.fact || typeof result.fact !== 'object') {
                throw new Error('Поле fact должно быть объектом');
            }

            // Проверяем обязательные поля факта
            const factRequiredFields = ['_id', 't', 'c', 'd'];
            for (const field of factRequiredFields) {
                if (!(field in result.fact)) {
                    throw new Error(`Отсутствует обязательное поле в факте: ${field}`);
                }
            }

            // Проверяем, что factCounters является массивом
            if (!Array.isArray(result.factCounters)) {
                throw new Error('Поле factCounters должно быть массивом');
            }

            // Проверяем, что factResult содержит информацию о сохранении
            if (!result.factResult || typeof result.factResult !== 'object') {
                throw new Error('Поле factResult должно быть объектом');
            }

            // Проверяем, что indexResult содержит информацию о сохранении индексов
            if (!result.indexResult || typeof result.indexResult !== 'object') {
                throw new Error('Поле indexResult должно быть объектом');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testProcessEventWithCounters', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 6: Обработка ошибок в методе run
     */
    async testRunErrorHandling(title) {
        this.logger.debug(title);

        try {
            // Создаем контроллер с невалидным провайдером
            const invalidProvider = {
                getRelevantFacts: () => { throw new Error('Тестовая ошибка getRelevantFacts'); },
                getRelevantFactCounters: () => { throw new Error('Тестовая ошибка getRelevantFactCounters'); },
                saveFact: () => { throw new Error('Тестовая ошибка saveFact'); },
                saveFactIndexList: () => { throw new Error('Тестовая ошибка saveFactIndexList'); }
            };

            const testController = new FactController(invalidProvider, this._testFieldConfig, this._testIndexConfig, 500);

            // Пытаемся выполнить метод run
            try {
                await testController.run();
                throw new Error('Метод run должен выбрасывать ошибку при проблемах с провайдером');
            } catch (error) {
                // Ожидаем, что ошибка будет выброшена
                if (!error.message.includes('Тестовая ошибка')) {
                    throw new Error(`Ожидалась тестовая ошибка, получена: ${error.message}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunErrorHandling', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.debug('\n=== Результаты тестирования FactController ===');
        this.logger.debug(`Пройдено: ${this.testResults.passed}`);
        this.logger.debug(`Провалено: ${this.testResults.failed}`);

        if (this.testResults.errors.length > 0) {
            this.logger.debug('\nОшибки:');
            for (const error of this.testResults.errors) {
                this.logger.debug(`  - ${error.test}: ${error.error}`);
            }
        }

        const totalTests = this.testResults.passed + this.testResults.failed;
        const successRate = totalTests > 0 ? (this.testResults.passed / totalTests * 100).toFixed(1) : 0;
        this.logger.debug(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new FactControllerTest();
    test.runAllTests().catch(console.error);
}

module.exports = FactControllerTest;