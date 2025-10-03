const { MongoProvider, FactController } = require('../index');
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
            "fact_types": [1, 2, 3],
            "generator": {
                "type": "date",
                "min": "2025-01-01",
                "max": "2025-10-01"
            }
        },
        {
            "src": "a",
            "dst": "a",
            "fact_types": [1, 2, 3],
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
            "fact_types": [1, 2, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            }
        },
        {
            "src": "f2",
            "dst": "f2",
            "fact_types": [1, 3],
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
            "fact_types": [2, 3],
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
        this.logger.debug('=== Тестирование всех методов FactController (5 тестов) ===\n');

        try {
            // Подключение к базе данных
            await this.provider.connect();
            this.logger.debug('✓ Подключен к базе данных для тестирования FactController\n');

            // Запуск тестов
            await this.testRunBasic('1. Тест базового выполнения метода run...');
            await this.testRunWithExistingFacts('2. Тест выполнения с существующими фактами...');
            await this.testRunMultipleTimes('3. Тест многократного выполнения...');
            await this.testRunWithEmptyDatabase('4. Тест выполнения с пустой базой данных...');
            await this.testRunErrorHandling('5. Тест обработки ошибок...');

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

            if (!result.result) {
                throw new Error('Результат должен содержать поле result');
            }

            // Проверяем структуру сгенерированного факта
            const fact = result.fact;
            const requiredFields = ['i', 't', 'c', 'd'];
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
            const savedFacts = await this.provider.findFacts({ i: fact.i });
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
                    i: 'test-fact-001',
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
                    i: 'test-fact-002',
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
                i: 'test-fact-query',
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
            const testFactIndexHashValues = testFactIndexValues.map(index => index.h);
            const excludedFact = testFacts[1];
            const relevantFacts = await this.provider.getRelevantFacts(testFactIndexHashValues, excludedFact.i);

            // Проверяем, что relevantFacts содержит существующие факты
            if (relevantFacts.length === 0) {
                throw new Error('relevantFacts должен содержать существующие факты');
            }

            // Проверяем, что найденные факты содержат ожидаемые ID
            // relevantFacts содержит объекты с полем fact, поэтому извлекаем факты
            const foundIds = relevantFacts.map(f => f.fact ? f.fact.i : f.i).filter(id => id); // Фильтруем пустые ID
            const expectedIds = testFacts.map(f => f.i);
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
                if (!result.fact || !result.relevantFacts || !result.result) {
                    throw new Error(`Результат ${i + 1} должен содержать все обязательные поля`);
                }
            }

            // Проверяем, что все факты уникальны
            const factIds = results.map(r => r.fact.i);
            const uniqueIds = new Set(factIds);
            if (uniqueIds.size !== factIds.length) {
                throw new Error('Все сгенерированные факты должны иметь уникальные ID');
            }

            // Проверяем, что все факты сохранены в базе данных
            for (const result of results) {
                const savedFacts = await this.provider.findFacts({ i: result.fact.i });
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
     * Тест обработки ошибок в методе run
     */
    async testRunErrorHandling(title) {
        this.logger.debug(title);

        try {
            // Создаем контроллер с невалидным провайдером
            const invalidProvider = {
                getRelevantFacts: () => { throw new Error('Тестовая ошибка getRelevantFacts'); },
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