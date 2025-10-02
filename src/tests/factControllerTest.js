const { MongoProvider, FactController } = require('../index');
const Logger = require('../utils/logger');
const EnvConfig = require('../utils/envConfig');

/**
 * Тесты для всех методов FactController
 */
class FactControllerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        const mongoConfig = EnvConfig.getMongoConfig();
        this.provider = new MongoProvider(
            mongoConfig.connectionString,
            'factControllerTestDB'
        );
        this.controller = new FactController(this.provider);
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
            await this.testRunBasic();
            await this.testRunWithExistingFacts();
            await this.testRunMultipleTimes();
            await this.testRunWithEmptyDatabase();
            await this.testRunErrorHandling();

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
    async testRunBasic() {
        this.logger.debug('1. Тест базового выполнения метода run...');
        
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
            const requiredFields = ['i', 't', 'a', 'c', 'd'];
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
    async testRunWithExistingFacts() {
        this.logger.debug('2. Тест выполнения метода run с существующими фактами...');
        
        try {
            // Создаем несколько тестовых фактов с общими значениями полей
            const testFacts = [
                {
                    i: 'test-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(),
                    d: new Date(),
                    f1: 'shared-value',
                    f2: 'unique1'
                },
                {
                    i: 'test-fact-002',
                    t: 1,
                    a: 200,
                    c: new Date(),
                    d: new Date(),
                    f1: 'shared-value',
                    f2: 'unique2'
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
                a: 300,
                c: new Date(),
                d: new Date(),
                f1: 'shared-value', // То же значение, что и в тестовых фактах
                f2: 'unique3'
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
    async testRunMultipleTimes() {
        this.logger.debug('3. Тест многократного выполнения метода run...');
        
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
    async testRunWithEmptyDatabase() {
        this.logger.debug('4. Тест выполнения метода run с пустой базой данных...');
        
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
    async testRunErrorHandling() {
        this.logger.debug('5. Тест обработки ошибок в методе run...');
        
        try {
            // Создаем контроллер с невалидным провайдером
            const invalidProvider = {
                getRelevantFacts: () => { throw new Error('Тестовая ошибка getRelevantFacts'); },
                saveFact: () => { throw new Error('Тестовая ошибка saveFact'); },
                saveFactIndexList: () => { throw new Error('Тестовая ошибка saveFactIndexList'); }
            };

            const testController = new FactController(invalidProvider);

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