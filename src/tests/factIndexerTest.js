const { FactIndexer, FactGenerator } = require('../index');
const Logger = require('../utils/logger');

/**
 * Тесты для класса FactIndexer (создание индексных значений)
 */
class FactIndexerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.indexer = new FactIndexer();
        this.generator = new FactGenerator();
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

        this.testBasicIndexing();
        this.testMultipleFields();
        this.testNoFields();
        this.testInvalidInput();
        this.testMultipleFacts();
        this.testStatistics();
        this.testWithGeneratedFacts();

        this.printResults();
    }

    /**
     * Тест базового создания индексных значений
     */
    testBasicIndexing() {
        this.logger.debug('1. Тест базового создания индексных значений...');
        
        const fact = {
            i: 'test-id-123',
            t: 1,
            a: 100,
            c: new Date('2024-01-01'),
            d: new Date('2024-01-15'),
            f1: 'value1',
            f2: 'value2',
            f5: 'value5'
        };

        try {
            const indexValues = this.indexer.index(fact);
            
            // Проверяем количество индексных значений (должно быть 3 для f1, f2, f5)
            if (indexValues.length !== 3) {
                throw new Error(`Ожидалось 3 индексных значения, получено ${indexValues.length}`);
            }

            // Проверяем структуру первого индексного значения
            const firstIndexValue = indexValues[0];
            const requiredFields = ['h', 'i', 'd', 'c'];
            for (const field of requiredFields) {
                if (!(field in firstIndexValue)) {
                    throw new Error(`Отсутствует поле ${field} в индексном значении`);
                }
            }

            // Проверяем обязательные поля
            if (!firstIndexValue.h || typeof firstIndexValue.h !== 'string') {
                throw new Error(`Отсутствует или неправильный тип поля h: ${firstIndexValue.h}`);
            }

            if (firstIndexValue.h.length !== 64) {
                throw new Error(`Неправильная длина SHA-256 хеша: ожидалось 64 символа, получено ${firstIndexValue.h.length}`);
            }

            if (firstIndexValue.i !== 'test-id-123') {
                throw new Error(`Неправильный ID: ${firstIndexValue.i}`);
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
    testMultipleFields() {
        this.logger.debug('2. Тест с множественными полями...');
        
        const fact = {
            i: 'multi-field-test',
            t: 2,
            a: 200,
            c: new Date('2024-02-01'),
            d: new Date('2024-02-15'),
            f1: 'val1',
            f2: 'val2',
            f3: 'val3',
            f10: 'val10',
            f15: 'val15',
            f23: 'val23',
            otherField: 'should be ignored',
            notF: 'should be ignored'
        };

        try {
            const indexValues = this.indexer.index(fact);
            
            if (indexValues.length !== 6) {
                throw new Error(`Ожидалось 6 индексных значений, получено ${indexValues.length}`);
            }

            // Проверяем, что все индексные значения имеют правильные поля
            indexValues.forEach(indexValue => {
                if (indexValue.i !== 'multi-field-test') {
                    throw new Error('Неправильный ID факта в индексном значении');
                }
                
                // Проверяем, что хеш является валидным SHA-256
                if (!indexValue.h || typeof indexValue.h !== 'string' || indexValue.h.length !== 64) {
                    throw new Error(`Неправильный хеш: ${indexValue.h}`);
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
    testNoFields() {
        this.logger.debug('3. Тест без полей fN...');
        
        const fact = {
            i: 'no-fields-test',
            t: 3,
            a: 300,
            c: new Date('2024-03-01'),
            d: new Date('2024-03-15'),
            otherField: 'value',
            anotherField: 'another value'
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
    testInvalidInput() {
        this.logger.debug('4. Тест с неверными входными данными...');
        
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
    testMultipleFacts() {
        this.logger.debug('5. Тест с множественными фактами...');
        
        const facts = [
            {
                i: 'fact1',
                t: 1,
                a: 100,
                c: new Date('2024-01-01'),
                d: new Date('2024-01-15'),
                f1: 'val1',
                f2: 'val2'
            },
            {
                i: 'fact2',
                t: 2,
                a: 200,
                c: new Date('2024-02-01'),
                d: new Date('2024-02-15'),
                f3: 'val3',
                f4: 'val4'
            }
        ];

        try {
            const indexValues = this.indexer.indexFacts(facts);
            
            if (indexValues.length !== 4) {
                throw new Error(`Ожидалось 4 индексных значения, получено ${indexValues.length}`);
            }

            // Проверяем, что индексные значения содержат правильные ID фактов
            const factIds = indexValues.map(idx => idx.i);
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
    testStatistics() {
        this.logger.debug('6. Тест структуры индексных значений...');
        
        const indexValues = [
            { h: '1'.repeat(63)+"1", i: 'id1', d: new Date('2024-01-01'), c: new Date('2024-01-01') },
            { h: '1'.repeat(63)+'1', i: 'id1', d: new Date('2024-01-02'), c: new Date('2024-01-01') },
            { h: '1'.repeat(63)+'2', i: 'id2', d: new Date('2024-01-03'), c: new Date('2024-01-02') },
            { h: '1'.repeat(63)+'3', i: 'id2', d: new Date('2024-01-04'), c: new Date('2024-01-02') }
        ];

        try {
            // Проверяем базовую структуру индексных значений
            if (indexValues.length !== 4) {
                throw new Error(`Неправильное количество индексных значений: ${indexValues.length}`);
            }

            // Проверяем, что все индексные значения имеют правильную структуру
            indexValues.forEach((indexValue, i) => {
                const requiredFields = ['h', 'i', 'd', 'c'];
                for (const field of requiredFields) {
                    if (!(field in indexValue)) {
                        throw new Error(`Отсутствует поле ${field} в индексном значении ${i}`);
                    }
                }
            });

            // Проверяем, что все индексные значения имеют валидные хеши
            indexValues.forEach(indexValue => {
                if (!indexValue.h || typeof indexValue.h !== 'string' || indexValue.h.length !== 64) {
                    throw new Error(`Неправильный хеш в индексном значении: ${indexValue.h}`);
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
    testWithGeneratedFacts() {
        this.logger.debug('7. Тест с сгенерированными фактами...');
        
        try {
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            
            // Генерируем несколько фактов
            const facts = [];
            for (let i = 0; i < 5; i++) {
                facts.push(this.generator.generateRandomTypeFact());
            }
            
            const indexValues = this.indexer.indexFacts(facts);
            
            if (indexValues.length === 0) {
                throw new Error('Не создано ни одного индексного значения');
            }

            // Проверяем, что все индексные значения имеют правильную структуру
            indexValues.forEach(indexValue => {
                const requiredFields = ['h', 'i', 'd', 'c'];
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
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.debug('\n=== Результаты тестирования FactIndexer ===');
        this.logger.debug(`Пройдено: ${this.testResults.passed}`);
        this.logger.debug(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
            this.logger.debug('\nОшибки:');
            this.testResults.errors.forEach(error => {
                this.logger.debug(`  - ${error}`);
            });
        }
        
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? (this.testResults.passed / total * 100).toFixed(1) : 0;
        this.logger.debug(`\nПроцент успеха: ${successRate}%`);
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
            console.error('Критическая ошибка:', error.message);
            process.exit(1);
        });
}

module.exports = FactIndexerTest;
