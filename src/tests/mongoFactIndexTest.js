const { MongoProvider, FactIndexer, MessageGenerator, FactMapper } = require('../index');
const Logger = require('../utils/logger');
const config = require('../common/config');

/**
 * Тесты для работы с индексными значениями фактов в MongoDB
 */
class MongoFactIndexTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        const DATABASE_NAME = 'factTestDB';
        this.logger.info(`Starting MongoFactIndex tests... ${config.database.connectionString} ${DATABASE_NAME}`);
        this.provider = new MongoProvider(
            config.database.connectionString,
            DATABASE_NAME,
            config.database.options,
            null,
            config.facts.includeFactDataToIndex,
            config.facts.lookupFacts,
            config.facts.indexBulkUpdate
        );

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
                fieldName: "f5",
                dateName: "dt",
                indexTypeName: "test_type_5",
                indexType: 5,
                indexValue: 2
            },
            {
                fieldName: "f4",
                dateName: "dt",
                indexTypeName: "test_type_4",
                indexType: 4,
                indexValue: 1
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
        this.indexer = new FactIndexer(this.testIndexConfig, config.facts.includeFactDataToIndex);

        // Тестовая конфигурация полей
        const testFieldConfig = [
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
                "message_types": [1, 2, 3],
                "key_order": 1
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

        this.generator = new MessageGenerator(testFieldConfig);
        this.mapper = new FactMapper(testFieldConfig);

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
        this.logger.info('Starting MongoFactIndex tests...');
        this.logger.debug('=== Тестирование работы с индексными значениями фактов в MongoDB ===\n');

        try {
            await this.testConnection('1. Тест подключения к MongoDB...');
            await this.testIndexValuesInsertion('2. Тест вставки индексных значений...');
            await this.testIndexValuesStats('4. Тест статистики индексных значений...');
            await this.testIndexValuesSchema('5. Тест схемы индексных значений...');
            await this.testClearIndexValues('6. Тест очистки индексных значений...');
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        } finally {
            await this.provider.disconnect();
            this.printResults();
        }
    }

    /**
     * Тест подключения к MongoDB
     */
    async testConnection(title) {
        this.logger.debug(title);
        
        try {
            const connected = await this.provider.connect();
            
            if (!connected) {
                throw new Error('Не удалось подключиться к MongoDB');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testConnection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест вставки индексных значений
     */
    async testIndexValuesInsertion(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекцию перед тестом
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые факты
            const facts = [];
            for (let i = 0; i < 5; i++) {
                facts.push(this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage()));
            }
            
            // Создаем индексные значения
            const indexValues = this.indexer.indexFacts(facts);
            
            if (indexValues.length === 0) {
                throw new Error('Не создано ни одного индексного значения');
            }

            // Вставляем в MongoDB
            const result = await this.provider.saveFactIndexList(indexValues);
            
            if (!result.success) {
                throw new Error(`Операция вставки не выполнена: ${result.error || 'неизвестная ошибка'}`);
            }
            
            // Для первой вставки все документы должны быть вставлены как новые
            if (result.inserted !== indexValues.length) {
                throw new Error(`Ожидалось вставить ${indexValues.length} новых документов, вставлено ${result.inserted}`);
            }
            
            // Проверим повторную вставку тех же данных (должны быть проигнорированы или обновлены)
            const result2 = await this.provider.saveFactIndexList(indexValues);
            if (!result2.success) {
                throw new Error(`Операция повторной вставки не выполнена: ${result2.error || 'неизвестная ошибка'}`);
            }
            
            // При повторной вставке новых документов не должно быть (должны обновляться или игнорироваться)
            if (result2.inserted > 0) {
                throw new Error(`При повторной вставке было создано ${result2.inserted} новых документов (ожидалось 0)`);
            }
            
            // Проверим, что общее количество обработанных документов правильное
            const totalProcessed = result2.updated + result2.duplicatesIgnored;
            if (totalProcessed !== indexValues.length) {
                throw new Error(`Ожидалось обработать ${indexValues.length} дубликатов, обработано ${totalProcessed}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testIndexValuesInsertion: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения статистики индексных значений
     */
    async testIndexValuesStats(title) {
        this.logger.debug(title);
        
        try {
            const stats = await this.provider.getFactIndexStats();
            
            if (!stats || typeof stats.documentCount !== 'number') {
                throw new Error('Некорректная статистика');
            }

            if (stats.documentCount === 0) {
                throw new Error('Коллекция индексных значений пуста');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testIndexValuesStats: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения схемы индексных значений
     */
    async testIndexValuesSchema(title) {
        this.logger.debug(title);
        
        try {
            const schema = await this.provider._getFactIndexCollectionSchema();
            
            if (!schema || schema.isEmpty) {
                throw new Error('Схема пуста или не получена');
            }

            const requiredFields = ['_id', 'dt', 'c', 'v', 'it', 't']; // d необязательное поле
            const schemaFields = schema.fields.map(f => f.name);
            
            for (const field of requiredFields) {
                if (!schemaFields.includes(field)) {
                    throw new Error(`Отсутствует обязательное поле: ${field}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testIndexValuesSchema: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест очистки индексных значений
     */
    async testClearIndexValues(title) {
        this.logger.debug(title);
        
        try {
            const result = await this.provider.clearFactIndexCollection();
            
            if (typeof result.deletedCount !== 'number') {
                throw new Error('Некорректный результат очистки');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testClearIndexValues: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования индексных значений фактов в MongoDB ===');
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
    const test = new MongoFactIndexTest();
    test.runAllTests().catch(console.error);
}

module.exports = MongoFactIndexTest;
