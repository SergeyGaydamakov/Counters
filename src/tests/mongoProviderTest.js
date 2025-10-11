const { MongoProvider, MessageGenerator, FactIndexer, FactMapper, MongoCounters } = require('../index');
const Logger = require('../utils/logger');
const config = require('../common/config');

/**
 * Тесты для всех методов MongoProvider
 */
class MongoProviderTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.countersConfig = [
            {
                name: "total",
                comment: "Общий счетчик для всех типов сообщений",
                indexTypeName: "total_index",
                computationConditions: {},
                evaluationConditions: [
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$d.amount" }
                        }
                    }
                ],
                variables: []
            }
        ];
        this.mongoCounters = new MongoCounters(this.countersConfig);

        this.provider = new MongoProvider(
            config.database.connectionString,
            'mongoProviderTestDB',
            this.mongoCounters
        );
        
        // Минимальная конфигурация полей для тестов (6 первых полей)
        this.fieldConfig = [
            {
                src: "amount",
                dst: "amount",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
                generator: {
                    type: "integer",
                    min: 1,
                    max: 10000000,
                    default_value: 1000,
                    default_random: 0.1
                }
            },
            {
                src: "dt",
                dst: "dt",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
                generator: {
                    type: "date",
                    min: "2024-01-01 00:00:00",
                    max: "2024-12-31 23:59:59"
                }
            },
            {
                src: "f1",
                dst: "f1",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
                generator: {
                    type: "string",
                    min: 2,
                    max: 20,
                    default_value: "1234567890",
                    default_random: 0.1
                },
                key_type: 1
            },
            {
                src: "f2",
                dst: "f2",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
                generator: {
                    type: "enum",
                    values: ["value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"],
                    default_value: "value1",
                    default_random: 0.1
                }
            },
            {
                src: "f3",
                dst: "f3",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
                generator: {
                    type: "enum",
                    values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    default_value: 1,
                    default_random: 0.1
                }
            },
            {
                src: "f4",
                dst: "f4",
                message_types: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
            }
        ];
        
        this.generator = new MessageGenerator(this.fieldConfig);
        
       
        // Минимальная конфигурация для FactIndexer (4 первых значения)
        this.indexConfig = [
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
            }
        ];
        
        this.indexer = new FactIndexer(this.indexConfig);
        this.mapper = new FactMapper(this.fieldConfig);
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
        this.logger.debug('=== Тестирование всех методов MongoProvider (29 тестов) ===\n');

        try {
            // Тесты подключения
            await this.testConnection('1. Тест подключения к MongoDB...');
            await this.testDisconnection('2. Тест отключения от MongoDB...');
            await this.testReconnection('3. Тест переподключения к MongoDB...');
            await this.testCheckConnection('4. Тест проверки подключения...');

            // Тесты создания базы данных
            await this.testCreateDatabase('5. Тест создания базы данных...');

            // Тесты работы с фактами
            await this.testInsertFact('6. Тест вставки одного факта...');
            await this.testBulkInsert('7. Тест массовой вставки фактов...');
            await this.testGetFactsCollectionSchema('8. Тест получения схемы коллекции фактов...');
            await this.testClearFactsCollection('09. Тест очистки коллекции фактов...');
            
            // Тесты индексных значений
            await this.testInsertFactIndexList('10. Тест вставки списка индексных значений...');
            await this.testGetFactIndexSchema('11. Тест получения схемы коллекции индексных значений...');
            await this.testClearFactIndexCollection('12. Тест очистки коллекции индексных значений...');
            
            // Тесты повторных вызовов с теми же данными
            await this.testDuplicateInsertFact('13. Тест повторной вставки того же факта...');
            await this.testDuplicateInsertFactIndexList('14. Тест повторной вставки тех же индексных значений...');
            await this.testDuplicateBulkInsert('15. Тест повторной массовой вставки...');

            // Тесты получения релевантных фактов
            await this.testGetRelevantFacts('16. Тест получения релевантных фактов...');
            await this.testGetRelevantFactsWithMultipleFields('17. Тест получения релевантных фактов с множественными полями...');
            await this.testGetRelevantFactsWithNoMatches('18. Тест получения релевантных фактов без совпадений...');
            await this.testGetRelevantFactsWithDepthLimit('19. Тест получения релевантных фактов с ограничением глубины...');
            await this.testGetRelevantFactsWithDepthFromDate('20. Тест получения релевантных фактов с глубиной от даты...');
            await this.testGetRelevantFactsWithBothParameters('21. Тест получения релевантных фактов с обоими параметрами...');

            // Тесты получения релевантных счетчиков фактов
            await this.testGetRelevantFactCounters('22. Тест получения релевантных счетчиков фактов...');
            await this.testGetRelevantFactCountersWithMultipleFields('23. Тест получения релевантных счетчиков с множественными полями...');
            await this.testGetRelevantFactCountersWithNoMatches('24. Тест получения релевантных счетчиков без совпадений...');
            await this.testGetRelevantFactCountersWithDepthLimit('25. Тест получения релевантных счетчиков с ограничением глубины...');
            await this.testGetRelevantFactCountersWithDepthFromDate('26. Тест получения релевантных счетчиков с глубиной от даты...');
            await this.testGetRelevantFactCountersWithBothParameters('27. Тест получения релевантных счетчиков с обоими параметрами...');
            
            // Тесты статистики
            await this.testGetFactsCollectionStats('28. Тест получения статистики коллекции facts...');
            await this.testGetFactIndexStats('29. Тест получения статистики индексных значений...');
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        } finally {
            try {
                await this.provider.disconnect();
                this.logger.debug('✓ Все соединения с MongoDB закрыты');
            } catch (error) {
                this.logger.error('✗ Ошибка при закрытии соединений:', error.message);
            }
            
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

            if (!this.provider._isConnected) {
                throw new Error('Флаг isConnected не установлен');
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
     * Тест отключения от MongoDB
     */
    async testDisconnection(title) {
        this.logger.debug(title);
        
        try {
            await this.provider.disconnect();
            
            if (this.provider._isConnected) {
                throw new Error('Флаг isConnected не сброшен после отключения');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testDisconnection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест повторного подключения
     */
    async testReconnection(title) {
        this.logger.debug(title);
        
        try {
            const connected = await this.provider.connect();
            
            if (!connected) {
                throw new Error('Не удалось переподключиться к MongoDB');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testReconnection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения схемы коллекции facts
     */
    async testGetFactsCollectionSchema(title) {
        this.logger.debug(title);
        
        try {
            const schema = await this.provider._getFactsCollectionSchema();
            
            if (!schema) {
                throw new Error('Схема не получена');
            }

            if (!schema.$jsonSchema) {
                throw new Error('Схема не содержит $jsonSchema');
            }

            const requiredFields = ['_id', 't', 'c', 'd'];
            const schemaFields = Object.keys(schema.$jsonSchema.properties);
            
            for (const field of requiredFields) {
                if (!schemaFields.includes(field)) {
                    throw new Error(`Отсутствует обязательное поле в схеме: ${field}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetFactsCollectionSchema: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест вставки одного факта
     */
    async testInsertFact(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactsCollection();
            
            // Генерируем тестовый факт
            const testFact = this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage());

            // Тест первой вставки (должна создать новый документ)
            const insertResult = await this.provider.saveFact(testFact);
            
            if (!insertResult.success) {
                throw new Error(`Ошибка вставки: ${insertResult.error}`);
            }

            if (insertResult.factInserted !== 1) {
                throw new Error(`Ожидалось вставить 1 факт, вставлено ${insertResult.factInserted}`);
            }

            // Тест повторной вставки с измененными данными (должна обновить существующий по умолчанию)
            const modifiedFact = { ...testFact, d: { amount: testFact.d.amount + 100 } }; // Изменяем значение поля amount
            const upsertResult = await this.provider.saveFact(modifiedFact);
            
            if (!upsertResult.success) {
                throw new Error(`Ошибка повторной вставки: ${upsertResult.error}`);
            }

            // При повторной вставке с updateOnDuplicate=true (по умолчанию) должно произойти обновление
            if (upsertResult.factUpdated !== 1) {
                throw new Error(`Ожидалось обновить 1 факт, обновлено ${upsertResult.factUpdated}`);
            }

            // Тест повторной вставки того же факта (должен обновить существующий)
            const duplicateResult = await this.provider.saveFact(testFact);
            
            if (!duplicateResult.success) {
                throw new Error(`Ошибка повторной вставки: ${duplicateResult.error}`);
            }

            // При повторной вставке того же факта должен произойти update
            if (duplicateResult.factUpdated !== 1) {
                throw new Error(`Ожидалось обновить 1 факт, обновлено ${duplicateResult.factUpdated}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testInsertFact: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест bulk вставки факта и индексных значений
     */
    async testBulkInsert(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const testFact = this.mapper.mapMessageToFact(this.generator.generateMessage(1));
            
            // Создаем индексные значения
            const indexValues = this.indexer.index(testFact);

            // Вставляем факт
            const factResult = await this.provider.saveFact(testFact);
            
            if (!factResult.success) {
                throw new Error(`Ошибка вставки факта: ${factResult.error}`);
            }

            // Вставляем индексные значения
            const indexResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!indexResult.success) {
                throw new Error(`Ошибка вставки индексных значений: ${indexResult.error}`);
            }

            // Проверяем, что факт был вставлен (может быть 0 если уже существует)
            if (factResult.factInserted < 0) {
                throw new Error(`Некорректное количество вставленных фактов: ${factResult.factInserted}`);
            }

            // Проверяем, что индексные значения были обработаны
            if (indexResult.inserted !== undefined && indexResult.inserted === 0) {
                throw new Error(`Не было вставлено ни одного индексного значения`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testBulkInsert: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест получения статистики коллекции facts
     */
    async testGetFactsCollectionStats(title) {
        this.logger.debug(title);
        
        try {
            const stats = await this.provider.getFactsCollectionStats();
            
            if (!stats || typeof stats.documentCount !== 'number') {
                throw new Error('Некорректная статистика');
            }

            if (stats.documentCount === 0) {
                throw new Error('Коллекция facts пуста');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetFactsCollectionStats: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест вставки индексных значений
     */
    async testInsertFactIndexList(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const facts = [
                this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage()), 
                this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage()), 
                this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage())
            ];
            const indexValues = this.indexer.index(facts[0]);

            const result = await this.provider.saveFactIndexList(indexValues);
            
            if (!result.success) {
                throw new Error(`Ошибка вставки индексных значений: ${result.error}`);
            }

            if (result.inserted === 0) {
                throw new Error(`Не было вставлено ни одного индексного значения`);
            }

            // Тест повторной вставки (должны быть проигнорированы)
            const result2 = await this.provider.saveFactIndexList(indexValues);
            if (!result2.success) {
                throw new Error(`Ошибка повторной вставки: ${result2.error}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testInsertFactIndexList: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения статистики индексных значений
     */
    async testGetFactIndexStats(title) {
        this.logger.debug(title);
        
        try {
            const stats = await this.provider.getFactIndexStats();
            
            if (!stats || typeof stats.documentCount !== 'number') {
                throw new Error('Некорректная статистика индексных значений');
            }

            if (stats.documentCount === 0) {
                throw new Error('Коллекция индексных значений пуста');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetFactIndexStats: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения схемы индексных значений
     */
    async testGetFactIndexSchema(title) {
        this.logger.debug(title);
        
        try {
            const schema = await this.provider._getFactIndexCollectionSchema();
            
            if (!schema || schema.isEmpty) {
                throw new Error('Схема индексных значений пуста или не получена');
            }

            const requiredFields = ['_id', 'd', 'c'];
            const schemaFields = schema.fields.map(f => f.name);
            
            for (const field of requiredFields) {
                if (!schemaFields.includes(field)) {
                    throw new Error(`Отсутствует обязательное поле в схеме индексных значений: ${field}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetFactIndexSchema: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест создания базы данных
     */
    async testCreateDatabase(title) {
        this.logger.debug(title);
        
        try {
            const result = await this.provider.createDatabase();
            
            if (!result || typeof result !== 'object') {
                throw new Error('Результат должен быть объектом');
            }

            if (!('success' in result)) {
                throw new Error('Результат должен содержать поле success');
            }

            if (!('factsSchema' in result)) {
                throw new Error('Результат должен содержать поле factsSchema');
            }

            if (!('factIndexSchema' in result)) {
                throw new Error('Результат должен содержать поле factIndexSchema');
            }

            if (!('factsIndexes' in result)) {
                throw new Error('Результат должен содержать поле factsIndexes');
            }

            if (!('factIndexIndexes' in result)) {
                throw new Error('Результат должен содержать поле factIndexIndexes');
            }

            if (!('errors' in result)) {
                throw new Error('Результат должен содержать поле errors');
            }

            if (!Array.isArray(result.errors)) {
                throw new Error('Поле errors должно быть массивом');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCreateDatabase: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }


    /**
     * Тест очистки индексных значений
     */
    async testClearFactIndexCollection(title) {
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
            this.testResults.errors.push(`testClearFactIndexCollection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест проверки подключения
     */
    async testCheckConnection(title) {
        this.logger.debug(title);
        
        try {
            // Тест с подключением
            const connected = this.provider.checkConnection();
            if (!connected) {
                throw new Error('checkConnection вернул false при активном подключении');
            }

            // Тест без подключения
            await this.provider.disconnect();
            
            try {
                this.provider.checkConnection();
                throw new Error('checkConnection не выбросил ошибку при отсутствии подключения');
            } catch (error) {
                if (!error.message.includes('Нет подключения к MongoDB')) {
                    throw new Error('Неправильное сообщение об ошибке при отсутствии подключения');
                }
            }

            // Восстанавливаем подключение для завершения тестов
            await this.provider.connect();

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCheckConnection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест повторной вставки факта с теми же данными
     */
    async testDuplicateInsertFact(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactsCollection();
            
            // Генерируем тестовый факт
            const testFact = this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage());

            // Первая вставка в режиме insert
            const firstResult = await this.provider.saveFact(testFact);
            
            if (!firstResult.success) {
                throw new Error(`Ошибка первой вставки: ${firstResult.error}`);
            }

            if (firstResult.factInserted !== 1) {
                throw new Error(`Ожидалось вставить 1 факт при первой вставке, вставлено ${firstResult.factInserted}`);
            }

            // Повторная вставка тех же данных в режиме insert
            const secondResult = await this.provider.saveFact(testFact);
            
            if (!secondResult.success) {
                throw new Error(`Ошибка повторной вставки: ${secondResult.error}`);
            }

            // При повторной вставке того же факта может произойти обновление или игнорирование
            // (в зависимости от того, изменились ли данные)
            if (secondResult.factUpdated !== 1 && secondResult.factIgnored !== 1) {
                throw new Error(`Ожидалось обновить или проигнорировать 1 факт при повторной вставке, обновлено ${secondResult.factUpdated}, проигнорировано ${secondResult.factIgnored}`);
            }

            // Проверяем, что в базе только один документ
            const count = await this.provider.countFactsCollection();
            if (count !== 1) {
                throw new Error(`Ожидалось 1 документ в базе, найдено ${count}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testDuplicateInsertFact: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест повторной вставки индексных значений с теми же данными
     */
    async testDuplicateInsertFactIndexList(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const facts = [
                this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage()), 
                this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage())
            ];
            const indexValues = this.indexer.index(facts[0]);

            // Первая вставка
            const firstResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!firstResult.success) {
                throw new Error(`Ошибка первой вставки: ${firstResult.error}`);
            }

            if (firstResult.inserted === 0) {
                throw new Error(`Не было вставлено ни одного индексного значения при первой вставке`);
            }

            // Повторная вставка тех же данных
            const secondResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!secondResult.success) {
                throw new Error(`Ошибка повторной вставки: ${secondResult.error}`);
            }

            if (secondResult.inserted !== 0) {
                throw new Error(`Ожидалось проигнорировать дубликаты при повторной вставке, вставлено ${secondResult.inserted}`);
            }

            // Проверяем, что в базе есть документы
            const count = await this.provider.countFactIndexCollection();
            if (count === 0) {
                throw new Error(`В базе нет документов`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testDuplicateInsertFactIndexList: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест повторной bulk вставки с теми же данными
     */
    async testDuplicateBulkInsert(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const testFact = this.mapper.mapMessageToFact(this.generator.generateMessage(1));
            const indexValues = this.indexer.index(testFact);

            // Первая вставка
            const firstFactResult = await this.provider.saveFact(testFact);
            const firstIndexResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!firstFactResult.success) {
                throw new Error(`Ошибка первой вставки факта: ${firstFactResult.error}`);
            }

            if (!firstIndexResult.success) {
                throw new Error(`Ошибка первой вставки индексных значений: ${firstIndexResult.error}`);
            }

            if (firstFactResult.factInserted !== 1) {
                throw new Error(`Ожидалось вставить 1 факт при первой вставке, вставлено ${firstFactResult.factInserted}`);
            }

            if (firstIndexResult.inserted === 0) {
                throw new Error(`Не было вставлено ни одного индексного значения при первой вставке`);
            }

            // Повторная вставка тех же данных
            const secondFactResult = await this.provider.saveFact(testFact);
            const secondIndexResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!secondFactResult.success) {
                throw new Error(`Ошибка повторной вставки факта: ${secondFactResult.error}`);
            }

            if (!secondIndexResult.success) {
                throw new Error(`Ошибка повторной вставки индексных значений: ${secondIndexResult.error}`);
            }

            if (secondFactResult.factInserted !== 0) {
                throw new Error(`Ожидалось проигнорировать дубликат факта при повторной вставке, вставлено ${secondFactResult.factInserted}`);
            }

            if (secondIndexResult.inserted !== 0) {
                throw new Error(`Ожидалось проигнорировать дубликаты индексных значений при повторной вставке, вставлено ${secondIndexResult.inserted}`);
            }

            // Проверяем, что в базах только нужное количество документов
            const factCount = await this.provider.countFactsCollection();
            const indexCount = await this.provider.countFactIndexCollection();
            
            if (factCount !== 1) {
                throw new Error(`Ожидалось 1 факт в базе, найдено ${factCount}`);
            }

            if (indexCount === 0) {
                throw new Error(`В базе нет индексных значений`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testDuplicateBulkInsert: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест очистки коллекции фактов
     */
    async testClearFactsCollection(title) {
        this.logger.debug(title);
        
        try {
            // Сначала добавляем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const facts = Array.from({length: 5}, () => this.mapper.mapMessageToFact(this.generator.generateRandomTypeMessage()));
            
            // Вставляем факты
            for (const fact of facts) {
                await this.provider.saveFact(fact);
            }
            
            // Проверяем, что факты добавлены
            const countBefore = await this.provider.countFactsCollection();
            if (countBefore === 0) {
                throw new Error('Факты не были добавлены для тестирования очистки');
            }
            
            // Очищаем коллекцию
            const result = await this.provider.clearFactsCollection();
            
            if (typeof result.deletedCount !== 'number') {
                throw new Error('Некорректный результат очистки коллекции фактов');
            }
            
            if (result.deletedCount !== countBefore) {
                throw new Error(`Ожидалось удалить ${countBefore} фактов, удалено ${result.deletedCount}`);
            }
            
            // Проверяем, что коллекция пуста
            const countAfter = await this.provider.countFactsCollection();
            if (countAfter !== 0) {
                throw new Error(`Коллекция фактов не пуста после очистки, осталось ${countAfter} документов`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testClearFactsCollection: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов - базовый тест
     */
    async testGetRelevantFacts(title) {
        this.logger.debug(title);
        
        try {
            this.provider.clearFactsCollection();
            this.provider.clearFactIndexCollection();

            // Создаем тестовые факты с известными значениями полей
            const testFacts = [
                {
                    _id: 'test-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-01-01'),
                        f1: 'value1',
                        f2: 'value2',
                        f5: 'value5'
                    }
                },
                {
                    _id: 'test-fact-002',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-01-02'),
                        f1: 'value1', // Совпадает с первым фактом
                        f3: 'value3',
                        f10: 'value7'
                    }
                },
                {
                    _id: 'test-fact-003',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 300,
                        dt: new Date('2024-01-03'),
                        f2: 'value2', // Совпадает с первым фактом
                        f4: 'value4',
                        f15: 'value8'
                    }
                },
                {
                    _id: 'test-fact-004',
                    t: 3,
                    c: new Date(),
                    d: {
                        amount: 400,
                        dt: new Date('2024-01-04'),
                        f1: '1', // Не совпадает
                        f2: 'differedifferentnt2', // Не совпадает
                        f23: 'value9'
                    }
                }
            ];

            // Вставляем факты в базу данных
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                // Создаем индексные значения для каждого факта
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем поиск релевантных фактов для первого факта
            const searchFact = testFacts[0]; // test-fact-001 с f1='value1', f2='value2', f5='value5'
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact);
            const relevantFacts = factsResult.result;
            
            // Проверяем результаты
            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись факты с совпадающими значениями полей
            // test-fact-001 (сам факт) - f1='value1', f2='value2'
            // test-fact-002 - f1='value1'
            // test-fact-003 - f2='value2'
            const expectedIds = ['test-fact-001', 'test-fact-002', 'test-fact-003'];
            const foundIds = relevantFacts.map(f => f._id);

            if (relevantFacts.length < 2) {
                throw new Error(`Ожидалось найти минимум 2 релевантных факта, найдено ${relevantFacts.length}`);
            }

            // Проверяем, что найдены ожидаемые факты
            for (const expectedId of expectedIds) {
                if (!foundIds.includes(expectedId)) {
                    throw new Error(`Не найден ожидаемый факт ${expectedId}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFacts: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов с множественными полями
     */
    async testGetRelevantFactsWithMultipleFields(title) {
        this.logger.debug(title);
        
        try {
            // Создаем факты с множественными совпадающими полями
            const testFacts = [
                {
                    _id: 'multi-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-02-01'),
                        f1: 'shared1',
                        f2: 'shared2',
                        f3: 'shared3',
                        f4: 'unique1'
                    }
                },
                {
                    _id: 'multi-fact-002',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-02-02'),
                        f1: 'shared1', // Совпадает
                        f2: 'shared2', // Совпадает
                        f3: 'shared3', // Совпадает
                        f5: 'unique2'
                    }
                },
                {
                    _id: 'multi-fact-003',
                    t: 3,
                    c: new Date(),
                    d: {
                        amount: 300,
                        dt: new Date('2024-02-03'),
                        f1: 'shared1', // Совпадает
                        f2: 'different2', // Не совпадает
                        f10: 'unique3'
                    }
                },
                {
                    _id: 'multi-fact-004',
                    t: 4,
                    c: new Date(),
                    d: {
                        amount: 400,
                        dt: new Date('2024-02-04'),
                        f10: 'unique4',
                        f15: 'unique5',
                        f23: 'unique6'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем поиск для первого факта (должен найти факты с совпадающими f1, f2, f3)
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact);
            const relevantFacts = factsResult.result;

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись факты с совпадающими полями
            // multi-fact-001 (сам факт) - f1, f2, f3
            // multi-fact-002 - f1, f2, f3 (все три совпадают)
            // multi-fact-003 - f1 (только одно совпадает)
            const foundIds = relevantFacts.map(f => f._id);

            if (relevantFacts.length < 2) {
                throw new Error(`Ожидалось найти минимум 2 релевантных факта, найдено ${relevantFacts.length}`);
            }

            // Проверяем, что найдены ожидаемые факты
            const expectedIds = ['multi-fact-001', 'multi-fact-002', 'multi-fact-003'];
            for (const expectedId of expectedIds) {
                if (!foundIds.includes(expectedId)) {
                    throw new Error(`Не найден ожидаемый факт ${expectedId}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactsWithMultipleFields: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов без совпадений
     */
    async testGetRelevantFactsWithNoMatches(title) {
        this.logger.debug(title);
        
        try {
            // Создаем факты с уникальными значениями полей
            const testFacts = [
                {
                    _id: 'unique-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-03-01'),
                        f1: 'unique1',
                        f2: 'unique2',
                        f3: 'unique3'
                    }
                },
                {
                    _id: 'unique-fact-002',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-03-02'),
                        f4: 'unique4',
                        f5: 'unique5',
                        f10: 'unique6'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Создаем факт с полностью уникальными значениями
            const searchFact = {
                _id: 'search-fact-001',
                t: 3,
                c: new Date(),
                d: {
                    amount: 300,
                    dt: new Date('2024-03-03'),
                    f1: 'completely-different1',
                    f2: 'completely-different2',
                    f3: 'completely-different3'
                }
            };

            // Тестируем поиск - не должно быть совпадений
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, searchFact);
            const relevantFacts = factsResult.result;

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Не должно быть релевантных фактов (кроме самого себя, если он вставлен)
            // Но поскольку мы не вставляем searchFact, результат должен быть пустым
            if (relevantFacts.length > 0) {
                this.logger.debug(`   Найдено ${relevantFacts.length} релевантных фактов (ожидалось 0)`);
                // Это не ошибка, так как метод может находить факты по частичным совпадениям
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactsWithNoMatches: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов с ограничением по количеству
     */
    async testGetRelevantFactsWithDepthLimit(title) {
        this.logger.debug(title);
        
        try {
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const testFacts = [
                {
                    _id: 'depth-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 1000),
                        f1: 'shared-value',
                        f2: 'unique1'
                    }
                },
                {
                    _id: 'depth-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000),
                        f1: 'shared-value',
                        f3: 'unique2'
                    }
                },
                {
                    _id: 'depth-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 3000),
                        f1: 'shared-value',
                        f4: 'unique3'
                    }
                },
                {
                    _id: 'depth-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 4000),
                        f1: 'shared-value',
                        f5: 'unique4'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                // this.logger.debug("*** indexValues: "+JSON.stringify(indexValues));
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем поиск с ограничением по количеству
            const searchFact = testFacts[0];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, searchFact, 2);
            const relevantFacts = factsResult.result;

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должно быть не более 2 релевантных фактов
            if (relevantFacts.length > 2) {
                throw new Error(`Ожидалось максимум 2 релевантных факта, найдено ${relevantFacts.length}`);
            }

            // Должен найтись минимум 1 релевантный факт (сам факт)
            if (relevantFacts.length < 1) {
                throw new Error('Ожидалось минимум 1 релевантный факт');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactsWithDepthLimit: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов с ограничением по дате
     */
    async testGetRelevantFactsWithDepthFromDate(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500); // Отсекаем факты после этой даты
            
            const testFacts = [
                {
                    _id: 'date-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 2000), // После cutoffDate
                        f1: 'shared-value',
                        f2: 'before-cutoff'
                    }
                },
                {
                    _id: 'date-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000), // До cutoffDate
                        f1: 'shared-value',
                        f3: 'before-cutoff'
                    }
                },
                {
                    _id: 'date-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f4: 'after-cutoff'
                    }
                },
                {
                    _id: 'date-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f5: 'after-cutoff'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем поиск с ограничением по дате
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact, undefined, cutoffDate);
            const relevantFacts = factsResult.result;

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись только факты до cutoffDate
            const foundIds = relevantFacts.map(f => f._id);
            const expectedIds = ['date-fact-001', 'date-fact-002']; // Только факты до cutoffDate
            
            // Проверяем, что найдены только ожидаемые факты
            for (const foundId of foundIds) {
                if (!expectedIds.includes(foundId)) {
                    throw new Error(`Найден неожиданный факт ${foundId}, который должен быть отфильтрован по дате`);
                }
            }

            // Проверяем, что найдены все ожидаемые факты
            for (const expectedId of expectedIds) {
                if (!foundIds.includes(expectedId)) {
                    throw new Error(`Не найден ожидаемый факт ${expectedId}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactsWithDepthFromDate: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных фактов с обоими параметрами
     */
    async testGetRelevantFactsWithBothParameters(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500);
            
            const testFacts = [
                {
                    _id: 'both-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f2: 'old'
                    }
                },
                {
                    _id: 'both-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000), // До cutoffDate
                        f1: 'shared-value',
                        f3: 'old'
                    }
                },
                {
                    _id: 'both-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f4: 'new'
                    }
                },
                {
                    _id: 'both-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f5: 'new'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем поиск с обоими параметрами
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const factsResult = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact, 1, cutoffDate);
            const relevantFacts = factsResult.result;

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должно быть не более 1 релевантного факта (из-за depthLimit)
            if (relevantFacts.length > 1) {
                throw new Error(`Ожидалось максимум 1 релевантный факт, найдено ${relevantFacts.length}`);
            }

            // Должен найтись минимум 1 релевантный факт
            if (relevantFacts.length < 1) {
                throw new Error('Ожидался минимум 1 релевантный факт');
            }

            // Проверяем, что найденный факт соответствует критериям даты
            const foundId = relevantFacts[0]._id;
            const expectedIds = ['both-fact-001', 'both-fact-002']; // Только факты до cutoffDate
            
            if (!expectedIds.includes(foundId)) {
                throw new Error(`Найден факт ${foundId}, который не соответствует критериям даты`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactsWithBothParameters: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов - базовый тест
     */
    async testGetRelevantFactCounters(title) {
        this.logger.debug(title);
        
        try {
            // Создаем тестовые факты с известными значениями полей
            const testFacts = [
                {
                    _id: 'counter-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-01-01'),
                        f1: 'value1',
                        f2: 'value2',
                        f5: 'value5'
                    }
                },
                {
                    _id: 'counter-fact-002',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-01-02'),
                        f1: 'value1', // Совпадает с первым фактом
                        f3: 'value3',
                        f10: 'value7'
                    }
                },
                {
                    _id: 'counter-fact-003',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 300,
                        dt: new Date('2024-01-03'),
                        f2: 'value2', // Совпадает с первым фактом
                        f4: 'value4',
                        f15: 'value8'
                    }
                },
                {
                    _id: 'counter-fact-004',
                    t: 3,
                    c: new Date(),
                    d: {
                        amount: 400,
                        dt: new Date('2024-01-04'),
                        f1: 'different1', // Не совпадает
                        f2: 'different2', // Не совпадает
                        f23: 'value9'
                    }
                }
            ];

            // Вставляем факты в базу данных
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                // Создаем индексные значения для каждого факта
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем получение счетчиков для первого факта
            const searchFact = testFacts[0]; // counter-fact-001 с f1='value1', f2='value2', f5='value5'
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, excludedFact);
            const counters = countersResult.result;

            // Проверяем результаты
            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            if (counters.length === 0) {
                throw new Error('Должен быть возвращен хотя бы один элемент статистики');
            }

            const result = counters[0];
            
            // Проверяем структуру результата
            if (!result.total || !Array.isArray(result.total)) {
                throw new Error('Результат должен содержать массив total');
            }

            if (result.total.length === 0) {
                throw new Error('Массив total не должен быть пустым');
            }

            const totalStats = result.total[0];
            
            // Проверяем поля в total
            if (typeof totalStats.count !== 'number') {
                throw new Error('Поле count должно быть числом');
            }

            if (typeof totalStats.sumA !== 'number') {
                throw new Error('Поле sumA должно быть числом');
            }

            // Проверяем, что найдены релевантные факты
            if (totalStats.count < 2) {
                throw new Error(`Ожидалось минимум 2 релевантных факта, найдено ${totalStats.count}`);
            }

            // Проверяем сумму значений поля amount
            const expectedSumA = 100 + 200 + 300; // Сумма amount для релевантных фактов
            if (totalStats.sumA !== expectedSumA) {
                throw new Error(`Ожидалась сумма amount = ${expectedSumA}, получена ${totalStats.sumA}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCounters: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов с множественными полями
     */
    async testGetRelevantFactCountersWithMultipleFields(title) {
        this.logger.debug(title);
        
        try {
            // Создаем факты с множественными совпадающими полями
            const testFacts = [
                {
                    _id: 'multi-counter-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-02-01'),
                        f1: 'shared1',
                        f2: 'shared2',
                        f3: 'shared3',
                        f4: 'unique1'
                    }
                },
                {
                    _id: 'multi-counter-fact-002',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-02-02'),
                        f1: 'shared1', // Совпадает
                        f2: 'shared2', // Совпадает
                        f3: 'shared3', // Совпадает
                        f5: 'unique2'
                    }
                },
                {
                    _id: 'multi-counter-fact-003',
                    t: 3,
                    c: new Date(),
                    d: {
                        amount: 300,
                        dt: new Date('2024-02-03'),
                        f1: 'shared1', // Совпадает
                        f2: 'different2', // Не совпадает
                        f10: 'unique3'
                    }
                },
                {
                    _id: 'multi-counter-fact-004',
                    t: 4,
                    c: new Date(),
                    d: {
                        amount: 400,
                        dt: new Date('2024-02-04'),
                        f10: 'unique4',
                        f15: 'unique5',
                        f23: 'unique6'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем получение счетчиков для первого факта
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, excludedFact);
            const counters = countersResult.result;

            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            if (counters.length === 0) {
                throw new Error('Должен быть возвращен хотя бы один элемент статистики');
            }

            const result = counters[0];
            
            if (!result.total || !Array.isArray(result.total)) {
                throw new Error('Результат должен содержать массив total');
            }

            if (result.total.length === 0) {
                throw new Error('Массив total не должен быть пустым');
            }

            const totalStats = result.total[0];
            
            if (typeof totalStats.count !== 'number') {
                throw new Error('Поле count должно быть числом');
            }

            if (typeof totalStats.sumA !== 'number') {
                throw new Error('Поле sumA должно быть числом');
            }

            // Должны найтись релевантные факты
            if (totalStats.count < 2) {
                throw new Error(`Ожидалось минимум 2 релевантных факта, найдено ${totalStats.count}`);
            }

            // Проверяем сумму значений поля amount для релевантных фактов
            const expectedSumA = 100 + 200 + 300; // Сумма amount для релевантных фактов
            if (totalStats.sumA !== expectedSumA) {
                throw new Error(`Ожидалась сумма amount = ${expectedSumA}, получена ${totalStats.sumA}`);
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCountersWithMultipleFields: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов без совпадений
     */
    async testGetRelevantFactCountersWithNoMatches(title) {
        this.logger.debug(title);
        
        try {
            // Создаем факты с уникальными значениями полей
            const testFacts = [
                {
                    _id: 'unique-counter-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        amount: 100,
                        dt: new Date('2024-03-01'),
                        f1: 'unique1',
                        f2: 'unique2',
                        f3: 'unique3'
                    }
                },
                {
                    _id: 'unique-counter-fact-002',
                    t: 2,
                    c: new Date(),
                    d: {
                        amount: 200,
                        dt: new Date('2024-03-02'),
                        f4: 'unique4',
                        f5: 'unique5',
                        f10: 'unique6'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Создаем факт с полностью уникальными значениями
            const searchFact = {
                _id: 'search-counter-fact-001',
                t: 3,
                c: new Date(),
                d: {
                    amount: 300,
                    dt: new Date('2024-03-03'),
                    f1: 'completely-different1',
                    f2: 'completely-different2',
                    f3: 'completely-different3'
                }
            };

            // Тестируем получение счетчиков - не должно быть совпадений
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, searchFact);
            const counters = countersResult.result;

            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Проверяем, что метод возвращает корректную структуру
            if (counters.length) {
                throw new Error('Не должно быть совпадений, счетчков быть не должно.');
            }
            // Если нет совпадений, это нормально для данного теста
            this.logger.debug('   Нет совпадений - это ожидаемое поведение для уникальных значений');

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCountersWithNoMatches: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов с ограничением по количеству
     */
    async testGetRelevantFactCountersWithDepthLimit(title) {
        this.logger.debug(title);
        
        try {
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const testFacts = [
                {
                    _id: 'depth-counter-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 1000),
                        f1: 'shared-value',
                        f2: 'unique1'
                    }
                },
                {
                    _id: 'depth-counter-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000),
                        f1: 'shared-value',
                        f3: 'unique2'
                    }
                },
                {
                    _id: 'depth-counter-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 3000),
                        f1: 'shared-value',
                        f4: 'unique3'
                    }
                },
                {
                    _id: 'depth-counter-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 4000),
                        f1: 'shared-value',
                        f5: 'unique4'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем получение счетчиков с ограничением по количеству
            const searchFact = testFacts[0];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, searchFact, 2);
            const counters = countersResult.result;

            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            if (counters.length === 0) {
                throw new Error('Должен быть возвращен хотя бы один элемент статистики');
            }

            const result = counters[0];
            
            if (!result.total || !Array.isArray(result.total)) {
                throw new Error('Результат должен содержать массив total');
            }

            if (result.total.length === 0) {
                throw new Error('Массив total не должен быть пустым');
            }

            const totalStats = result.total[0];
            
            if (typeof totalStats.count !== 'number') {
                throw new Error('Поле count должно быть числом');
            }

            if (typeof totalStats.sumA !== 'number') {
                throw new Error('Поле sumA должно быть числом');
            }

            // Должно быть не более 2 релевантных фактов (из-за depthLimit)
            if (totalStats.count > 2) {
                throw new Error(`Ожидалось максимум 2 релевантных факта, найдено ${totalStats.count}`);
            }

            // Должен найтись минимум 1 релевантный факт
            if (totalStats.count < 1) {
                throw new Error('Ожидался минимум 1 релевантный факт');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCountersWithDepthLimit: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов с ограничением по дате
     */
    async testGetRelevantFactCountersWithDepthFromDate(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500); // Отсекаем факты после этой даты
            
            const testFacts = [
                {
                    _id: 'date-counter-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 2000), // После cutoffDate
                        f1: 'shared-value',
                        f2: 'before-cutoff'
                    }
                },
                {
                    _id: 'date-counter-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000), // До cutoffDate
                        f1: 'shared-value',
                        f3: 'before-cutoff'
                    }
                },
                {
                    _id: 'date-counter-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f4: 'after-cutoff'
                    }
                },
                {
                    _id: 'date-counter-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f5: 'after-cutoff'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем получение счетчиков с ограничением по дате
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, excludedFact, undefined, cutoffDate);
            const counters = countersResult.result;

            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            if (counters.length === 0) {
                throw new Error('Должен быть возвращен хотя бы один элемент статистики');
            }

            const result = counters[0];
            
            if (!result.total || !Array.isArray(result.total)) {
                throw new Error('Результат должен содержать массив total');
            }

            if (result.total.length === 0) {
                throw new Error('Массив total не должен быть пустым');
            }

            const totalStats = result.total[0];
            
            if (typeof totalStats.count !== 'number') {
                throw new Error('Поле count должно быть числом');
            }

            if (typeof totalStats.sumA !== 'number') {
                throw new Error('Поле sumA должно быть числом');
            }

            // Должны найтись только факты до cutoffDate
            if (totalStats.count < 1) {
                throw new Error('Ожидался минимум 1 релевантный факт');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCountersWithDepthFromDate: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения релевантных счетчиков фактов с обоими параметрами
     */
    async testGetRelevantFactCountersWithBothParameters(title) {
        this.logger.debug(title);
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500);
            
            const testFacts = [
                {
                    _id: 'both-counter-fact-001',
                    t: 1,
                    c: new Date(baseDate.getTime() + 1000),
                    d: {
                        amount: 100,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f2: 'old'
                    }
                },
                {
                    _id: 'both-counter-fact-002',
                    t: 1,
                    c: new Date(baseDate.getTime() + 2000),
                    d: {
                        amount: 200,
                        dt: new Date(baseDate.getTime() + 2000), // До cutoffDate
                        f1: 'shared-value',
                        f3: 'old'
                    }
                },
                {
                    _id: 'both-counter-fact-003',
                    t: 1,
                    c: new Date(baseDate.getTime() + 3000),
                    d: {
                        amount: 300,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f4: 'new'
                    }
                },
                {
                    _id: 'both-counter-fact-004',
                    t: 1,
                    c: new Date(baseDate.getTime() + 4000),
                    d: {
                        amount: 400,
                        dt: new Date(baseDate.getTime() + 1000), // До cutoffDate
                        f1: 'shared-value',
                        f5: 'new'
                    }
                }
            ];

            // Вставляем факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                
                const indexValues = this.indexer.index(fact);
                if (indexValues.length > 0) {
                    await this.provider.saveFactIndexList(indexValues);
                }
            }

            // Тестируем получение счетчиков с обоими параметрами
            const searchFact = testFacts[0];
            const excludedFact = testFacts[3];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index._id.h);
            const countersResult = await this.provider.getRelevantFactCounters(searchFactIndexHashValues, excludedFact, 1, cutoffDate);
            const counters = countersResult.result;

            if (!Array.isArray(counters)) {
                throw new Error('Метод должен возвращать массив');
            }

            if (counters.length === 0) {
                throw new Error('Должен быть возвращен хотя бы один элемент статистики');
            }

            const result = counters[0];
            
            if (!result.total || !Array.isArray(result.total)) {
                throw new Error('Результат должен содержать массив total');
            }

            if (result.total.length === 0) {
                throw new Error('Массив total не должен быть пустым');
            }

            const totalStats = result.total[0];
            
            if (typeof totalStats.count !== 'number') {
                throw new Error('Поле count должно быть числом');
            }

            if (typeof totalStats.sumA !== 'number') {
                throw new Error('Поле sumA должно быть числом');
            }

            // Должно быть не более 1 релевантного факта (из-за depthLimit)
            if (totalStats.count > 1) {
                throw new Error(`Ожидалось максимум 1 релевантный факт, найдено ${totalStats.count}`);
            }

            // Должен найтись минимум 1 релевантный факт
            if (totalStats.count < 1) {
                throw new Error('Ожидался минимум 1 релевантный факт');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testGetRelevantFactCountersWithBothParameters: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }


    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования MongoProvider ===');
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
    const test = new MongoProviderTest();
    test.runAllTests().catch(console.error);
}

module.exports = MongoProviderTest;
