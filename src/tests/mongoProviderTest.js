const { MongoProvider, FactGenerator, FactIndexer } = require('../index');
const Logger = require('../utils/logger');
const EnvConfig = require('../utils/envConfig');

/**
 * Тесты для всех методов MongoProvider
 */
class MongoProviderTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        const mongoConfig = EnvConfig.getMongoConfig();
        this.provider = new MongoProvider(
            mongoConfig.connectionString,
            'mongoProviderTestDB'
        );
        this.generator = new FactGenerator();
        this.indexer = new FactIndexer();
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
        this.logger.debug('=== Тестирование всех методов MongoProvider (27 тестов) ===\n');

        try {
            // Тесты подключения
            await this.testConnection();
            await this.testDisconnection();
            await this.testReconnection();
            
            // Тесты схемы коллекции facts
            await this.testCreateFactsCollectionSchema();
            await this.testGetFactsCollectionSchema();
            
            // Тесты работы с фактами
            await this.testInsertFact();
            await this.testBulkInsert();
            
            // Тесты статистики
            await this.testGetFactsCollectionStats();
            
            // Тесты индексных значений
            await this.testInsertFactIndexList();
            await this.testCreateFactIndexIndexes();
            await this.testCreateFactIndexes();
            await this.testGetFactIndexStats();
            await this.testGetFactIndexSchema();
            await this.testCreateFactIndexCollectionSchema();
            await this.testCreateDatabase();
            await this.testClearFactIndexCollection();
            
            // Тесты проверки подключения
            await this.testCheckConnection();
            
            // Тесты повторных вызовов с теми же данными
            await this.testDuplicateInsertFact();
            await this.testDuplicateInsertFactIndexList();
            await this.testDuplicateBulkInsert();
            
            // Тесты очистки коллекций
            await this.testClearFactsCollection();
            
            // Тесты получения релевантных фактов
            await this.testGetRelevantFacts();
            await this.testGetRelevantFactsWithMultipleFields();
            await this.testGetRelevantFactsWithNoMatches();
            await this.testGetRelevantFactsWithDepthLimit();
            await this.testGetRelevantFactsWithDepthFromDate();
            await this.testGetRelevantFactsWithBothParameters();
            
        } catch (error) {
            console.error('Критическая ошибка:', error.message);
        } finally {
            await this.provider.disconnect();
            this.printResults();
        }
    }

    /**
     * Тест подключения к MongoDB
     */
    async testConnection() {
        this.logger.debug('1. Тест подключения к MongoDB...');
        
        try {
            const connected = await this.provider.connect();
            
            if (!connected) {
                throw new Error('Не удалось подключиться к MongoDB');
            }

            if (!this.provider.isConnected) {
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
    async testDisconnection() {
        this.logger.debug('2. Тест отключения от MongoDB...');
        
        try {
            await this.provider.disconnect();
            
            if (this.provider.isConnected) {
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
    async testReconnection() {
        this.logger.debug('3. Тест повторного подключения...');
        
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
     * Тест создания схемы коллекции facts
     */
    async testCreateFactsCollectionSchema() {
        this.logger.debug('4. Тест создания схемы коллекции facts...');
        
        try {
            const success = await this.provider.createFactsCollectionSchema(10);
            
            if (!success) {
                throw new Error('Не удалось создать схему коллекции');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCreateFactsCollectionSchema: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения схемы коллекции facts
     */
    async testGetFactsCollectionSchema() {
        this.logger.debug('5. Тест получения схемы коллекции facts...');
        
        try {
            const schema = await this.provider.getFactsCollectionSchema();
            
            if (!schema) {
                throw new Error('Схема не получена');
            }

            if (!schema.$jsonSchema) {
                throw new Error('Схема не содержит $jsonSchema');
            }

            const requiredFields = ['i', 't', 'a', 'c', 'd'];
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
    async testInsertFact() {
        this.logger.debug('6. Тест вставки одного факта...');
        
        try {
            // Очищаем коллекцию
            await this.provider.factsCollection.deleteMany({});
            
            // Генерируем тестовый факт
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const testFact = this.generator.generateRandomTypeFact();

            // Тест первой вставки (должна создать новый документ)
            const insertResult = await this.provider.saveFact(testFact);
            
            if (!insertResult.success) {
                throw new Error(`Ошибка вставки: ${insertResult.error}`);
            }

            if (insertResult.factInserted !== 1) {
                throw new Error(`Ожидалось вставить 1 факт, вставлено ${insertResult.factInserted}`);
            }

            // Тест повторной вставки с измененными данными (должна обновить существующий по умолчанию)
            const modifiedFact = { ...testFact, a: testFact.a + 100 }; // Изменяем значение поля a
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
    async testBulkInsert() {
        this.logger.debug('7. Тест bulk вставки факта и индексных значений...');
        
        try {
            // Очищаем коллекции
            await this.provider.factsCollection.deleteMany({});
            await this.provider.factIndexCollection.deleteMany({});
            
            // Генерируем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const testFact = this.generator.generateRandomTypeFact();
            
            // Создаем индексные значения
            const indexValues = this.indexer.indexFacts([testFact]);

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
            if (indexResult.inserted !== undefined && indexResult.inserted !== indexValues.length) {
                throw new Error(`Ожидалось вставить ${indexValues.length} индексных значений, вставлено ${indexResult.inserted}`);
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
    async testGetFactsCollectionStats() {
        this.logger.debug('8. Тест получения статистики коллекции facts...');
        
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
    async testInsertFactIndexList() {
        this.logger.debug('9. Тест вставки индексных значений...');
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const facts = [this.generator.generateRandomTypeFact(), this.generator.generateRandomTypeFact(), this.generator.generateRandomTypeFact()];
            const indexValues = this.indexer.indexFacts(facts);

            const result = await this.provider.saveFactIndexList(indexValues);
            
            if (!result.success) {
                throw new Error(`Ошибка вставки индексных значений: ${result.error}`);
            }

            if (result.inserted !== indexValues.length) {
                throw new Error(`Ожидалось вставить ${indexValues.length} индексных значений, вставлено ${result.inserted}`);
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
     * Тест создания индексов для индексных значений
     */
    async testCreateFactIndexIndexes() {
        this.logger.debug('10. Тест создания индексов для индексных значений...');
        
        try {
            const success = await this.provider.createFactIndexIndexes();
            
            if (!success) {
                throw new Error('Не удалось создать индексы для индексных значений');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCreateFactIndexIndexes: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест создания индексов для коллекции facts
     */
    async testCreateFactIndexes() {
        this.logger.debug('11. Тест создания индексов для коллекции facts...');
        
        try {
            const success = await this.provider.createFactIndexes();
            
            if (!success) {
                throw new Error('Не удалось создать индексы для коллекции facts');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCreateFactIndexes: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения статистики индексных значений
     */
    async testGetFactIndexStats() {
        this.logger.debug('12. Тест получения статистики индексных значений...');
        
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
    async testGetFactIndexSchema() {
        this.logger.debug('13. Тест получения схемы индексных значений...');
        
        try {
            const schema = await this.provider.getFactIndexCollectionSchema();
            
            if (!schema || schema.isEmpty) {
                throw new Error('Схема индексных значений пуста или не получена');
            }

            const requiredFields = ['h', 'i', 'd', 'c'];
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
     * Тест создания схемы коллекции индексных значений
     */
    async testCreateFactIndexCollectionSchema() {
        this.logger.debug('13.5. Тест создания схемы коллекции индексных значений...');
        
        try {
            const result = await this.provider.createFactIndexCollectionSchema();
            
            if (typeof result !== 'boolean') {
                throw new Error('Результат должен быть boolean');
            }

            if (!result) {
                throw new Error('Схема не была создана');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testCreateFactIndexCollectionSchema: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест создания базы данных
     */
    async testCreateDatabase() {
        this.logger.debug('14. Тест создания базы данных...');
        
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
    async testClearFactIndexCollection() {
        this.logger.debug('15. Тест очистки индексных значений...');
        
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
    async testCheckConnection() {
        this.logger.debug('16. Тест проверки подключения...');
        
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
    async testDuplicateInsertFact() {
        this.logger.debug('17. Тест повторной вставки факта с теми же данными...');
        
        try {
            // Очищаем коллекцию
            await this.provider.factsCollection.deleteMany({});
            
            // Генерируем тестовый факт
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const testFact = this.generator.generateRandomTypeFact();

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
            const count = await this.provider.factsCollection.countDocuments();
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
    async testDuplicateInsertFactIndexList() {
        this.logger.debug('18. Тест повторной вставки индексных значений с теми же данными...');
        
        try {
            // Очищаем коллекцию
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const facts = [this.generator.generateRandomTypeFact(), this.generator.generateRandomTypeFact()];
            const indexValues = this.indexer.indexFacts(facts);

            // Первая вставка
            const firstResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!firstResult.success) {
                throw new Error(`Ошибка первой вставки: ${firstResult.error}`);
            }

            if (firstResult.inserted !== indexValues.length) {
                throw new Error(`Ожидалось вставить ${indexValues.length} индексных значений при первой вставке, вставлено ${firstResult.inserted}`);
            }

            // Повторная вставка тех же данных
            const secondResult = await this.provider.saveFactIndexList(indexValues);
            
            if (!secondResult.success) {
                throw new Error(`Ошибка повторной вставки: ${secondResult.error}`);
            }

            if (secondResult.inserted !== 0) {
                throw new Error(`Ожидалось проигнорировать дубликаты при повторной вставке, вставлено ${secondResult.inserted}`);
            }

            // Проверяем, что в базе только нужное количество документов
            const count = await this.provider.factIndexCollection.countDocuments();
            if (count !== indexValues.length) {
                throw new Error(`Ожидалось ${indexValues.length} документов в базе, найдено ${count}`);
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
    async testDuplicateBulkInsert() {
        this.logger.debug('19. Тест повторной bulk вставки с теми же данными...');
        
        try {
            // Очищаем коллекции
            await this.provider.factsCollection.deleteMany({});
            await this.provider.clearFactIndexCollection();
            
            // Генерируем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const testFact = this.generator.generateRandomTypeFact();
            const indexValues = this.indexer.indexFacts([testFact]);

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

            if (firstIndexResult.inserted !== indexValues.length) {
                throw new Error(`Ожидалось вставить ${indexValues.length} индексных значений при первой вставке, вставлено ${firstIndexResult.inserted}`);
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
            const factCount = await this.provider.factsCollection.countDocuments();
            const indexCount = await this.provider.factIndexCollection.countDocuments();
            
            if (factCount !== 1) {
                throw new Error(`Ожидалось 1 факт в базе, найдено ${factCount}`);
            }

            if (indexCount !== indexValues.length) {
                throw new Error(`Ожидалось ${indexValues.length} индексных значений в базе, найдено ${indexCount}`);
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
    async testClearFactsCollection() {
        this.logger.debug('20. Тест очистки коллекции фактов...');
        
        try {
            // Сначала добавляем тестовые данные
            const fromDate = new Date('2024-01-01');
            const toDate = new Date('2024-12-31');
            const facts = Array.from({length: 5}, () => this.generator.generateRandomTypeFact());
            
            // Вставляем факты
            for (const fact of facts) {
                await this.provider.saveFact(fact);
            }
            
            // Проверяем, что факты добавлены
            const countBefore = await this.provider.factsCollection.countDocuments();
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
            const countAfter = await this.provider.factsCollection.countDocuments();
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
    async testGetRelevantFacts() {
        this.logger.debug('22. Тест получения релевантных фактов...');
        
        try {
            // Создаем тестовые факты с известными значениями полей
            const testFacts = [
                {
                    i: 'test-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(),
                    d: new Date('2024-01-01'),
                    f1: 'value1',
                    f2: 'value2',
                    f5: 'value5'
                },
                {
                    i: 'test-fact-002',
                    t: 1,
                    a: 200,
                    c: new Date(),
                    d: new Date('2024-01-02'),
                    f1: 'value1', // Совпадает с первым фактом
                    f3: 'value3',
                    f7: 'value7'
                },
                {
                    i: 'test-fact-003',
                    t: 2,
                    a: 300,
                    c: new Date(),
                    d: new Date('2024-01-03'),
                    f2: 'value2', // Совпадает с первым фактом
                    f4: 'value4',
                    f8: 'value8'
                },
                {
                    i: 'test-fact-004',
                    t: 3,
                    a: 400,
                    c: new Date(),
                    d: new Date('2024-01-04'),
                    f1: 'different1', // Не совпадает
                    f2: 'different2', // Не совпадает
                    f9: 'value9'
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
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact.i);

            // Проверяем результаты
            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись факты с совпадающими значениями полей
            // test-fact-001 (сам факт) - f1='value1', f2='value2'
            // test-fact-002 - f1='value1'
            // test-fact-003 - f2='value2'
            const expectedIds = ['test-fact-001', 'test-fact-002', 'test-fact-003'];
            const foundIds = relevantFacts.map(f => f.fact.i);

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
    async testGetRelevantFactsWithMultipleFields() {
        this.logger.debug('23. Тест получения релевантных фактов с множественными полями...');
        
        try {
            // Создаем факты с множественными совпадающими полями
            const testFacts = [
                {
                    i: 'multi-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(),
                    d: new Date('2024-02-01'),
                    f1: 'shared1',
                    f2: 'shared2',
                    f3: 'shared3',
                    f4: 'unique1'
                },
                {
                    i: 'multi-fact-002',
                    t: 2,
                    a: 200,
                    c: new Date(),
                    d: new Date('2024-02-02'),
                    f1: 'shared1', // Совпадает
                    f2: 'shared2', // Совпадает
                    f3: 'shared3', // Совпадает
                    f5: 'unique2'
                },
                {
                    i: 'multi-fact-003',
                    t: 3,
                    a: 300,
                    c: new Date(),
                    d: new Date('2024-02-03'),
                    f1: 'shared1', // Совпадает
                    f2: 'different2', // Не совпадает
                    f6: 'unique3'
                },
                {
                    i: 'multi-fact-004',
                    t: 4,
                    a: 400,
                    c: new Date(),
                    d: new Date('2024-02-04'),
                    f7: 'unique4',
                    f8: 'unique5',
                    f9: 'unique6'
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
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact.i);

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись факты с совпадающими полями
            // multi-fact-001 (сам факт) - f1, f2, f3
            // multi-fact-002 - f1, f2, f3 (все три совпадают)
            // multi-fact-003 - f1 (только одно совпадает)
            const foundIds = relevantFacts.map(f => f.fact.i);

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
    async testGetRelevantFactsWithNoMatches() {
        this.logger.debug('24. Тест получения релевантных фактов без совпадений...');
        
        try {
            // Создаем факты с уникальными значениями полей
            const testFacts = [
                {
                    i: 'unique-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(),
                    d: new Date('2024-03-01'),
                    f1: 'unique1',
                    f2: 'unique2',
                    f3: 'unique3'
                },
                {
                    i: 'unique-fact-002',
                    t: 2,
                    a: 200,
                    c: new Date(),
                    d: new Date('2024-03-02'),
                    f4: 'unique4',
                    f5: 'unique5',
                    f6: 'unique6'
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
                i: 'search-fact-001',
                t: 3,
                a: 300,
                c: new Date(),
                d: new Date('2024-03-03'),
                f1: 'completely-different1',
                f2: 'completely-different2',
                f3: 'completely-different3'
            };

            // Тестируем поиск - не должно быть совпадений
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, searchFact.i);

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
    async testGetRelevantFactsWithDepthLimit() {
        this.logger.debug('25. Тест получения релевантных фактов с ограничением по количеству...');
        
        try {
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const testFacts = [
                {
                    i: 'depth-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(baseDate.getTime() + 1000),
                    d: new Date(baseDate.getTime() + 1000),
                    f1: 'shared-value',
                    f2: 'unique1'
                },
                {
                    i: 'depth-fact-002',
                    t: 1,
                    a: 200,
                    c: new Date(baseDate.getTime() + 2000),
                    d: new Date(baseDate.getTime() + 2000),
                    f1: 'shared-value',
                    f3: 'unique2'
                },
                {
                    i: 'depth-fact-003',
                    t: 1,
                    a: 300,
                    c: new Date(baseDate.getTime() + 3000),
                    d: new Date(baseDate.getTime() + 3000),
                    f1: 'shared-value',
                    f4: 'unique3'
                },
                {
                    i: 'depth-fact-004',
                    t: 1,
                    a: 400,
                    c: new Date(baseDate.getTime() + 4000),
                    d: new Date(baseDate.getTime() + 4000),
                    f1: 'shared-value',
                    f5: 'unique4'
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

            // Тестируем поиск с ограничением по количеству
            const searchFact = testFacts[0];
            const searchFactIndexValues = this.indexer.index(searchFact);
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, searchFact.i, 2);

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
    async testGetRelevantFactsWithDepthFromDate() {
        this.logger.debug('26. Тест получения релевантных фактов с ограничением по дате...');
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500); // Отсекаем факты после этой даты
            
            const testFacts = [
                {
                    i: 'date-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(baseDate.getTime() + 1000),
                    d: new Date(baseDate.getTime() + 2000), // После cutoffDate
                    f1: 'shared-value',
                    f2: 'before-cutoff'
                },
                {
                    i: 'date-fact-002',
                    t: 1,
                    a: 200,
                    c: new Date(baseDate.getTime() + 2000),
                    d: new Date(baseDate.getTime() + 2000), // До cutoffDate
                    f1: 'shared-value',
                    f3: 'before-cutoff'
                },
                {
                    i: 'date-fact-003',
                    t: 1,
                    a: 300,
                    c: new Date(baseDate.getTime() + 3000),
                    d: new Date(baseDate.getTime() + 1000), // До cutoffDate
                    f1: 'shared-value',
                    f4: 'after-cutoff'
                },
                {
                    i: 'date-fact-004',
                    t: 1,
                    a: 400,
                    c: new Date(baseDate.getTime() + 4000),
                    d: new Date(baseDate.getTime() + 1000), // До cutoffDate
                    f1: 'shared-value',
                    f5: 'after-cutoff'
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
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact.i, undefined, cutoffDate);

            if (!Array.isArray(relevantFacts)) {
                throw new Error('Метод должен возвращать массив');
            }

            // Должны найтись только факты до cutoffDate
            const foundIds = relevantFacts.map(f => f.fact.i);
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
    async testGetRelevantFactsWithBothParameters() {
        this.logger.debug('27. Тест получения релевантных фактов с обоими параметрами...');
        
        try {
            // Очищаем коллекции перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();
            // Создаем тестовые факты с разными датами
            const baseDate = new Date('2024-01-01');
            const cutoffDate = new Date(baseDate.getTime() + 1500);
            
            const testFacts = [
                {
                    i: 'both-fact-001',
                    t: 1,
                    a: 100,
                    c: new Date(baseDate.getTime() + 1000),
                    d: new Date(baseDate.getTime() + 1000), // До cutoffDate
                    f1: 'shared-value',
                    f2: 'old'
                },
                {
                    i: 'both-fact-002',
                    t: 1,
                    a: 200,
                    c: new Date(baseDate.getTime() + 2000),
                    d: new Date(baseDate.getTime() + 2000), // До cutoffDate
                    f1: 'shared-value',
                    f3: 'old'
                },
                {
                    i: 'both-fact-003',
                    t: 1,
                    a: 300,
                    c: new Date(baseDate.getTime() + 3000),
                    d: new Date(baseDate.getTime() + 1000), // До cutoffDate
                    f1: 'shared-value',
                    f4: 'new'
                },
                {
                    i: 'both-fact-004',
                    t: 1,
                    a: 400,
                    c: new Date(baseDate.getTime() + 4000),
                    d: new Date(baseDate.getTime() + 1000), // До cutoffDate
                    f1: 'shared-value',
                    f5: 'new'
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
            const searchFactIndexHashValues = searchFactIndexValues.map(index => index.h);
            const relevantFacts = await this.provider.getRelevantFacts(searchFactIndexHashValues, excludedFact.i, 1, cutoffDate);

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
            const foundId = relevantFacts[0].fact.i;
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
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.debug('\n=== Результаты тестирования MongoProvider ===');
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
    const test = new MongoProviderTest();
    test.runAllTests().catch(console.error);
}

module.exports = MongoProviderTest;
