const { MongoProvider, FactIndexer, FactGenerator } = require('../index');

/**
 * Пример использования MongoProvider для работы с индексными значениями
 */
async function mongoFactIndexExample() {
    console.log('=== Пример работы с индексными значениями в MongoDB ===\n');

    // Создаем экземпляры классов
    const provider = new MongoProvider(
        'mongodb://localhost:27017',
        'factTestDB'
    );
    const indexer = new FactIndexer();
    const generator = new FactGenerator();

    try {
        // 1. Подключение к MongoDB
        console.log('1. Подключение к MongoDB...');
        const connected = await provider.connect();
        if (!connected) {
            throw new Error('Не удалось подключиться к MongoDB');
        }
        console.log('   ✓ Подключение успешно\n');

        // 2. Генерация тестовых фактов
        console.log('2. Генерация тестовых фактов...');
        const fromDate = new Date('2024-01-01');
        const toDate = new Date('2024-12-31');
        const facts = generator.generateFacts(10, 1, fromDate, toDate);
        console.log(`   ✓ Сгенерировано ${facts.length} фактов\n`);

        // 3. Создание индексных значений
        console.log('3. Создание индексных значений...');
        const indexValues = indexer.indexFacts(facts);
        console.log(`   ✓ Создано ${indexValues.length} индексных значений\n`);

        // 4. Показываем примеры индексных значений
        console.log('4. Примеры индексных значений:');
        indexValues.slice(0, 3).forEach((indexValue, i) => {
            console.log(`   ${i + 1}.`, JSON.stringify(indexValue, null, 2));
        });
        console.log();

        // 5. Вставка индексных значений в MongoDB
        console.log('5. Вставка индексных значений в MongoDB...');
        const insertResult = await provider.saveFactIndexList(indexValues);
        console.log(`   ✓ Вставлено ${insertResult.insertedCount} индексных значений\n`);

        // 6. Создание индексов для индексных значений
        console.log('6. Создание индексов для индексных значений...');
        const indexSuccess = await provider.createFactIndexIndexes();
        if (indexSuccess) {
            console.log('   ✓ Индексы созданы успешно\n');
        } else {
            console.log('   ⚠ Некоторые индексы не удалось создать\n');
        }

        // 7. Получение статистики
        console.log('7. Статистика коллекции индексных значений:');
        const stats = await provider.getFactIndexStats();
        console.log();

        // 8. Получение схемы коллекции
        console.log('8. Схема коллекции индексных значений:');
        const schema = await provider.getFactIndexCollectionSchema();
        console.log();

        // 9. Очистка (опционально)
        console.log('9. Очистка коллекции индексных значений...');
        const clearResult = await provider.clearFactIndexCollection();
        console.log(`   ✓ Удалено ${clearResult.deletedCount} индексных значений\n`);

        console.log('=== Пример завершен успешно ===');

    } catch (error) {
        console.error('✗ Ошибка в примере:', error.message);
        console.error(error.stack);
    } finally {
        // Закрытие соединения
        await provider.disconnect();
    }
}

// Запуск примера, если файл выполняется напрямую
if (require.main === module) {
    mongoFactIndexExample().catch(console.error);
}

module.exports = mongoFactIndexExample;
