const MongoProvider = require('../db-providers/mongoProvider');

/**
 * Пример использования класса MongoProvider
 */
async function demonstrateMongoProvider() {
    // Создаем экземпляр провайдера
    const mongoProvider = new MongoProvider(
        'mongodb://localhost:27017', 
        'testdb'
    );

    try {
        // 1. Подключаемся к MongoDB
        console.log('=== 1. Подключение к MongoDB ===');
        const connected = await mongoProvider.connect();
        if (!connected) {
            console.error('Не удалось подключиться к MongoDB');
            return;
        }

        // 2. Создаем схему валидации для коллекции facts
        console.log('\n=== 2. Создание схемы валидации ===');
        await mongoProvider.createFactsCollectionSchema(23); // Поддержка до 23 динамических полей

        // 3. Получаем информацию о схеме
        console.log('\n=== 3. Информация о схеме ===');
        const schema = await mongoProvider.getFactsCollectionSchema();
        if (schema) {
            console.log('Схема найдена:', Object.keys(schema.$jsonSchema.properties).length, 'полей');
        }

        // 4. Получаем статистику коллекции
        console.log('\n=== 4. Статистика коллекции ===');
        await mongoProvider.getFactsCollectionStats();

        console.log('\n✓ Демонстрация завершена успешно');

    } catch (error) {
        console.error('✗ Ошибка в демонстрации:', error.message);
    } finally {
        // Закрываем соединение
        await mongoProvider.disconnect();
    }
}

// Запуск демонстрации только если файл запущен напрямую
if (require.main === module) {
    demonstrateMongoProvider().catch(console.error);
}

module.exports = { demonstrateMongoProvider };
