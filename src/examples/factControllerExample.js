const FactController = require('../controllers/factController');
const MongoProvider = require('../db-providers/mongoProvider');

/**
 * Пример использования FactController для создания фактов с индексными значениями
 */
async function factControllerExample() {
    // Параметры подключения к MongoDB
    const connectionString = 'mongodb://localhost:27017';
    const databaseName = 'CounterTest';

    // Создаем провайдер данных
    const mongoProvider = new MongoProvider(connectionString, databaseName);
    
    // Создаем экземпляр контроллера с dbProvider
    const factController = new FactController(mongoProvider);

    try {
        // Подключаемся к MongoDB
        console.log('Подключение к MongoDB...');
        const connected = await mongoProvider.connect();
        if (!connected) {
            console.error('Не удалось подключиться к MongoDB');
            return;
        }
        console.log('✓ Подключено\n');

        console.log('=== Использование FactController ===');

        // Пример 1: Создание одного факта с индексными значениями
        console.log('\n=== Пример 1: Создание одного факта ===');
        const fact1 = {
            i: '550e8400-e29b-41d4-a716-446655440001', // GUID
            t: 1, // тип факта
            c: new Date(), // дата создания
            f1: 'значение1',
            f2: 'значение2',
            f5: 'значение5',
            z: 'дополнительное поле для размера'
        };

        const result1 = await factController.saveFact(fact1);
        console.log('Результат создания факта:', result1);

        // Пример 2: Создание факта без полей fN (не будет индексных значений)
        console.log('\n=== Пример 2: Создание факта без индексных полей ===');
        const fact2 = {
            i: '550e8400-e29b-41d4-a716-446655440002',
            t: 2,
            c: new Date(),
            z: 'факт без полей f1, f2, f3...'
        };

        const result2 = await factController.saveFact(fact2);
        console.log('Результат создания факта без индексов:', result2);

        // Пример 3: Создание нескольких фактов
        console.log('\n=== Пример 3: Создание нескольких фактов ===');
        const facts = [
            {
                i: '550e8400-e29b-41d4-a716-446655440003',
                t: 3,
                c: new Date(),
                f1: 'множественное1',
                f3: 'множественное3'
            },
            {
                i: '550e8400-e29b-41d4-a716-446655440004',
                t: 4,
                c: new Date(),
                f2: 'множественное2',
                f4: 'множественное4',
                f6: 'множественное6'
            }
        ];

        const result3 = await factController.saveFacts(facts);
        console.log('Результат создания множественных фактов:', result3);


        // Пример 4: Попытка создать дубликат (должен обновиться)
        console.log('\n=== Пример 4: Создание дубликата ===');
        const duplicateFact = {
            i: '550e8400-e29b-41d4-a716-446655440001', // тот же ID
            t: 1,
            c: new Date(),
            f1: 'обновленное значение1',
            f2: 'обновленное значение2',
            f5: 'обновленное значение5'
        };

        const result4 = await factController.saveFact(duplicateFact);
        console.log('Результат создания дубликата:', result4);

    } finally {
        // Отключаемся от MongoDB
        console.log('\nОтключение от MongoDB...');
        await mongoProvider.disconnect();
        console.log('✓ Отключено');
    }
}

// Запуск примера, если файл выполняется напрямую
if (require.main === module) {
    factControllerExample()
        .then(() => {
            console.log('\n✓ Пример завершен');
            process.exit(0);
        })
        .catch((error) => {
            console.error('✗ Ошибка выполнения примера:', error.message);
            process.exit(1);
        });
}

module.exports = factControllerExample;
