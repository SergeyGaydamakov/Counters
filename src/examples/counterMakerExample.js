const { MongoCounters } = require('../index');

/**
 * Пример использования класса MongoCounters
 */
function exampleUsage() {
    console.log('=== Пример использования MongoCounters ===\n');

    // Пример 1: Инициализация с массивом конфигурации
    console.log('1. Инициализация с массивом конфигурации:');
    const config = [
        {
            name: 'payment_counter',
            comment: 'Счетчик платежей',
            condition: { 
                messageTypeId: [50, 70],
                status: 'A'
            },
            aggregate: [
                { $match: { status: 'A' } },
                { $group: { _id: null, count: { $sum: 1 }, total: { $sum: '$amount' } } }
            ]
        },
        {
            name: 'rejected_counter',
            comment: 'Счетчик отклоненных транзакций',
            condition: { 
                messageTypeId: [50, 70],
                status: 'R'
            },
            aggregate: [
                { $match: { status: 'R' } },
                { $count: 'rejected_count' }
            ]
        }
    ];

    const counterMaker1 = new MongoCounters(config);
    console.log(`   Создано счетчиков: ${counterMaker1.getCounterCount()}`);
    console.log(`   Имена счетчиков: ${counterMaker1.getCounterNames().join(', ')}\n`);

    // Пример 2: Инициализация с файлом конфигурации
    console.log('2. Инициализация с файлом конфигурации:');
    const counterMaker2 = new MongoCounters('./countersConfig.json');
    console.log(`   Создано счетчиков: ${counterMaker2.getCounterCount()}`);
    console.log(`   Имена счетчиков: ${counterMaker2.getCounterNames().join(', ')}\n`);

    // Пример 3: Создание счетчиков для факта
    console.log('3. Создание счетчиков для факта:');
    const fact = {
        _id: 'test_fact_123',
        t: 50,
        c: new Date(),
        d: {
            messageTypeId: 50,
            status: 'A',
            amount: 1000,
            mcc: '5411',
            country: '643'
        }
    };

    const counters = counterMaker1.make(fact);
    console.log(`   Факт: ${JSON.stringify(fact.d, null, 2)}`);
    console.log(`   Примененные счетчики: ${Object.keys(counters).join(', ')}`);
    
    if (Object.keys(counters).length > 0) {
        console.log('   Структура для $facet:');
        console.log(JSON.stringify(counters, null, 2));
    }
    console.log();

    // Пример 4: Факт, который не подходит под условия
    console.log('4. Факт, который не подходит под условия:');
    const unsuitableFact = {
        _id: 'unsuitable_fact',
        t: 60,
        c: new Date(),
        d: {
            messageTypeId: 60,
            status: 'A',
            amount: 500
        }
    };

    const unsuitableCounters = counterMaker1.make(unsuitableFact);
    console.log(`   Факт: ${JSON.stringify(unsuitableFact.d, null, 2)}`);
    console.log(`   Примененные счетчики: ${Object.keys(unsuitableCounters).length === 0 ? 'нет' : Object.keys(unsuitableCounters).join(', ')}`);
    console.log();

    // Пример 5: Получение конфигурации конкретного счетчика
    console.log('5. Получение конфигурации счетчика:');
    const paymentConfig = counterMaker1.getCounterConfig('payment_counter');
    if (paymentConfig) {
        console.log(`   Конфигурация счетчика 'payment_counter':`);
        console.log(`   - Комментарий: ${paymentConfig.comment}`);
        console.log(`   - Условие: ${JSON.stringify(paymentConfig.condition, null, 2)}`);
        console.log(`   - Количество этапов aggregate: ${paymentConfig.aggregate.length}`);
    }
}

// Запуск примера, если файл выполняется напрямую
if (require.main === module) {
    try {
        exampleUsage();
    } catch (error) {
        console.error('Ошибка при выполнении примера:', error.message);
        process.exit(1);
    }
}

module.exports = { exampleUsage };
