const FactIndexer = require('../generators/factIndexer');

/**
 * Пример использования FactIndexer с полем dateName
 */

console.log('=== Пример использования FactIndexer с полем dateName ===\n');

// Создаем конфигурацию с разными полями дат
const config = [
    {
        fieldName: "f1",
        dateName: "transactionDate",
        indexTypeName: "transaction_id",
        indexType: 1,
        indexValue: 1
    },
    {
        fieldName: "f2",
        dateName: "dt",
        indexTypeName: "user_id",
        indexType: 2,
        indexValue: 2
    },
    {
        fieldName: "f3",
        dateName: "customTimestamp",
        indexTypeName: "order_id",
        indexType: 3,
        indexValue: 1
    }
];

// Создаем экземпляр FactIndexer
const indexer = new FactIndexer(config);

// Тестовый факт с разными полями дат
const fact = {
    t: 1,
    i: "fact_12345",
    d: new Date('2024-01-01'), // стандартное поле d
    c: new Date('2024-01-02'),
    
    // Поля для индексации
    f1: "transaction_abc123",
    f2: "user_xyz789",
    f3: "order_def456",
    
    // Разные поля дат
    transactionDate: new Date('2024-03-15T10:30:00Z'),
    dt: new Date('2024-04-20T14:45:00Z'),
    customTimestamp: new Date('2024-05-25T09:15:00Z')
};

console.log('Исходный факт:');
console.log(JSON.stringify(fact, null, 2));
console.log('\n');

// Создаем индексные значения
const indexValues = indexer.index(fact);

console.log('Созданные индексные значения:');
indexValues.forEach((indexValue, i) => {
    console.log(`${i + 1}. Поле: f${indexValue.it === 1 ? '1' : indexValue.it === 2 ? '2' : '3'}`);
    console.log(`   - Значение поля: ${indexValue.f}`);
    console.log(`   - Хеш: ${indexValue.h}`);
    console.log(`   - Дата: ${indexValue.d.toISOString()}`);
    console.log(`   - ID факта: ${indexValue.i}`);
    console.log(`   - Тип факта: ${indexValue.t}`);
    console.log(`   - Дата создания: ${indexValue.c.toISOString()}`);
    console.log('');
});

// Пример с невалидной датой
console.log('=== Пример с невалидной датой ===\n');

const factWithInvalidDate = {
    t: 1,
    i: "fact_67890",
    d: new Date('2024-01-01'),
    c: new Date('2024-01-02'),
    f1: "transaction_xyz789",
    transactionDate: "invalid_date_string" // невалидная дата
};

console.log('Факт с невалидной датой:');
console.log(JSON.stringify(factWithInvalidDate, null, 2));
console.log('\n');

const indexValuesInvalid = indexer.index(factWithInvalidDate);
console.log('Индексное значение (должна использоваться дата по умолчанию):');
const invalidIndex = indexValuesInvalid[0];
console.log(`- Дата: ${invalidIndex.d.toISOString()} (должна быть ${factWithInvalidDate.d.toISOString()})`);
console.log(`- Использована дата по умолчанию: ${invalidIndex.d.getTime() === factWithInvalidDate.d.getTime() ? 'Да' : 'Нет'}`);

console.log('\n=== Пример завершен ===');
