const { FactIndexer, FactGenerator } = require('../index');

/**
 * Пример использования FactIndexer (создание индексных значений)
 */
async function factIndexerExample() {
    console.log('=== Пример использования FactIndexer ===\n');

    // Создаем экземпляры классов
    const indexer = new FactIndexer();
    const generator = new FactGenerator();

    // Генерируем тестовые факты
    console.log('1. Генерация тестовых фактов...');
    const fromDate = new Date('2024-01-01');
    const toDate = new Date('2024-12-31');
    
    const facts = [];
    for (let i = 0; i < 3; i++) {
        facts.push(generator.generateRandomTypeFact());
    }
    console.log(`   Сгенерировано ${facts.length} фактов\n`);

    // Показываем структуру одного факта
    console.log('2. Структура факта:');
    console.log(JSON.stringify(facts[0], null, 2));
    console.log();

    // Создаем индексные значения для всех фактов
    console.log('3. Создание индексных значений...');
    const allIndexValues = indexer.indexFacts(facts);
    console.log(`   Создано ${allIndexValues.length} индексных значений\n`);

    // Показываем примеры индексных значений
    console.log('4. Примеры индексных значений:');
    allIndexValues.slice(0, 5).forEach((indexValue, i) => {
        console.log(`   Индексное значение ${i + 1}:`, JSON.stringify(indexValue, null, 2));
    });
    console.log();

    // Показываем базовую информацию об индексных значениях
    console.log('5. Информация об индексных значениях:');
    console.log(`   Общее количество индексных значений: ${allIndexValues.length}`);
    
    // Подсчитываем количество индексных значений
    console.log(`   Общее количество индексных значений: ${allIndexValues.length}`);
    console.log();

    // Демонстрация работы с отдельным фактом
    console.log('6. Создание индексных значений для отдельного факта:');
    const singleFact = {
        i: 'custom-id-123',
        t: 2,
        a: 500,
        c: new Date('2024-06-01'),
        d: new Date('2024-06-15'),
        f1: 'custom_value_1',
        f5: 'custom_value_5',
        f10: 'custom_value_10',
        otherField: 'ignored'
    };

    const singleIndexValues = indexer.index(singleFact);
    console.log(`   Факт с ID "${singleFact.i}" создал ${singleIndexValues.length} индексных значений:`);
    singleIndexValues.forEach((indexValue, i) => {
        console.log(`     ${i + 1}. Хеш: ${indexValue.h}, ID факта: ${indexValue.i}`);
    });
    console.log();

    // Демонстрация обработки факта без полей fN
    console.log('7. Факт без полей fN:');
    const factWithoutFields = {
        i: 'no-fields-id',
        t: 3,
        a: 1000,
        c: new Date('2024-07-01'),
        d: new Date('2024-07-15'),
        otherField: 'some value',
        anotherField: 'another value'
    };

    const noFieldsIndexValues = indexer.index(factWithoutFields);
    console.log(`   Факт без полей fN создал ${noFieldsIndexValues.length} индексных значений:`);
    if (noFieldsIndexValues.length > 0) {
        console.log('   ', JSON.stringify(noFieldsIndexValues[0], null, 2));
    } else {
        console.log('   (индексные значения не созданы, так как нет полей fN)');
    }
    console.log();

    console.log('=== Пример завершен ===');
}

// Запуск примера, если файл выполняется напрямую
if (require.main === module) {
    factIndexerExample().catch(console.error);
}

module.exports = factIndexerExample;
