const FactGenerator = require('../generators/factGenerator');

/**
 * Тесты для FactGenerator
 * Использует конфигурацию полей в виде структуры (не файл)
 */

// Тестовая конфигурация полей
const testFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": [1, 2, 3] // user_action, system_event, payment
    },
    {
        "src": "f2",
        "dst": "f2",
        "types": [1, 3] // user_action, payment
    },
    {
        "src": "f3",
        "dst": "f3",
        "types": [2, 3] // system_event, payment
    },
    {
        "src": "f4",
        "dst": "f4",
        "types": [1] // user_action
    },
    {
        "src": "f5",
        "dst": "f5",
        "types": [2] // system_event
    }
];

// Расширенная конфигурация для тестов с большим количеством типов
const extendedFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": [1, 2, 3] // type1, type2, type3
    },
    {
        "src": "f2",
        "dst": "f2",
        "types": [1, 4] // type1, type4
    },
    {
        "src": "f3",
        "dst": "f3",
        "types": [2, 3, 4] // type2, type3, type4
    },
    {
        "src": "f4",
        "dst": "f4",
        "types": [1, 2] // type1, type2
    },
    {
        "src": "f5",
        "dst": "f5",
        "types": [3, 4] // type3, type4
    },
    {
        "src": "f6",
        "dst": "f6",
        "types": [1] // type1
    }
];

// Неверная конфигурация для тестов валидации
const invalidFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": "not_an_array" // Ошибка: должно быть массивом
    }
];

// Неполная конфигурация для тестов валидации
const incompleteFieldConfig = [
    {
        "src": "f1",
        // Отсутствует dst
        "types": [1]
    }
];

/**
 * Тест создания генератора с валидной конфигурацией
 */
function testValidConstructor(testName) {
    console.log(`=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(testFieldConfig);
        
        // Проверяем, что генератор создался успешно
        console.log('✅ Генератор создан успешно');
        
        // Проверяем доступные поля
        const expectedFields = ['f1', 'f2', 'f3', 'f4', 'f5'];
        const actualFields = generator._availableFields;
        console.log(`✅ Доступные поля: [${actualFields.join(', ')}]`);
        
        // Проверяем доступные типы
        const expectedTypes = [1, 2, 3]; // user_action, system_event, payment
        const actualTypes = generator._availableTypes;
        console.log(`✅ Доступные типы: [${actualTypes.join(', ')}]`);
        
        // Проверяем карту полей по типам
        console.log('✅ Карта полей по типам:');
        expectedTypes.forEach(type => {
            const fields = generator._typeFieldsMap[type];
            console.log(`   ${type}: [${fields.join(', ')}]`);
        });
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест создания генератора с неверной конфигурацией
 */
function testInvalidConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(invalidFieldConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации: ${error.message}`);
        return true;
    }
}

/**
 * Тест создания генератора с неполной конфигурацией
 */
function testIncompleteConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(incompleteFieldConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации: ${error.message}`);
        return true;
    }
}

/**
 * Тест создания генератора без конфигурации
 */
function testNullConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(null);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка: ${error.message}`);
        return true;
    }
}

/**
 * Тест генерации факта конкретного типа
 */
function testGenerateFact(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(testFieldConfig);
        
        // Генерируем факт типа user_action (тип 1)
        const fact = generator.generateFact(1);
        
        // Проверяем структуру факта
        console.log('✅ Факт сгенерирован успешно');
        console.log(`   ID: ${fact.i}`);
        console.log(`   Тип: ${fact.t}`);
        console.log(`   Количество: ${fact.a}`);
        console.log(`   Дата создания: ${fact.c.toISOString()}`);
        console.log(`   Дата факта: ${fact.d.toISOString()}`);
        
        // Проверяем поля факта
        const expectedFields = ['f1', 'f2', 'f4']; // Поля для user_action (тип 1)
        const actualFields = Object.keys(fact).filter(key => key.startsWith('f'));
        console.log(`   Поля: [${actualFields.join(', ')}]`);
        
        // Проверяем, что все ожидаемые поля присутствуют
        const hasAllFields = expectedFields.every(field => actualFields.includes(field));
        if (hasAllFields) {
            console.log('✅ Все ожидаемые поля присутствуют');
        } else {
            console.log('❌ Не все ожидаемые поля присутствуют');
            return false;
        }
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации факта несуществующего типа
 */
function testGenerateFactInvalidType(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(testFieldConfig);
        const fact = generator.generateFact(999); // Несуществующий тип
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка: ${error.message}`);
        return true;
    }
}

/**
 * Тест генерации случайного факта
 */
function testGenerateRandomTypeFact(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(testFieldConfig);
        
        // Генерируем несколько случайных фактов
        for (let i = 0; i < 5; i++) {
            const fact = generator.generateRandomTypeFact();
            console.log(`   Случайный факт ${i + 1}: тип=${fact.t}, поля=[${Object.keys(fact).filter(k => k.startsWith('f')).join(', ')}]`);
        }
        
        console.log('✅ Случайные факты генерируются успешно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации фактов с целевым размером
 */
function testGenerateFactWithTargetSize(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const targetSize = 500; // 500 байт
        const generator = new FactGenerator(testFieldConfig, new Date(), new Date(), targetSize);
        
        const fact = generator.generateFact(1); // user_action
        const actualSize = Buffer.byteLength(JSON.stringify(fact), 'utf8');
        
        console.log(`✅ Факт сгенерирован с целевым размером ${targetSize} байт`);
        console.log(`   Фактический размер: ${actualSize} байт`);
        console.log(`   Поле z: ${fact.z ? `длина ${fact.z.length}` : 'отсутствует'}`);
        
        // Проверяем, что размер близок к целевому (допуск ±50 байт)
        if (Math.abs(actualSize - targetSize) <= 50) {
            console.log('✅ Размер факта соответствует целевому');
        } else {
            console.log('⚠️ Размер факта не соответствует целевому');
        }
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации фактов с разными датами
 */
function testGenerateFactWithCustomDates(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const fromDate = new Date('2024-01-01');
        const toDate = new Date('2024-12-31');
        const generator = new FactGenerator(testFieldConfig, fromDate, toDate);
        
        const fact = generator.generateFact(1); // user_action
        
        console.log(`✅ Факт сгенерирован с пользовательскими датами`);
        console.log(`   Диапазон дат: ${fromDate.toISOString()} - ${toDate.toISOString()}`);
        console.log(`   Дата факта: ${fact.d.toISOString()}`);
        
        // Проверяем, что дата факта в заданном диапазоне
        if (fact.d >= fromDate && fact.d <= toDate) {
            console.log('✅ Дата факта находится в заданном диапазоне');
        } else {
            console.log('❌ Дата факта выходит за заданный диапазон');
            return false;
        }
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации фактов для всех типов
 */
function testGenerateFactForAllTypes(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(extendedFieldConfig);
        
        console.log('✅ Генерация фактов для всех типов:');
        generator._availableTypes.forEach(type => {
            const fact = generator.generateFact(type);
            const fields = Object.keys(fact).filter(key => key.startsWith('f'));
            console.log(`   Тип ${type}: ${fields.length} полей [${fields.join(', ')}]`);
        });
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест производительности генерации
 */
function testPerformance(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(testFieldConfig);
        const iterations = 1000;
        
        const startTime = Date.now();
        
        for (let i = 0; i < iterations; i++) {
            generator.generateRandomTypeFact();
        }
        
        const endTime = Date.now();
        const duration = endTime - startTime;
        const avgTime = duration / iterations;
        
        console.log(`✅ Сгенерировано ${iterations} фактов за ${duration}мс`);
        console.log(`   Среднее время генерации: ${avgTime.toFixed(3)}мс на факт`);
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Запуск всех тестов
 */
function runAllTests() {
    console.log('🧪 Запуск тестов FactGenerator\n');
    
    const tests = [
        { func: testValidConstructor, name: '1. Создание генератора с валидной конфигурацией' },
        { func: testInvalidConstructor, name: '2. Создание генератора с неверной конфигурацией' },
        { func: testIncompleteConstructor, name: '3. Создание генератора с неполной конфигурацией' },
        { func: testNullConstructor, name: '4. Создание генератора без конфигурации' },
        { func: testGenerateFact, name: '5. Генерация факта конкретного типа' },
        { func: testGenerateFactInvalidType, name: '6. Генерация факта несуществующего типа' },
        { func: testGenerateRandomTypeFact, name: '7. Генерация случайного факта' },
        { func: testGenerateFactWithTargetSize, name: '8. Генерация факта с целевым размером' },
        { func: testGenerateFactWithCustomDates, name: '9. Генерация факта с пользовательскими датами' },
        { func: testGenerateFactForAllTypes, name: '10. Генерация фактов для всех типов' },
        { func: testPerformance, name: '11. Производительность генерации' }
    ];
    
    let passed = 0;
    let failed = 0;
    
    tests.forEach(test => {
        try {
            if (test.func(test.name)) {
                passed++;
            } else {
                failed++;
            }
        } catch (error) {
            console.log(`❌ Неожиданная ошибка в тесте "${test.name}": ${error.message}`);
            failed++;
        }
    });
    
    console.log(`\n📊 Результаты тестирования:`);
    console.log(`   ✅ Пройдено: ${passed}`);
    console.log(`   ❌ Провалено: ${failed}`);
    console.log(`   📈 Успешность: ${((passed / (passed + failed)) * 100).toFixed(1)}%`);
    
    return failed === 0;
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const success = runAllTests();
    process.exit(success ? 0 : 1);
}

module.exports = {
    runAllTests,
    testValidConstructor,
    testInvalidConstructor,
    testIncompleteConstructor,
    testNullConstructor,
    testGenerateFact,
    testGenerateFactInvalidType,
    testGenerateRandomTypeFact,
    testGenerateFactWithTargetSize,
    testGenerateFactWithCustomDates,
    testGenerateFactForAllTypes,
    testPerformance
};
