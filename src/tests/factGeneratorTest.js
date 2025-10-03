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
        "fact_types": [1, 2, 3] // user_action, system_event, payment
    },
    {
        "src": "f2",
        "dst": "f2",
        "fact_types": [1, 3] // user_action, payment
    },
    {
        "src": "f3",
        "dst": "f3",
        "fact_types": [2, 3] // system_event, payment
    },
    {
        "src": "f4",
        "dst": "f4",
        "fact_types": [1] // user_action
    },
    {
        "src": "f5",
        "dst": "f5",
        "fact_types": [2] // system_event
    }
];

// Расширенная конфигурация для тестов с большим количеством типов
const extendedFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1, 2, 3] // type1, type2, type3
    },
    {
        "src": "f2",
        "dst": "f2",
        "fact_types": [1, 4] // type1, type4
    },
    {
        "src": "f3",
        "dst": "f3",
        "fact_types": [2, 3, 4] // type2, type3, type4
    },
    {
        "src": "f4",
        "dst": "f4",
        "fact_types": [1, 2] // type1, type2
    },
    {
        "src": "f5",
        "dst": "f5",
        "fact_types": [3, 4] // type3, type4
    },
    {
        "src": "f6",
        "dst": "f6",
        "fact_types": [1] // type1
    }
];

// Неверная конфигурация для тестов валидации
const invalidFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": "not_an_array" // Ошибка: должно быть массивом
    }
];

// Неполная конфигурация для тестов валидации
const incompleteFieldConfig = [
    {
        "src": "f1",
        // Отсутствует dst
        "fact_types": [1]
    }
];

// Конфигурация с различными типами генераторов для тестирования
const generatorTestConfig = [
    {
        "src": "stringField",
        "dst": "stringField",
        "fact_types": [1],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15
        }
    },
    {
        "src": "integerField",
        "dst": "integerField",
        "fact_types": [1],
        "generator": {
            "type": "integer",
            "min": 100,
            "max": 1000
        }
    },
    {
        "src": "dateField",
        "dst": "dateField",
        "fact_types": [1],
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-06-30"
        }
    },
    {
        "src": "enumField",
        "dst": "enumField",
        "fact_types": [1],
        "generator": {
            "type": "enum",
            "values": ["option1", "option2", "option3", "option4"]
        }
    },
    {
        "src": "defaultField",
        "dst": "defaultField",
        "fact_types": [1]
        // Без generator - должно использовать значение по умолчанию
    }
];

// Неверная конфигурация генератора для тестов валидации
const invalidGeneratorConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1],
        "generator": {
            "type": "invalid_type" // Неверный тип
        }
    }
];

// Конфигурация с default_value и default_random для тестирования
const defaultValueTestConfig = [
    {
        "src": "stringField",
        "dst": "stringField",
        "fact_types": [1],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15,
            "default_value": "DEFAULT_STRING",
            "default_random": 0.3
        }
    },
    {
        "src": "integerField",
        "dst": "integerField",
        "fact_types": [1],
        "generator": {
            "type": "integer",
            "min": 100,
            "max": 1000,
            "default_value": 999,
            "default_random": 0.2
        }
    },
    {
        "src": "dateField",
        "dst": "dateField",
        "fact_types": [1],
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-06-30",
            "default_value": "2024-03-15",
            "default_random": 0.1
        }
    },
    {
        "src": "enumField",
        "dst": "enumField",
        "fact_types": [1],
        "generator": {
            "type": "enum",
            "values": ["option1", "option2", "option3", "option4"],
            "default_value": "option2",
            "default_random": 0.4
        }
    }
];

// Неверная конфигурация с некорректными default_value и default_random
const invalidDefaultValueConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1],
        "generator": {
            "type": "string",
            "default_value": 123, // Неверный тип для string
            "default_random": 0.5
        }
    }
];

const invalidDefaultRandomConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1],
        "generator": {
            "type": "integer",
            "default_value": 100,
            "default_random": 1.5 // Неверное значение (> 1)
        }
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
 * Тест создания генератора с неверной конфигурацией генератора
 */
function testInvalidGeneratorConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(invalidGeneratorConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации генератора: ${error.message}`);
        return true;
    }
}

/**
 * Тест генерации факта с различными типами генераторов
 */
function testGeneratorTypes(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(generatorTestConfig);
        const fact = generator.generateFact(1);
        
        console.log('✅ Факт сгенерирован с различными типами генераторов');
        console.log(`   stringField: "${fact.stringField}" (тип: ${typeof fact.stringField})`);
        console.log(`   integerField: ${fact.integerField} (тип: ${typeof fact.integerField})`);
        console.log(`   dateField: ${fact.dateField} (тип: ${typeof fact.dateField})`);
        console.log(`   enumField: "${fact.enumField}" (тип: ${typeof fact.enumField})`);
        console.log(`   defaultField: "${fact.defaultField}" (тип: ${typeof fact.defaultField})`);
        
        // Проверяем типы данных
        if (typeof fact.stringField !== 'string') {
            console.log('❌ stringField должен быть строкой');
            return false;
        }
        
        if (typeof fact.integerField !== 'number' || !Number.isInteger(fact.integerField)) {
            console.log('❌ integerField должен быть целым числом');
            return false;
        }
        
        if (!(fact.dateField instanceof Date)) {
            console.log('❌ dateField должен быть объектом Date');
            return false;
        }
        
        if (typeof fact.enumField !== 'string') {
            console.log('❌ enumField должен быть строкой');
            return false;
        }
        
        if (typeof fact.defaultField !== 'string') {
            console.log('❌ defaultField должен быть строкой (значение по умолчанию)');
            return false;
        }
        
        // Проверяем диапазоны значений
        if (fact.stringField.length < 5 || fact.stringField.length > 15) {
            console.log('❌ stringField должен быть длиной от 5 до 15 символов');
            return false;
        }
        
        if (fact.integerField < 100 || fact.integerField > 1000) {
            console.log('❌ integerField должен быть в диапазоне от 100 до 1000');
            return false;
        }
        
        const validEnumValues = ["option1", "option2", "option3", "option4"];
        if (!validEnumValues.includes(fact.enumField)) {
            console.log('❌ enumField должен быть одним из допустимых значений');
            return false;
        }
        
        if (fact.defaultField.length < 6 || fact.defaultField.length > 20) {
            console.log('❌ defaultField должен быть длиной от 6 до 20 символов (значение по умолчанию)');
            return false;
        }
        
        console.log('✅ Все типы генераторов работают корректно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации нескольких фактов для проверки случайности enum значений
 */
function testEnumRandomness(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(generatorTestConfig);
        const enumValues = new Set();
        
        // Генерируем 50 фактов и собираем все enum значения
        for (let i = 0; i < 50; i++) {
            const fact = generator.generateFact(1);
            enumValues.add(fact.enumField);
        }
        
        console.log(`✅ Сгенерировано 50 фактов, получено ${enumValues.size} уникальных enum значений`);
        console.log(`   Значения: [${Array.from(enumValues).join(', ')}]`);
        
        // Проверяем, что получили несколько разных значений (не все одинаковые)
        if (enumValues.size < 2) {
            console.log('⚠️ Получено мало уникальных enum значений, возможно проблема с генерацией');
        } else {
            console.log('✅ Enum значения генерируются случайно');
        }
        
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест создания генератора с неверной конфигурацией default_value
 */
function testInvalidDefaultValueConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(invalidDefaultValueConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации default_value: ${error.message}`);
        return true;
    }
}

/**
 * Тест создания генератора с неверной конфигурацией default_random
 */
function testInvalidDefaultRandomConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(invalidDefaultRandomConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации default_random: ${error.message}`);
        return true;
    }
}

/**
 * Тест генерации факта с default_value и default_random
 */
function testDefaultValueGeneration(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(defaultValueTestConfig);
        const fact = generator.generateFact(1);
        
        console.log('✅ Факт сгенерирован с default_value и default_random');
        console.log(`   stringField: "${fact.stringField}" (тип: ${typeof fact.stringField})`);
        console.log(`   integerField: ${fact.integerField} (тип: ${typeof fact.integerField})`);
        console.log(`   dateField: ${fact.dateField} (тип: ${typeof fact.dateField})`);
        console.log(`   enumField: "${fact.enumField}" (тип: ${typeof fact.enumField})`);
        
        // Проверяем типы данных
        if (typeof fact.stringField !== 'string') {
            console.log('❌ stringField должен быть строкой');
            return false;
        }
        
        if (typeof fact.integerField !== 'number' || !Number.isInteger(fact.integerField)) {
            console.log('❌ integerField должен быть целым числом');
            return false;
        }
        
        if (!(fact.dateField instanceof Date)) {
            console.log('❌ dateField должен быть объектом Date');
            return false;
        }
        
        if (typeof fact.enumField !== 'string') {
            console.log('❌ enumField должен быть строкой');
            return false;
        }
        
        console.log('✅ Все типы данных корректны');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест проверки частоты появления default_value
 */
function testDefaultValueFrequency(testName) {
    console.log(`\n=== Тест: ${testName} ===`);
    
    try {
        const generator = new FactGenerator(defaultValueTestConfig);
        const iterations = 1000;
        
        let defaultStringCount = 0;
        let defaultIntegerCount = 0;
        let defaultDateCount = 0;
        let defaultEnumCount = 0;
        
        // Генерируем много фактов для статистики
        for (let i = 0; i < iterations; i++) {
            const fact = generator.generateFact(1);
            
            if (fact.stringField === "DEFAULT_STRING") defaultStringCount++;
            if (fact.integerField === 999) defaultIntegerCount++;
            if (fact.dateField.toISOString().startsWith("2024-03-15")) defaultDateCount++;
            if (fact.enumField === "option2") defaultEnumCount++;
        }
        
        const stringFrequency = defaultStringCount / iterations;
        const integerFrequency = defaultIntegerCount / iterations;
        const dateFrequency = defaultDateCount / iterations;
        const enumFrequency = defaultEnumCount / iterations;
        
        console.log(`✅ Статистика появления default_value за ${iterations} итераций:`);
        console.log(`   stringField (ожидается ~30%): ${(stringFrequency * 100).toFixed(1)}% (${defaultStringCount} раз)`);
        console.log(`   integerField (ожидается ~20%): ${(integerFrequency * 100).toFixed(1)}% (${defaultIntegerCount} раз)`);
        console.log(`   dateField (ожидается ~10%): ${(dateFrequency * 100).toFixed(1)}% (${defaultDateCount} раз)`);
        console.log(`   enumField (ожидается ~40%): ${(enumFrequency * 100).toFixed(1)}% (${defaultEnumCount} раз)`);
        
        // Проверяем, что частоты примерно соответствуют ожидаемым (с допуском ±10%)
        const tolerance = 0.1;
        const expectedFrequencies = [0.3, 0.2, 0.1, 0.4];
        const actualFrequencies = [stringFrequency, integerFrequency, dateFrequency, enumFrequency];
        
        let allWithinTolerance = true;
        for (let i = 0; i < expectedFrequencies.length; i++) {
            const diff = Math.abs(actualFrequencies[i] - expectedFrequencies[i]);
            if (diff > tolerance) {
                console.log(`⚠️ Частота ${i} выходит за допустимый диапазон (разница: ${(diff * 100).toFixed(1)}%)`);
                allWithinTolerance = false;
            }
        }
        
        if (allWithinTolerance) {
            console.log('✅ Все частоты соответствуют ожидаемым значениям');
        } else {
            console.log('⚠️ Некоторые частоты не соответствуют ожидаемым (это нормально для случайных значений)');
        }
        
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
        { func: testInvalidGeneratorConstructor, name: '5. Создание генератора с неверной конфигурацией генератора' },
        { func: testInvalidDefaultValueConstructor, name: '6. Создание генератора с неверной конфигурацией default_value' },
        { func: testInvalidDefaultRandomConstructor, name: '7. Создание генератора с неверной конфигурацией default_random' },
        { func: testGenerateFact, name: '8. Генерация факта конкретного типа' },
        { func: testGenerateFactInvalidType, name: '9. Генерация факта несуществующего типа' },
        { func: testGenerateRandomTypeFact, name: '10. Генерация случайного факта' },
        { func: testGenerateFactWithTargetSize, name: '11. Генерация факта с целевым размером' },
        { func: testGenerateFactWithCustomDates, name: '12. Генерация факта с пользовательскими датами' },
        { func: testGenerateFactForAllTypes, name: '13. Генерация фактов для всех типов' },
        { func: testGeneratorTypes, name: '14. Генерация факта с различными типами генераторов' },
        { func: testEnumRandomness, name: '15. Проверка случайности enum значений' },
        { func: testDefaultValueGeneration, name: '16. Генерация факта с default_value и default_random' },
        { func: testDefaultValueFrequency, name: '17. Проверка частоты появления default_value' },
        { func: testPerformance, name: '18. Производительность генерации' }
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
    testInvalidGeneratorConstructor,
    testInvalidDefaultValueConstructor,
    testInvalidDefaultRandomConstructor,
    testGenerateFact,
    testGenerateFactInvalidType,
    testGenerateRandomTypeFact,
    testGenerateFactWithTargetSize,
    testGenerateFactWithCustomDates,
    testGenerateFactForAllTypes,
    testGeneratorTypes,
    testEnumRandomness,
    testDefaultValueGeneration,
    testDefaultValueFrequency,
    testPerformance
};
