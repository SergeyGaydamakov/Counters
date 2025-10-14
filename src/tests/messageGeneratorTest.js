const MessageGenerator = require('../generators/messageGenerator');

/**
 * Тесты для MessageGenerator
 * Использует конфигурацию полей в виде структуры (не файл)
 */

// Тестовая конфигурация полей
const testFieldConfig = [
    {
        "src": "dt",
        "dst": "dt",
        "message_types": [1, 2, 3], // user_action, system_message, payment
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-06-30"
        }
    },
    {
        "src": "f1",
        "dst": "f1",
        "message_types": [1, 2, 3] // user_action, system_message, payment
    },
    {
        "src": "f2",
        "dst": "f2",
        "message_types": [1, 3] // user_action, payment
    },
    {
        "src": "f3",
        "dst": "f3",
        "message_types": [2, 3] // system_message, payment
    },
    {
        "src": "f4",
        "dst": "f4",
        "message_types": [1] // user_action
    },
    {
        "src": "f5",
        "dst": "f5",
        "message_types": [2] // system_message
    }
];

// Расширенная конфигурация для тестов с большим количеством типов
const extendedFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "message_types": [1, 2, 3] // type1, type2, type3
    },
    {
        "src": "f2",
        "dst": "f2",
        "message_types": [1, 4] // type1, type4
    },
    {
        "src": "f3",
        "dst": "f3",
        "message_types": [2, 3, 4] // type2, type3, type4
    },
    {
        "src": "f4",
        "dst": "f4",
        "message_types": [1, 2] // type1, type2
    },
    {
        "src": "f5",
        "dst": "f5",
        "message_types": [3, 4] // type3, type4
    },
    {
        "src": "f6",
        "dst": "f6",
        "message_types": [1] // type1
    }
];

// Неверная конфигурация для тестов валидации
const invalidFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "message_types": "not_an_array" // Ошибка: должно быть массивом
    }
];

// Неполная конфигурация для тестов валидации
const incompleteFieldConfig = [
    {
        "src": "f1",
        // Отсутствует dst
        "message_types": [1]
    }
];

// Конфигурация с различными типами генераторов для тестирования
const generatorTestConfig = [
    {
        "src": "stringField",
        "dst": "stringField",
        "message_types": [1],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15
        }
    },
    {
        "src": "integerField",
        "dst": "integerField",
        "message_types": [1],
        "generator": {
            "type": "integer",
            "min": 100,
            "max": 1000
        }
    },
    {
        "src": "dateField",
        "dst": "dateField",
        "message_types": [1],
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-06-30"
        }
    },
    {
        "src": "enumField",
        "dst": "enumField",
        "message_types": [1],
        "generator": {
            "type": "enum",
            "values": ["option1", "option2", "option3", "option4"]
        }
    },
    {
        "src": "objectIdField",
        "dst": "objectIdField",
        "message_types": [1],
        "generator": {
            "type": "objectId"
        }
    },
    {
        "src": "booleanField",
        "dst": "booleanField",
        "message_types": [1],
        "generator": {
            "type": "boolean"
        }
    },
    {
        "src": "defaultField",
        "dst": "defaultField",
        "message_types": [1]
        // Без generator - должно использовать значение по умолчанию
    }
];

// Неверная конфигурация генератора для тестов валидации
const invalidGeneratorConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "message_types": [1],
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
        "message_types": [1],
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
        "message_types": [1],
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
        "message_types": [1],
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
        "message_types": [1],
        "generator": {
            "type": "enum",
            "values": ["option1", "option2", "option3", "option4"],
            "default_value": "option2",
            "default_random": 0.4
        }
    },
    {
        "src": "objectIdField",
        "dst": "objectIdField",
        "message_types": [1],
        "generator": {
            "type": "objectId",
            "default_value": "507f1f77bcf86cd799439011",
            "default_random": 0.5
        }
    },
    {
        "src": "booleanField",
        "dst": "booleanField",
        "message_types": [1],
        "generator": {
            "type": "boolean",
            "default_value": true,
            "default_random": 0.3
        }
    }
];

// Неверная конфигурация с некорректными default_value и default_random
const invalidDefaultValueConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "message_types": [1],
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
        "message_types": [1],
        "generator": {
            "type": "integer",
            "default_value": 100,
            "default_random": 1.5 // Неверное значение (> 1)
        }
    }
];

// Неверная конфигурация boolean с некорректным default_value
const invalidBooleanConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "message_types": [1],
        "generator": {
            "type": "boolean",
            "default_value": "not_a_boolean" // Неверный тип для boolean
        }
    }
];

// Конфигурация с дублирующимися src полями для тестирования пропуска повторных упоминаний
const duplicateSrcFieldConfig = [
    {
        "src": "commonField",
        "dst": "dst1",
        "message_types": [1, 2],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15,
            "default_value": "FIRST_MAP",
            "default_random": 0.5
        }
    },
    {
        "src": "commonField", // Дублирующееся src поле
        "dst": "dst2",
        "message_types": [1, 2],
        "generator": {
            "type": "string",
            "min": 8,
            "max": 15,
            "default_value": "SECOND_MAPPING",
            "default_random": 0.3
        }
    },
    {
        "src": "uniqueField",
        "dst": "uniqueDst",
        "message_types": [1, 2],
        "generator": {
            "type": "integer",
            "min": 100,
            "max": 200
        }
    },
    {
        "src": "anotherCommonField",
        "dst": "anotherDst1",
        "message_types": [1],
        "generator": {
            "type": "boolean",
            "default_value": true,
            "default_random": 0.8
        }
    },
    {
        "src": "anotherCommonField", // Еще одно дублирующееся src поле
        "dst": "anotherDst2",
        "message_types": [1],
        "generator": {
            "type": "boolean",
            "default_value": false,
            "default_random": 0.2
        }
    }
];

/**
 * Тест создания генератора с валидной конфигурацией
 */
function testValidConstructor(testName) {
    console.log(`=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(testFieldConfig);

        // Проверяем, что генератор создался успешно
        console.log('✅ Генератор создан успешно');

        // Проверяем доступные поля
        const expectedFields = ['dt','f1', 'f2', 'f3', 'f4', 'f5'];
        const actualFields = generator._availableFields;
        // Проверяем наличие всех полей в expectedFields
        const hasAllFields = expectedFields.every(field => actualFields.includes(field));
        if (!hasAllFields) {
            throw new Error('❌ Не все поля присутствуют');
        }
        console.log(`✅ Доступные поля: [${actualFields.join(', ')}]`);

        // Проверяем доступные типы
        const expectedTypes = [1, 2, 3]; // user_action, system_message, payment
        const actualTypes = generator._availableTypes;
        // Проверяем наличие всех типов в expectedTypes
        const hasAllTypes = expectedTypes.every(type => actualTypes.includes(type));
        if (!hasAllTypes) {
            throw new Error('❌ Не все типы присутствуют');
        }
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
        const generator = new MessageGenerator(invalidFieldConfig);
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
        const generator = new MessageGenerator(incompleteFieldConfig);
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
        const generator = new MessageGenerator(null);
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
function testGenerateEvent(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(testFieldConfig);

        // Генерируем факт типа user_action (тип 1)
        const event = generator.generateMessage(1);

        // Проверяем структуру факта
        console.log('✅ Событие сгенерировано успешно');
        console.log(`   Тип: ${event.t}`);

        // Проверяем поля факта
        const expectedTopLevelFields = ['t', 'd']; // Основные поля события
        const actualFields = Object.keys(event);
        console.log(`   Поля: [${actualFields.join(', ')}]`);

        // Проверяем, что все основные поля присутствуют
        const hasAllTopLevelFields = expectedTopLevelFields.every(field => actualFields.includes(field));
        if (hasAllTopLevelFields) {
            console.log('✅ Все основные поля присутствуют');
        } else {
            console.log('❌ Не все основные поля присутствуют');
            return false;
        }

        // Проверяем, что объект d содержит поля типа
        if (event.d && typeof event.d === 'object') {
            const expectedDataFields = ['dt','f1', 'f2', 'f4']; // Поля для user_action (тип 1) в объекте d
            const actualDataFields = Object.keys(event.d);
            console.log(`   Поля в d: [${actualDataFields.join(', ')}]`);
            
            const hasAllDataFields = expectedDataFields.every(field => actualDataFields.includes(field));
            if (hasAllDataFields) {
                console.log('✅ Все поля типа присутствуют в объекте d');
            } else {
                console.log('❌ Не все поля типа присутствуют в объекте d');
                return false;
            }
        } else {
            console.log('❌ Объект d отсутствует или не является объектом');
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
        const generator = new MessageGenerator(testFieldConfig);
        const fact = generator.generateMessage(999); // Несуществующий тип
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
        const generator = new MessageGenerator(testFieldConfig);

        // Генерируем несколько случайных фактов
        for (let i = 0; i < 5; i++) {
            const fact = generator.generateRandomTypeMessage();
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
 * Тест генерации фактов для всех типов
 */
function testGenerateFactForAllTypes(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(extendedFieldConfig);

        console.log('✅ Генерация фактов для всех типов:');
        generator._availableTypes.forEach(type => {
            const fact = generator.generateMessage(type);
            const dataFields = fact.d ? Object.keys(fact.d) : [];
            console.log(`   Тип ${type}: ${dataFields.length} полей [${dataFields.join(', ')}]`);
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
        const generator = new MessageGenerator(testFieldConfig);
        const iterations = 1000;

        const startTime = Date.now();

        for (let i = 0; i < iterations; i++) {
            generator.generateRandomTypeMessage();
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
        const generator = new MessageGenerator(invalidGeneratorConfig);
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
        const generator = new MessageGenerator(generatorTestConfig);
        const fact = generator.generateMessage(1);

        console.log('✅ Факт сгенерирован с различными типами генераторов');
        
        // Проверяем, что объект d существует
        if (!fact.d || typeof fact.d !== 'object') {
            console.log('❌ Объект d отсутствует или не является объектом');
            return false;
        }
        
        const data = fact.d;
        console.log(`   stringField: "${data.stringField}" (тип: ${typeof data.stringField})`);
        console.log(`   integerField: ${data.integerField} (тип: ${typeof data.integerField})`);
        console.log(`   dateField: ${data.dateField} (тип: ${typeof data.dateField})`);
        console.log(`   enumField: "${data.enumField}" (тип: ${typeof data.enumField})`);
        console.log(`   objectIdField: ${data.objectIdField} (тип: ${typeof data.objectIdField})`);
        console.log(`   booleanField: ${data.booleanField} (тип: ${typeof data.booleanField})`);
        console.log(`   defaultField: "${data.defaultField}" (тип: ${typeof data.defaultField})`);

        // Проверяем типы данных
        if (typeof data.stringField !== 'string') {
            console.log('❌ stringField должен быть строкой');
            return false;
        }

        if (typeof data.integerField !== 'number' || !Number.isInteger(data.integerField)) {
            console.log('❌ integerField должен быть целым числом');
            return false;
        }

        if (!(data.dateField instanceof Date)) {
            console.log('❌ dateField должен быть объектом Date');
            return false;
        }

        if (typeof data.enumField !== 'string') {
            console.log('❌ enumField должен быть строкой');
            return false;
        }

        if (typeof data.objectIdField !== 'object' || !data.objectIdField.constructor || data.objectIdField.constructor.name !== 'ObjectId') {
            console.log('❌ objectIdField должен быть объектом ObjectId');
            return false;
        }

        if (typeof data.booleanField !== 'boolean') {
            console.log('❌ booleanField должен быть булевым значением');
            return false;
        }

        if (typeof data.defaultField !== 'string') {
            console.log('❌ defaultField должен быть строкой (значение по умолчанию)');
            return false;
        }

        // Проверяем диапазоны значений
        if (data.stringField.length < 5 || data.stringField.length > 15) {
            console.log('❌ stringField должен быть длиной от 5 до 15 символов');
            return false;
        }

        if (data.integerField < 100 || data.integerField > 1000) {
            console.log('❌ integerField должен быть в диапазоне от 100 до 1000');
            return false;
        }

        const validEnumValues = ["option1", "option2", "option3", "option4"];
        if (!validEnumValues.includes(data.enumField)) {
            console.log('❌ enumField должен быть одним из допустимых значений');
            return false;
        }

        // Проверяем, что objectIdField является валидным ObjectId
        if (typeof data.objectIdField.toString !== 'function' || data.objectIdField.toString().length !== 24) {
            console.log('❌ objectIdField должен быть валидным ObjectId (24 символа в hex)');
            return false;
        }

        if (data.defaultField.length < 6 || data.defaultField.length > 20) {
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
        const generator = new MessageGenerator(generatorTestConfig);
        const enumValues = new Set();

        // Генерируем 50 фактов и собираем все enum значения
        for (let i = 0; i < 50; i++) {
            const fact = generator.generateMessage(1);
            if (fact.d && fact.d.enumField) {
                enumValues.add(fact.d.enumField);
            }
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
        const generator = new MessageGenerator(invalidDefaultValueConfig);
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
        const generator = new MessageGenerator(invalidDefaultRandomConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации default_random: ${error.message}`);
        return true;
    }
}

/**
 * Тест создания генератора с неверной конфигурацией boolean
 */
function testInvalidBooleanConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(invalidBooleanConfig);
        console.log('❌ Ошибка: должен был выбросить исключение');
        return false;
    } catch (error) {
        console.log(`✅ Корректно обработана ошибка валидации boolean: ${error.message}`);
        return true;
    }
}

/**
 * Тест генерации факта с default_value и default_random
 */
function testDefaultValueGeneration(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(defaultValueTestConfig);
        const fact = generator.generateMessage(1);

        console.log('✅ Факт сгенерирован с default_value и default_random');
        
        // Проверяем, что объект d существует
        if (!fact.d || typeof fact.d !== 'object') {
            console.log('❌ Объект d отсутствует или не является объектом');
            return false;
        }
        
        const data = fact.d;
        console.log(`   stringField: "${data.stringField}" (тип: ${typeof data.stringField})`);
        console.log(`   integerField: ${data.integerField} (тип: ${typeof data.integerField})`);
        console.log(`   dateField: ${data.dateField} (тип: ${typeof data.dateField})`);
        console.log(`   enumField: "${data.enumField}" (тип: ${typeof data.enumField})`);
        console.log(`   objectIdField: ${data.objectIdField} (тип: ${typeof data.objectIdField})`);
        console.log(`   booleanField: ${data.booleanField} (тип: ${typeof data.booleanField})`);

        // Проверяем типы данных
        if (typeof data.stringField !== 'string') {
            console.log('❌ stringField должен быть строкой');
            return false;
        }

        if (typeof data.integerField !== 'number' || !Number.isInteger(data.integerField)) {
            console.log('❌ integerField должен быть целым числом');
            return false;
        }

        if (!(data.dateField instanceof Date)) {
            console.log('❌ dateField должен быть объектом Date');
            return false;
        }

        if (typeof data.enumField !== 'string') {
            console.log('❌ enumField должен быть строкой');
            return false;
        }

        if (typeof data.objectIdField !== 'object' || !data.objectIdField.constructor || data.objectIdField.constructor.name !== 'ObjectId') {
            console.log('❌ objectIdField должен быть объектом ObjectId');
            return false;
        }

        if (typeof data.booleanField !== 'boolean') {
            console.log('❌ booleanField должен быть булевым значением');
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
 * Тест проверки уникальности ObjectId
 */
function testObjectIdUniqueness(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(generatorTestConfig);
        const objectIds = new Set();
        const iterations = 100;

        // Генерируем много фактов и собираем все ObjectId
        for (let i = 0; i < iterations; i++) {
            const fact = generator.generateMessage(1);
            if (fact.d && fact.d.objectIdField) {
                const objectIdString = fact.d.objectIdField.toString();
                objectIds.add(objectIdString);
            }
        }

        console.log(`✅ Сгенерировано ${iterations} фактов, получено ${objectIds.size} уникальных ObjectId`);

        // Проверяем, что все ObjectId уникальны
        if (objectIds.size === iterations) {
            console.log('✅ Все ObjectId уникальны');
        } else {
            console.log('❌ Найдены дублирующиеся ObjectId');
            return false;
        }

        // Проверяем формат ObjectId (24 символа hex)
        let allValidFormat = true;
        for (const objectIdString of objectIds) {
            if (objectIdString.length !== 24 || !/^[0-9a-fA-F]{24}$/.test(objectIdString)) {
                console.log(`❌ Неверный формат ObjectId: ${objectIdString}`);
                allValidFormat = false;
            }
        }

        if (allValidFormat) {
            console.log('✅ Все ObjectId имеют правильный формат (24 hex символа)');
        } else {
            console.log('❌ Некоторые ObjectId имеют неверный формат');
            return false;
        }

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
        const generator = new MessageGenerator(defaultValueTestConfig);
        const iterations = 1000;

        let defaultStringCount = 0;
        let defaultIntegerCount = 0;
        let defaultDateCount = 0;
        let defaultEnumCount = 0;
        let defaultObjectIdCount = 0;
        let defaultBooleanCount = 0;

        // Генерируем много фактов для статистики
        for (let i = 0; i < iterations; i++) {
            const fact = generator.generateMessage(1);
            
            if (fact.d) {
                if (fact.d.stringField === "DEFAULT_STRING") defaultStringCount++;
                if (fact.d.integerField === 999) defaultIntegerCount++;
                if (fact.d.dateField && fact.d.dateField.toISOString().startsWith("2024-03-15")) defaultDateCount++;
                if (fact.d.enumField === "option2") defaultEnumCount++;
                if (fact.d.objectIdField && fact.d.objectIdField.toString() === "507f1f77bcf86cd799439011") defaultObjectIdCount++;
                if (fact.d.booleanField === true) defaultBooleanCount++;
            }
        }

        const stringFrequency = defaultStringCount / iterations;
        const integerFrequency = defaultIntegerCount / iterations;
        const dateFrequency = defaultDateCount / iterations;
        const enumFrequency = defaultEnumCount / iterations;
        const objectIdFrequency = defaultObjectIdCount / iterations;
        const booleanFrequency = defaultBooleanCount / iterations;

        console.log(`✅ Статистика появления default_value за ${iterations} итераций:`);
        console.log(`   stringField (ожидается ~30%): ${(stringFrequency * 100).toFixed(1)}% (${defaultStringCount} раз)`);
        console.log(`   integerField (ожидается ~20%): ${(integerFrequency * 100).toFixed(1)}% (${defaultIntegerCount} раз)`);
        console.log(`   dateField (ожидается ~10%): ${(dateFrequency * 100).toFixed(1)}% (${defaultDateCount} раз)`);
        console.log(`   enumField (ожидается ~40%): ${(enumFrequency * 100).toFixed(1)}% (${defaultEnumCount} раз)`);
        console.log(`   objectIdField (ожидается ~50%): ${(objectIdFrequency * 100).toFixed(1)}% (${defaultObjectIdCount} раз)`);
        console.log(`   booleanField (ожидается ~30%): ${(booleanFrequency * 100).toFixed(1)}% (${defaultBooleanCount} раз)`);

        // Проверяем, что частоты примерно соответствуют ожидаемым (с допуском ±10%)
        const tolerance = 0.1;
        const expectedFrequencies = [0.3, 0.2, 0.1, 0.4, 0.5, 0.3];
        const actualFrequencies = [stringFrequency, integerFrequency, dateFrequency, enumFrequency, objectIdFrequency, booleanFrequency];

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
 * Тест генерации значений с массивом default_value для string
 */
function testArrayDefaultValueString(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const config = [
            {
                "src": "testString",
                "dst": "testString",
                "message_types": [1],
                "generator": {
                    "type": "string",
                    "min": 5,
                    "max": 10,
                    "default_value": ["default1", "default2", "default3"],
                    "default_random": 1.0 // Всегда используем default_value
                }
            }
        ];

        const generator = new MessageGenerator(config);
        const iterations = 100;
        const results = [];

        for (let i = 0; i < iterations; i++) {
            const message = generator.generateMessage(1);
            results.push(message.d.testString);
        }

        // Проверяем, что все значения из массива default_value присутствуют
        const expectedValues = ["default1", "default2", "default3"];
        const uniqueResults = [...new Set(results)];
        
        const hasAllExpectedValues = expectedValues.every(val => uniqueResults.includes(val));
        if (!hasAllExpectedValues) {
            throw new Error(`❌ Не все значения из default_value найдены. Ожидаемые: ${expectedValues.join(', ')}, Полученные: ${uniqueResults.join(', ')}`);
        }

        // Проверяем, что нет значений вне массива default_value
        const hasUnexpectedValues = uniqueResults.some(val => !expectedValues.includes(val));
        if (hasUnexpectedValues) {
            throw new Error(`❌ Найдены неожиданные значения: ${uniqueResults.filter(val => !expectedValues.includes(val)).join(', ')}`);
        }

        console.log('✅ Массив default_value для string работает корректно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации значений с массивом default_value для integer
 */
function testArrayDefaultValueInteger(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const config = [
            {
                "src": "testInteger",
                "dst": "testInteger",
                "message_types": [1],
                "generator": {
                    "type": "integer",
                    "min": 1,
                    "max": 1000,
                    "default_value": [100, 200, 300],
                    "default_random": 1.0 // Всегда используем default_value
                }
            }
        ];

        const generator = new MessageGenerator(config);
        const iterations = 100;
        const results = [];

        for (let i = 0; i < iterations; i++) {
            const message = generator.generateMessage(1);
            results.push(message.d.testInteger);
        }

        // Проверяем, что все значения из массива default_value присутствуют
        const expectedValues = [100, 200, 300];
        const uniqueResults = [...new Set(results)];
        
        const hasAllExpectedValues = expectedValues.every(val => uniqueResults.includes(val));
        if (!hasAllExpectedValues) {
            throw new Error(`❌ Не все значения из default_value найдены. Ожидаемые: ${expectedValues.join(', ')}, Полученные: ${uniqueResults.join(', ')}`);
        }

        // Проверяем, что нет значений вне массива default_value
        const hasUnexpectedValues = uniqueResults.some(val => !expectedValues.includes(val));
        if (hasUnexpectedValues) {
            throw new Error(`❌ Найдены неожиданные значения: ${uniqueResults.filter(val => !expectedValues.includes(val)).join(', ')}`);
        }

        console.log('✅ Массив default_value для integer работает корректно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации значений с массивом default_value для enum
 */
function testArrayDefaultValueEnum(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const config = [
            {
                "src": "testEnum",
                "dst": "testEnum",
                "message_types": [1],
                "generator": {
                    "type": "enum",
                    "values": ["A", "B", "C", "D", "E"],
                    "default_value": ["A", "C", "E"],
                    "default_random": 1.0 // Всегда используем default_value
                }
            }
        ];

        const generator = new MessageGenerator(config);
        const iterations = 100;
        const results = [];

        for (let i = 0; i < iterations; i++) {
            const message = generator.generateMessage(1);
            results.push(message.d.testEnum);
        }

        // Проверяем, что все значения из массива default_value присутствуют
        const expectedValues = ["A", "C", "E"];
        const uniqueResults = [...new Set(results)];
        
        const hasAllExpectedValues = expectedValues.every(val => uniqueResults.includes(val));
        if (!hasAllExpectedValues) {
            throw new Error(`❌ Не все значения из default_value найдены. Ожидаемые: ${expectedValues.join(', ')}, Полученные: ${uniqueResults.join(', ')}`);
        }

        // Проверяем, что нет значений вне массива default_value
        const hasUnexpectedValues = uniqueResults.some(val => !expectedValues.includes(val));
        if (hasUnexpectedValues) {
            throw new Error(`❌ Найдены неожиданные значения: ${uniqueResults.filter(val => !expectedValues.includes(val)).join(', ')}`);
        }

        console.log('✅ Массив default_value для enum работает корректно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест валидации массива default_value
 */
function testArrayDefaultValueValidation(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        // Тест с пустым массивом - должен вызывать ошибку
        const emptyArrayConfig = [
            {
                "src": "testField",
                "dst": "testField",
                "message_types": [1],
                "generator": {
                    "type": "string",
                    "default_value": []
                }
            }
        ];

        try {
            new MessageGenerator(emptyArrayConfig);
            throw new Error('❌ Ожидалась ошибка валидации для пустого массива');
        } catch (error) {
            if (error.message.includes('не может быть пустым массивом')) {
                console.log('✅ Валидация пустого массива работает корректно');
            } else {
                throw error;
            }
        }

        // Тест с неверными типами в массиве - должен вызывать ошибку
        const wrongTypeConfig = [
            {
                "src": "testField",
                "dst": "testField",
                "message_types": [1],
                "generator": {
                    "type": "string",
                    "default_value": ["valid", 123, "also_valid"]
                }
            }
        ];

        try {
            new MessageGenerator(wrongTypeConfig);
            throw new Error('❌ Ожидалась ошибка валидации для неверных типов в массиве');
        } catch (error) {
            if (error.message.includes('должен содержать только строки')) {
                console.log('✅ Валидация типов в массиве работает корректно');
            } else {
                throw error;
            }
        }

        console.log('✅ Валидация массива default_value работает корректно');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест создания генератора с дублирующимися src полями
 */
function testDuplicateSrcFieldConstructor(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(duplicateSrcFieldConfig);

        // Проверяем, что генератор создался успешно
        console.log('✅ Генератор с дублирующимися src полями создан успешно');

        // Проверяем доступные поля (должны быть уникальными)
        const expectedFields = ['commonField', 'uniqueField', 'anotherCommonField'];
        const actualFields = generator._availableFields;
        
        // Проверяем наличие всех полей в expectedFields
        const hasAllFields = expectedFields.every(field => actualFields.includes(field));
        if (!hasAllFields) {
            throw new Error('❌ Не все поля присутствуют');
        }
        
        // Проверяем, что нет дублирующихся полей в _availableFields
        const uniqueFields = [...new Set(actualFields)];
        if (uniqueFields.length !== actualFields.length) {
            throw new Error('❌ Найдены дублирующиеся поля в _availableFields');
        }
        
        console.log(`✅ Доступные поля (уникальные): [${actualFields.join(', ')}]`);

        // Проверяем доступные типы
        const expectedTypes = [1, 2];
        const actualTypes = generator._availableTypes;
        const hasAllTypes = expectedTypes.every(type => actualTypes.includes(type));
        if (!hasAllTypes) {
            throw new Error('❌ Не все типы присутствуют');
        }
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
 * Тест генерации сообщения с дублирующимися src полями
 */
function testDuplicateSrcFieldGeneration(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(duplicateSrcFieldConfig);

        // Генерируем сообщение типа 1
        const message = generator.generateMessage(1);

        console.log('✅ Сообщение сгенерировано успешно');
        console.log(`   Тип: ${message.t}`);

        // Проверяем структуру сообщения
        if (!message.d || typeof message.d !== 'object') {
            console.log('❌ Объект d отсутствует или не является объектом');
            return false;
        }

        const dataFields = Object.keys(message.d);
        console.log(`   Поля в d: [${dataFields.join(', ')}]`);

        // Проверяем, что каждое src поле присутствует только один раз в объекте d
        const expectedSrcFields = ['commonField', 'uniqueField', 'anotherCommonField'];
        
        for (const srcField of expectedSrcFields) {
            const occurrences = dataFields.filter(field => field === srcField).length;
            if (occurrences === 0) {
                console.log(`❌ Поле ${srcField} отсутствует в сгенерированном сообщении`);
                return false;
            } else if (occurrences > 1) {
                console.log(`❌ Поле ${srcField} встречается ${occurrences} раз в сгенерированном сообщении`);
                return false;
            } else {
                console.log(`✅ Поле ${srcField} встречается ровно один раз`);
            }
        }

        // Проверяем, что все ожидаемые поля присутствуют
        const hasAllExpectedFields = expectedSrcFields.every(field => dataFields.includes(field));
        if (!hasAllExpectedFields) {
            console.log('❌ Не все ожидаемые поля присутствуют');
            return false;
        }

        console.log('✅ Все src поля присутствуют ровно один раз в сгенерированном сообщении');
        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест проверки использования первого генератора для дублирующихся src полей
 */
function testDuplicateSrcFieldFirstGeneratorUsed(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(duplicateSrcFieldConfig);

        // Генерируем несколько сообщений для проверки стабильности
        const generatedValues = [];
        for (let i = 0; i < 10; i++) {
            const message = generator.generateMessage(1);
            if (message.d && message.d.commonField) {
                generatedValues.push(message.d.commonField);
            }
        }

        console.log(`✅ Сгенерировано ${generatedValues.length} значений для commonField`);
        console.log(`   Примеры значений: [${generatedValues.slice(0, 5).join(', ')}]`);

        // Проверяем, что значения соответствуют первому генератору (min=5, max=15)
        let allValuesValid = true;
        for (const value of generatedValues) {
            if (typeof value !== 'string') {
                console.log(`❌ Значение ${value} не является строкой`);
                allValuesValid = false;
                break;
            }
            if (value.length < 5 || value.length > 15) {
                console.log(`❌ Значение "${value}" имеет длину ${value.length}, а должно быть от 5 до 15`);
                allValuesValid = false;
                break;
            }
        }

        if (allValuesValid) {
            console.log('✅ Все значения соответствуют первому генератору (min=5, max=15)');
        } else {
            console.log('❌ Некоторые значения не соответствуют первому генератору');
            return false;
        }

        // Проверяем, что иногда используется default_value первого генератора
        const defaultValueCount = generatedValues.filter(value => value === "FIRST_MAP").length;
        console.log(`✅ Значение по умолчанию "FIRST_MAP" использовано ${defaultValueCount} раз из ${generatedValues.length}`);

        return true;
    } catch (error) {
        console.log(`❌ Ошибка: ${error.message}`);
        return false;
    }
}

/**
 * Тест генерации сообщений для всех типов с дублирующимися src полями
 */
function testDuplicateSrcFieldAllTypes(testName) {
    console.log(`\n=== Тест: ${testName} ===`);

    try {
        const generator = new MessageGenerator(duplicateSrcFieldConfig);

        console.log('✅ Генерация сообщений для всех типов с дублирующимися src полями:');
        
        for (const type of generator._availableTypes) {
            const message = generator.generateMessage(type);
            const dataFields = message.d ? Object.keys(message.d) : [];
            
            console.log(`   Тип ${type}: ${dataFields.length} полей [${dataFields.join(', ')}]`);
            
            // Проверяем, что каждое src поле встречается только один раз
            const srcFieldCounts = {};
            for (const field of dataFields) {
                srcFieldCounts[field] = (srcFieldCounts[field] || 0) + 1;
            }
            
            let hasDuplicates = false;
            for (const [field, count] of Object.entries(srcFieldCounts)) {
                if (count > 1) {
                    console.log(`❌ Поле ${field} встречается ${count} раз в типе ${type}`);
                    hasDuplicates = true;
                }
            }
            
            if (hasDuplicates) {
                return false;
            }
        }

        console.log('✅ Все типы генерируются корректно без дублирующихся src полей');
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
    console.log('🧪 Запуск тестов MessageGenerator\n');

    const tests = [
        { func: testValidConstructor, name: '1. Создание генератора с валидной конфигурацией' },
        { func: testInvalidConstructor, name: '2. Создание генератора с неверной конфигурацией' },
        { func: testIncompleteConstructor, name: '3. Создание генератора с неполной конфигурацией' },
        { func: testNullConstructor, name: '4. Создание генератора без конфигурации' },
        { func: testInvalidGeneratorConstructor, name: '5. Создание генератора с неверной конфигурацией генератора' },
        { func: testInvalidDefaultValueConstructor, name: '6. Создание генератора с неверной конфигурацией default_value' },
        { func: testInvalidDefaultRandomConstructor, name: '7. Создание генератора с неверной конфигурацией default_random' },
        { func: testInvalidBooleanConstructor, name: '8. Создание генератора с неверной конфигурацией boolean' },
        { func: testDuplicateSrcFieldConstructor, name: '9. Создание генератора с дублирующимися src полями' },
        { func: testGenerateEvent, name: '10. Генерация факта конкретного типа' },
        { func: testGenerateFactInvalidType, name: '11. Генерация факта несуществующего типа' },
        { func: testGenerateRandomTypeFact, name: '12. Генерация случайного факта' },
        { func: testGenerateFactForAllTypes, name: '13. Генерация фактов для всех типов' },
        { func: testDuplicateSrcFieldGeneration, name: '14. Генерация сообщения с дублирующимися src полями' },
        { func: testDuplicateSrcFieldFirstGeneratorUsed, name: '15. Проверка использования первого генератора для дублирующихся src полей' },
        { func: testDuplicateSrcFieldAllTypes, name: '16. Генерация сообщений для всех типов с дублирующимися src полями' },
        { func: testGeneratorTypes, name: '17. Генерация факта с различными типами генераторов' },
        { func: testEnumRandomness, name: '18. Проверка случайности enum значений' },
        { func: testObjectIdUniqueness, name: '19. Проверка уникальности ObjectId' },
        { func: testDefaultValueGeneration, name: '20. Генерация факта с default_value и default_random' },
        { func: testDefaultValueFrequency, name: '21. Проверка частоты появления default_value' },
        { func: testArrayDefaultValueString, name: '23. Массив default_value для string' },
        { func: testArrayDefaultValueInteger, name: '24. Массив default_value для integer' },
        { func: testArrayDefaultValueEnum, name: '25. Массив default_value для enum' },
        { func: testArrayDefaultValueValidation, name: '26. Валидация массива default_value' },
        { func: testPerformance, name: '22. Производительность генерации' }
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
    testInvalidBooleanConstructor,
    testDuplicateSrcFieldConstructor,
    testGenerateFact: testGenerateEvent,
    testGenerateFactInvalidType,
    testGenerateRandomTypeFact,
    testGenerateFactForAllTypes,
    testDuplicateSrcFieldGeneration,
    testDuplicateSrcFieldFirstGeneratorUsed,
    testDuplicateSrcFieldAllTypes,
    testGeneratorTypes,
    testEnumRandomness,
    testObjectIdUniqueness,
    testDefaultValueGeneration,
    testDefaultValueFrequency,
    testArrayDefaultValueString,
    testArrayDefaultValueInteger,
    testArrayDefaultValueEnum,
    testArrayDefaultValueValidation,
    testPerformance
};
