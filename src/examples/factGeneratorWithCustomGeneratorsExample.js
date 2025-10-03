const FactGenerator = require('../generators/factGenerator');

/**
 * Пример использования FactGenerator с различными типами генераторов
 */

// Конфигурация с различными типами генераторов
const customFieldConfig = [
    {
        "src": "userId",
        "dst": "userId",
        "fact_types": [1, 2, 3],
        "generator": {
            "type": "integer",
            "min": 1000,
            "max": 9999
        }
    },
    {
        "src": "userName",
        "dst": "userName",
        "fact_types": [1, 2],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15
        }
    },
    {
        "src": "actionType",
        "dst": "actionType",
        "fact_types": [1, 2, 3],
        "generator": {
            "type": "enum",
            "values": ["login", "logout", "purchase", "view", "search", "update"]
        }
    },
    {
        "src": "eventDate",
        "dst": "eventDate",
        "fact_types": [2, 3],
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-12-31"
        }
    },
    {
        "src": "description",
        "dst": "description",
        "fact_types": [1, 2, 3]
        // Без generator - будет использовано значение по умолчанию (string 6-20 символов)
    }
];

console.log('🚀 Пример использования FactGenerator с кастомными генераторами\n');

try {
    // Создаем генератор с кастомной конфигурацией
    const generator = new FactGenerator(customFieldConfig);
    
    console.log('✅ Генератор создан успешно');
    console.log(`   Доступные поля: [${generator._availableFields.join(', ')}]`);
    console.log(`   Доступные типы: [${generator._availableTypes.join(', ')}]\n`);
    
    // Генерируем несколько фактов разных типов
    console.log('📊 Генерация фактов:\n');
    
    for (let i = 1; i <= 3; i++) {
        const fact = generator.generateFact(i);
        console.log(`Факт типа ${i}:`);
        console.log(`  ID: ${fact.i}`);
        console.log(`  Тип: ${fact.t}`);
        console.log(`  Дата создания: ${fact.c.toISOString()}`);
        console.log(`  Поля:`);
        
        // Выводим поля с их значениями и типами
        Object.keys(fact).forEach(key => {
            if (key.startsWith('f') || ['userId', 'userName', 'actionType', 'eventDate', 'description'].includes(key)) {
                const value = fact[key];
                const type = typeof value;
                if (value instanceof Date) {
                    console.log(`    ${key}: ${value.toISOString()} (Date)`);
                } else {
                    console.log(`    ${key}: "${value}" (${type})`);
                }
            }
        });
        console.log('');
    }
    
    // Демонстрация случайности enum значений
    console.log('🎲 Демонстрация случайности enum значений (actionType):');
    const actionTypes = new Set();
    for (let i = 0; i < 20; i++) {
        const fact = generator.generateFact(1);
        actionTypes.add(fact.actionType);
    }
    console.log(`   Получено ${actionTypes.size} уникальных значений: [${Array.from(actionTypes).join(', ')}]\n`);
    
    // Демонстрация диапазонов значений
    console.log('📈 Демонстрация диапазонов значений:');
    const userIds = [];
    const userNames = [];
    for (let i = 0; i < 10; i++) {
        const fact = generator.generateFact(1);
        userIds.push(fact.userId);
        userNames.push(fact.userName);
    }
    
    console.log(`   userId (integer 1000-9999): min=${Math.min(...userIds)}, max=${Math.max(...userIds)}`);
    console.log(`   userName (string 5-15 символов): min=${Math.min(...userNames.map(n => n.length))}, max=${Math.max(...userNames.map(n => n.length))} символов`);
    console.log(`   description (по умолчанию 6-20 символов): min=${Math.min(...userNames.map(n => n.length))}, max=${Math.max(...userNames.map(n => n.length))} символов`);
    
} catch (error) {
    console.error(`❌ Ошибка: ${error.message}`);
}
