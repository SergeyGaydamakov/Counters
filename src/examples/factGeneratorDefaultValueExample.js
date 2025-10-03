const FactGenerator = require('../generators/factGenerator');

/**
 * Пример использования FactGenerator с default_value и default_random
 */

// Конфигурация с различными типами генераторов и значениями по умолчанию
const defaultValueFieldConfig = [
    {
        "src": "userId",
        "dst": "userId",
        "fact_types": [1, 2, 3],
        "generator": {
            "type": "integer",
            "min": 1000,
            "max": 9999,
            "default_value": 1234,
            "default_random": 0.1  // 10% вероятность получить 1234
        }
    },
    {
        "src": "userName",
        "dst": "userName",
        "fact_types": [1, 2],
        "generator": {
            "type": "string",
            "min": 5,
            "max": 15,
            "default_value": "admin",
            "default_random": 0.05  // 5% вероятность получить "admin"
        }
    },
    {
        "src": "actionType",
        "dst": "actionType",
        "fact_types": [1, 2, 3],
        "generator": {
            "type": "enum",
            "values": ["login", "logout", "purchase", "view", "search", "update"],
            "default_value": "login",
            "default_random": 0.2  // 20% вероятность получить "login"
        }
    },
    {
        "src": "eventDate",
        "dst": "eventDate",
        "fact_types": [2, 3],
        "generator": {
            "type": "date",
            "min": "2024-01-01",
            "max": "2024-12-31",
            "default_value": "2024-06-15",
            "default_random": 0.15  // 15% вероятность получить 2024-06-15
        }
    },
    {
        "src": "status",
        "dst": "status",
        "fact_types": [1, 2, 3],
        "generator": {
            "type": "enum",
            "values": ["active", "inactive", "pending", "suspended"],
            "default_value": "active",
            "default_random": 0.3  // 30% вероятность получить "active"
        }
    },
    {
        "src": "description",
        "dst": "description",
        "fact_types": [1, 2, 3]
        // Без generator - будет использовано значение по умолчанию (string 6-20 символов)
    }
];

console.log('🚀 Пример использования FactGenerator с default_value и default_random\n');

try {
    // Создаем генератор с кастомной конфигурацией
    const generator = new FactGenerator(defaultValueFieldConfig);
    
    console.log('✅ Генератор создан успешно');
    console.log(`   Доступные поля: [${generator._availableFields.join(', ')}]`);
    console.log(`   Доступные типы: [${generator._availableTypes.join(', ')}]\n`);
    
    // Генерируем несколько фактов разных типов
    console.log('📊 Генерация фактов с default_value и default_random:\n');
    
    for (let i = 1; i <= 3; i++) {
        const fact = generator.generateFact(i);
        console.log(`Факт типа ${i}:`);
        console.log(`  ID: ${fact.i}`);
        console.log(`  Тип: ${fact.t}`);
        console.log(`  Дата создания: ${fact.c.toISOString()}`);
        console.log(`  Поля:`);
        
        // Выводим поля с их значениями и типами
        Object.keys(fact).forEach(key => {
            if (key.startsWith('f') || ['userId', 'userName', 'actionType', 'eventDate', 'status', 'description'].includes(key)) {
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
    
    // Демонстрация частоты появления default_value
    console.log('🎲 Демонстрация частоты появления default_value:');
    const iterations = 1000;
    
    let defaultUserIdCount = 0;
    let defaultUserNameCount = 0;
    let defaultActionTypeCount = 0;
    let defaultEventDateCount = 0;
    let defaultStatusCount = 0;
    
    for (let i = 0; i < iterations; i++) {
        const fact = generator.generateFact(1);
        
        if (fact.userId === 1234) defaultUserIdCount++;
        if (fact.userName === "admin") defaultUserNameCount++;
        if (fact.actionType === "login") defaultActionTypeCount++;
        if (fact.eventDate && fact.eventDate.toISOString().startsWith("2024-06-15")) defaultEventDateCount++;
        if (fact.status === "active") defaultStatusCount++;
    }
    
    console.log(`   За ${iterations} итераций:`);
    console.log(`   userId = 1234: ${defaultUserIdCount} раз (${(defaultUserIdCount/iterations*100).toFixed(1)}%, ожидается ~10%)`);
    console.log(`   userName = "admin": ${defaultUserNameCount} раз (${(defaultUserNameCount/iterations*100).toFixed(1)}%, ожидается ~5%)`);
    console.log(`   actionType = "login": ${defaultActionTypeCount} раз (${(defaultActionTypeCount/iterations*100).toFixed(1)}%, ожидается ~20%)`);
    console.log(`   eventDate = "2024-06-15": ${defaultEventDateCount} раз (${(defaultEventDateCount/iterations*100).toFixed(1)}%, ожидается ~15%)`);
    console.log(`   status = "active": ${defaultStatusCount} раз (${(defaultStatusCount/iterations*100).toFixed(1)}%, ожидается ~30%)\n`);
    
    // Демонстрация различных значений actionType
    console.log('🎯 Демонстрация различных значений actionType:');
    const actionTypes = new Set();
    for (let i = 0; i < 50; i++) {
        const fact = generator.generateFact(1);
        actionTypes.add(fact.actionType);
    }
    console.log(`   Получено ${actionTypes.size} уникальных значений: [${Array.from(actionTypes).join(', ')}]`);
    
    // Подсчет количества default значений
    const defaultActionCount = Array.from(actionTypes).filter(type => type === "login").length;
    console.log(`   Из них "login" (default): ${defaultActionCount} раз\n`);
    
    // Демонстрация диапазонов значений
    console.log('📈 Демонстрация диапазонов значений:');
    const userIds = [];
    const userNames = [];
    for (let i = 0; i < 20; i++) {
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
