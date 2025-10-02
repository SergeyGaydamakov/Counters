// Скрипт для настройки шардирования коллекций fact и factIndex
// Использование: mongosh --file setup_sharding.js

print("=== Настройка шардирования для коллекций fact и factIndex ===");

// Конфигурация
const DATABASE_NAME = "CounterTest"; // Замените на нужное имя базы данных
const FACTS_COLLECTION = "facts";
const FACT_INDEX_COLLECTION = "factIndex";

// Управление выполняется в admin базе данных
const adminDb = db.getSiblingDB("admin");
let hasError = false;

// Функция для выполнения команд с обработкой ошибок
function executeCommand(command, description) {
    try {
        print(`Выполнение: ${description}`);
        const result = adminDb.runCommand(command);
        if (result.ok === 1) {
            print(`✓ Успешно: ${description}`);
            return { success: true, result: result };
        } else {
            print(`✗ Ошибка: ${description} - ${result.errmsg || 'Неизвестная ошибка'}`);
            return { success: false, error: result.errmsg || 'Неизвестная ошибка' };
        }
    } catch (error) {
        print(`✗ Исключение: ${description} - ${error.message}`);
        hasError = true;
        return { success: false, error: error.message };
    }
}

// 1. Включение шардирования для базы данных
print("\n1. Включение шардирования для базы данных...");
const enableShardingResult = executeCommand(
    { enableSharding: DATABASE_NAME },
    `Включение шардирования для базы данных ${DATABASE_NAME}`
);

if (!enableShardingResult.success) {
    print("Ошибка: Не удалось включить шардирование для базы данных");
    hasError = true;
    quit(1);
}

// 2. Настройка шардирования для коллекции facts
print("\n2. Настройка шардирования для коллекции facts...");
const factsShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACTS_COLLECTION}`,
        key: { i: 1 } // Шардирование по полю i
    },
    `Настройка шардирования для коллекции ${FACTS_COLLECTION} по ключу {i: 1}`
);

if (!factsShardingResult.success) {
    print("Ошибка: Не удалось настроить шардирование для коллекции facts");
    quit(1);
}

// 3. Настройка шардирования для коллекции factIndex
print("\n3. Настройка шардирования для коллекции factIndex...");
const factIndexShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACT_INDEX_COLLECTION}`,
        key: { h: 1, i: 1 } // Составной ключ: h (ascending) + i (ascending)
    },
    `Настройка шардирования для коллекции ${FACT_INDEX_COLLECTION} по ключу {h: 1, i: 1}`
);

if (!factIndexShardingResult.success) {
    print("Ошибка: Не удалось настроить шардирование для коллекции factIndex");
    quit(1);
}

// 4. Проверка статуса шардирования
print("\n4. Проверка статуса шардирования...");

// Получение списка шардов
const listShardsResult = executeCommand(
    { listShards: 1 },
    "Получение списка шардов"
);

if (listShardsResult.success) {
    print("Шарды:");
    listShardsResult.result.shards.forEach(shard => {
        print(`  - ${shard._id}: ${shard.host}`);
    });
}

// Проверка состояния балансировщика
const balancerStateResult = sh.isBalancerRunning();
if (balancerStateResult.ok) {
    print(`Состояние балансировщика: ${balancerStateResult.mode == 'full' ? 'включен' : 'отключен'}`);
} else {
    print("✗ Не удалось получить состояние балансировщика");
    hasError = true;
}

// 5. Создание схем валидации для коллекций
print("\n5. Создание схем валидации для коллекций...");

// Создание схемы для коллекции facts
try {
    print("Создание схемы валидации для коллекции facts...");
    
    // Создаем полную схему с динамическими полями
    const factsSchemaProperties = {
        _id: {
            bsonType: "objectId",
            description: "MongoDB ObjectId (автоматически генерируется)"
        },
        i: {
            bsonType: "objectId", 
            description: "Уникальный идентификатор факта (преобразованный из GUID)"
        },
        t: {
            bsonType: "int",
            minimum: 1,
            maximum: 100,
            description: "Тип факта - целое число от 1 до 100"
        },
        a: {
            bsonType: "int",
            minimum: 1,
            maximum: 1000000,
            description: "Количество - целое число от 1 до 1000000"
        },
        c: {
            bsonType: "date",
            description: "Дата и время создания объекта"
        },
        d: {
            bsonType: "date", 
            description: "Дата факта"
        },
        z: {
            bsonType: "string",
            description: "Поле заполнения для достижения целевого размера JSON (необязательное)"
        }
    };
    
    // Добавляем динамические поля
    for (let i = 1; i <= 23; i++) {
        factsSchemaProperties[`f${i}`] = {
            bsonType: "string",
            description: `Динамическое поле f${i} - строка`
        };
    }

    const factsSchema = {
        $jsonSchema: {
            bsonType: "object",
            title: "Facts Collection Schema",
            description: "Схема для коллекции фактов с автоматически генерируемой структурой",
            required: ["i", "t", "a", "c", "d"],
            properties: factsSchemaProperties,
            additionalProperties: false
        }
    };

    const factsDb = db.getSiblingDB(DATABASE_NAME);
    const factsCollections = factsDb.getCollectionInfos({ name: FACTS_COLLECTION });
    
    if (factsCollections.length > 0) {
        // Коллекция существует, обновляем схему валидации
        factsDb.runCommand({
            collMod: FACTS_COLLECTION,
            validator: factsSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print("✓ Схема валидации для коллекции facts обновлена");
    } else {
        // Коллекция не существует, создаем с валидацией
        factsDb.createCollection(FACTS_COLLECTION, {
            validator: factsSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print("✓ Коллекция facts создана со схемой валидации");
    }
} catch (error) {
    print(`✗ Ошибка создания схемы для facts: ${error.message}`);
    hasError = true;
}

// Создание схемы для коллекции factIndex
try {
    print("Создание схемы валидации для коллекции factIndex...");
    
    const factIndexSchema = {
        $jsonSchema: {
            bsonType: "object",
            title: "FactIndex Collection Schema",
            description: "Схема для коллекции индексных значений фактов",
            required: ["h", "i", "d", "c"],
            properties: {
                _id: {
                    bsonType: "objectId",
                    description: "MongoDB ObjectId (автоматически генерируется)"
                },
                h: {
                    bsonType: "string",
                    description: "Хеш значение типа + поля факта"
                },
                f: {
                    bsonType: "string",
                    description: "Поле факта (f1, f2, f3, ...)"
                },
                it: {
                    bsonType: "int",
                    minimum: 1,
                    maximum: 100,
                    description: "Тип индекса - целое число от 1 до 100"
                },
                i: {
                    bsonType: "string",
                    description: "Уникальный идентификатор факта (GUID)"
                },
                d: {
                    bsonType: "date",
                    description: "Дата факта"
                },
                c: {
                    bsonType: "date",
                    description: "Дата и время создания объекта"
                }
            },
            additionalProperties: false
        }
    };

    const factIndexDb = db.getSiblingDB(DATABASE_NAME);
    const factIndexCollections = factIndexDb.getCollectionInfos({ name: FACT_INDEX_COLLECTION });
    
    if (factIndexCollections.length > 0) {
        // Коллекция существует, обновляем схему валидации
        factIndexDb.runCommand({
            collMod: FACT_INDEX_COLLECTION,
            validator: factIndexSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print("✓ Схема валидации для коллекции factIndex обновлена");
    } else {
        // Коллекция не существует, создаем с валидацией
        factIndexDb.createCollection(FACT_INDEX_COLLECTION, {
            validator: factIndexSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print("✓ Коллекция factIndex создана со схемой валидации");
    }
} catch (error) {
    print(`✗ Ошибка создания схемы для factIndex: ${error.message}`);
    hasError = true;
}

// 6. Создание индексов для оптимизации шардированных коллекций
print("\n6. Создание индексов для оптимизации...");

// Индексы для коллекции facts
try {
    print("Создание индексов для коллекции facts...");
    const factsCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACTS_COLLECTION);
    const factsIndexes = factsCollection.getIndexes();
    print("Существующие индексы для коллекции facts:");
    factsIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
    // Если индекс с точным ключом key: {i: 1} не существует, создаем его
    if (factsIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({i: 1})) === undefined) {
        factsCollection.createIndex(
            { i: 1 },
            { name: "idx_i_unique", background: true, unique: true }
        );
        print("✓ Индекс создан: idx_i_unique");
    } else {
        print("✓ Индекс idx_i_unique уже существует");
    }
    // Если индекс с точным ключом key: {c: -1} не существует, создаем его
    if (factsIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({c: -1})) === undefined) {
        // Вспомогательный индекс для подсчета статистики
        factsCollection.createIndex(
            { c: -1 },
            { name: "idx_c", background: true, unique: false }
        );
        print("✓ Индекс создан: idx_c");
    } else {
        print("✓ Индекс idx_c уже существует");
    }
} catch (error) {
    print(`✗ Ошибка создания индексов для facts: ${error.message}`);
    hasError = true;
}

// Индексы для коллекции factIndex
try {
    print("Создание индексов для коллекции factIndex...");
    const factIndexCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACT_INDEX_COLLECTION);
    const factIndexIndexes = factIndexCollection.getIndexes();
    print("Существующие индексы для коллекции factIndex:");
    factIndexIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
    // Если индекс с точным ключом key: {h: 1, i: 1} не существует, создаем его
    if (factIndexIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({h: 1, i: 1})) === undefined) {
        // Уникальный составной индекс по h, i - ключ шардирования
        factIndexCollection.createIndex(
            { h: 1, i: 1 },
            { name: "idx_h_i_unique", background: true, unique: true }
        );
        print("✓ Индекс создан: idx_h_i_unique");
    } else {
        print("✓ Индекс idx_h_i_unique уже существует");
    }
    // Если индекс с точным ключом key: {h: 1, d: -1, i: 1} не существует, создаем его
    if (factIndexIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({h: 1, d: -1, i: 1})) === undefined) {
        // Составной индекс по h (хешированный), -d, i (для всех основных запросов)
        factIndexCollection.createIndex(
            { h: 1, d: -1, i: 1 },
            { name: "idx_h_d_i", background: true }
        );
        print("✓ Индекс создан: idx_h_d_i");
    } else {
        print("✓ Индекс idx_h_d_i уже существует");
    }
} catch (error) {
    print(`✗ Ошибка создания индексов для factIndex: ${error.message}`);
    hasError = true;
}

// 7. Создание зон шардирования
function CreateShardZones(databaseName = "CounterTest") {
    sh.stopBalancer();
    print("Creating shard zones:");
    var listShards = adminDb.runCommand({
        listShards: 1
    });
    if (!listShards.ok) {
        print("ERROR: Can not get listShards: " + listShards.errmsg);
        return false;
    }
    const shards = listShards.shards.map(shard => shard._id);
    print("Shards: " + shards.join(", "));
    // Добавление зон шардирования
    shards.forEach(shard => {
        var res = sh.addShardTag(shard, shard);
        if (!res.ok) {
            print("ERROR: Can not add shard <"+shard+"> to tag: " + res.errmsg);
            return false;
        }
        print("Shard <"+shard+"> added to tag: " + shard);
    });
    shards.forEach(shard => {
        var res = sh.addShardToZone(shard, shard);
        if (!res.ok) {
            print("ERROR: Can not add shard <"+shard+"> to zone: " + res.errmsg);
            return false;
        }
        print("Shard <"+shard+"> added to zone: " + shard);
    });

    // Добавление диапазонов ключей
    const ranges = [
        {
            namespace: databaseName+".facts",
            keys: [
                {i: MinKey},
                {i: ObjectId("555555555555555555555555")},
                {i: ObjectId("AAAAAAAAAAAAAAAAAAAAAAAA")},
                {i: MaxKey}
            ]
        },
        {
            namespace: databaseName+".factIndex",
            keys: [
                {h: MinKey, i: MinKey},
                {h: "5555555555555555555555555555555555555555555555555555555555555555", i: MinKey},
                {h: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", i: MinKey},
                {h: MaxKey, i: MaxKey}
            ]
        },
    ];
    print("*** TAGS ********************************************")
    ranges.forEach(range => {
        // Сначала удалим существующие диапазоны
        print("Removing tag ranges <"+range.namespace+">");
        // Удаляем старые зоны коллекции
        db.getSiblingDB("config").tags.deleteMany({"ns": range.namespace});

        // Затем добавляем новые диапазоны
        print("Adding tag ranges <"+range.namespace+">");
        let count = 0;
        shards.forEach(shard => {
            const res = sh.addTagRange(range.namespace, range.keys[count], range.keys[count+1], shard);
            if (!res.ok) {
                print("ERROR: Can not add tag range <"+count+"> for shard <"+shard+">: " + res.errmsg);
                return false;
            }
            print("Tag range <"+count+"> for shard <"+shard+"> added successfully.");
            count++;
        });
    });
    sh.startBalancer();
    return true;
}

print("\n7. Создание зон шардирования...");
if (CreateShardZones()) {
    print("✓ Зоны шардирования созданы");
} else {
    print("✗ Не удалось создать зоны шардирования");
    hasError = true;
}

// 8. Запуск балансировщика
print("\n8. Запуск балансировщика...");
const balancerStartResult = executeCommand(
    { balancerStart: 1 },
    "Запуск балансировщика"
);

if (balancerStartResult.success) {
    print("✓ Балансировщик запущен");
} else {
    print("⚠ Балансировщик уже запущен или произошла ошибка");
    hasError = true;
}

print("\n=== Настройка шардирования завершена ===");
if (hasError) {
    print("✗ Возникли ошибки при настройке шардирования, смотри сообщения выше");
} else {
    print("✓ Настройка шардирования завершена успешно");
}

print("Рекомендации:");
print("1. Мониторьте работу балансировщика");
print("2. Проверяйте распределение данных по шардам");
print("3. Оптимизируйте запросы для работы с шардированными коллекциями");
print("4. Регулярно проверяйте статистику производительности");
