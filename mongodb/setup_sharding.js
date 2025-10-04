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
print("process.argv: "+process.argv);
print("process.env: "+JSON.stringify(process.env));

// Функция для выполнения команд с обработкой ошибок
function executeCommand(command, description) {
    try {
        print(`Выполнение: ${description}`);
        const result = adminDb.runCommand(command);
        if (result.ok === 1) {
            // print(`✓ Успешно: ${description}`);
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

// 1. Проверка статуса шардирования
print("\n1. Проверка статуса шардирования...");

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
} else {
    print("✗ Не удалось получить список шардов. Скорее всего шардирование не включено. Выполнение скрипта прервано.");
    hasError = true;
    exit(1);
}

// 2. Включение шардирования для базы данных
print(`\n2. Включение шардирования для базы данных ${DATABASE_NAME}...`);
const enableShardingResult = executeCommand(
    { enableSharding: DATABASE_NAME },
    `Включение шардирования для базы данных ${DATABASE_NAME}`
);

if (!enableShardingResult.success) {
    print(`Ошибка: Не удалось включить шардирование для базы данных ${DATABASE_NAME}`);
    hasError = true;
    quit(1);
}

// Проверка состояния балансировщика
const balancerStateResult = sh.isBalancerRunning();
if (balancerStateResult.ok) {
    print(`Состояние балансировщика: ${balancerStateResult.mode == 'full' ? 'включен' : 'отключен'}`);
} else {
    print("✗ Не удалось получить состояние балансировщика");
    hasError = true;
}

// 3. Создание схемы для коллекции facts
try {
    print(`\n3. Создание схемы валидации для коллекции ${FACTS_COLLECTION}...`);
    
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
        c: {
            bsonType: "date",
            description: "Дата и время создания объекта"
        },
        d: {
            bsonType: "object",
            description: "JSON объект с данными факта"
        },
        z: {
            bsonType: "string",
            description: "Поле заполнения для достижения целевого размера JSON (необязательное)"
        }
    };
    
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

// 4. Настройка шардирования для коллекции facts
print(`\n4. Настройка шардирования для коллекции ${FACTS_COLLECTION}...`);
const factsShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACTS_COLLECTION}`,
        key: { i: 1 } // Шардирование по полю i
    },
    `Настройка шардирования для коллекции ${FACTS_COLLECTION} по ключу {i: 1}`
);

if (!factsShardingResult.success) {
    print(`Ошибка: Не удалось настроить шардирование для коллекции ${FACTS_COLLECTION}`);
    quit(1);
}

// 5. Создание схемы для коллекции factIndex
try {
    print(`\n5. Создание схемы валидации для коллекции ${FACT_INDEX_COLLECTION}...`);
    
    const factIndexSchema = {
        $jsonSchema: {
            bsonType: "object",
            title: "FactIndex Collection Schema",
            description: "Схема для коллекции индексных значений фактов",
            required: ["h", "i", "t", "d", "c"],
            properties: {
                _id: {
                    bsonType: "objectId",
                    description: "MongoDB ObjectId (автоматически генерируется)"
                },
                h: {
                    bsonType: "string",
                    description: "Хеш значение типа + поля факта"
                },
                i: {
                    bsonType: "objectId",
                    description: "Уникальный идентификатор факта (GUID)"
                },
                t: {
                    bsonType: "int",
                    minimum: 1,
                    description: "Тип факта - целое число >= 1"
                },
                d: {
                    bsonType: "date",
                    description: "Дата факта"
                },
                c: {
                    bsonType: "date",
                    description: "Дата и время создания индексного значения"
                },
                v: {
                    bsonType: "string",
                    description: "Индексное значение поля факта"
                },
                it: {
                    bsonType: "int",
                    minimum: 1,
                    description: "Тип индекса - целое число >= 1"
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
        print(`✓ Схема валидации для коллекции ${FACT_INDEX_COLLECTION} обновлена`);
    } else {
        // Коллекция не существует, создаем с валидацией
        factIndexDb.createCollection(FACT_INDEX_COLLECTION, {
            validator: factIndexSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print(`✓ Коллекция ${FACT_INDEX_COLLECTION} создана со схемой валидации`);
    }
} catch (error) {
    print(`✗ Ошибка создания схемы для ${FACT_INDEX_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 6. Настройка шардирования для коллекции factIndex
print(`\n6. Настройка шардирования для коллекции ${FACT_INDEX_COLLECTION}...`);
const factIndexShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACT_INDEX_COLLECTION}`,
        key: { h: 1, i: 1 } // Составной ключ: h (ascending) + i (ascending)
    },
    `Настройка шардирования для коллекции ${FACT_INDEX_COLLECTION} по ключу {h: 1, i: 1}`
);

if (!factIndexShardingResult.success) {
    print(`Ошибка: Не удалось настроить шардирование для коллекции ${FACT_INDEX_COLLECTION}`);
    quit(1);
}

// Индексы для коллекции facts
try {
    print(`\n7. Создание индексов для коллекции ${FACTS_COLLECTION}...`);
    const factsCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACTS_COLLECTION);
    const factsIndexes = factsCollection.getIndexes();
    print(`Существующие индексы для коллекции ${FACTS_COLLECTION}:`);
    factsIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
    // Если индекс с точным ключом key: {i: 1} не существует, создаем его
    if (factsIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({i: 1})) === undefined) {
        factsCollection.createIndex(
            { i: 1 },
            { name: "idx_i_unique", background: true, unique: true }
        );
        print(`✓ Индекс idx_i_unique создан`);
    } else {
        print(`✓ Индекс idx_i_unique уже существует`);
    }
    // Если индекс с точным ключом key: {c: -1} не существует, создаем его
    if (factsIndexes.find(index => JSON.stringify(index.key) === JSON.stringify({c: -1})) === undefined) {
        // Вспомогательный индекс для подсчета статистики
        factsCollection.createIndex(
            { c: -1 },
            { name: "idx_c", background: true, unique: false }
        );
        print("✓ Индекс idx_c создан");
    } else {
        print("✓ Индекс idx_c уже существует");
    }
} catch (error) {
    print(`✗ Ошибка создания индексов для ${FACTS_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 8. Индексы для коллекции factIndex
try {
    print(`\n8. Создание индексов для коллекции ${FACT_INDEX_COLLECTION}...`);
    const factIndexCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACT_INDEX_COLLECTION);
    const factIndexIndexes = factIndexCollection.getIndexes();
    print(`Существующие индексы для коллекции ${FACT_INDEX_COLLECTION}:`);
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
        print("✓ Индекс idx_h_i_unique создан");
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
        print("✓ Индекс idx_h_d_i создан");
    } else {
        print("✓ Индекс idx_h_d_i уже существует");
    }
} catch (error) {
    print(`✗ Ошибка создания индексов для ${FACT_INDEX_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 9. Создание зон шардирования
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
    /*
    shards.forEach(shard => {
        var res = sh.addShardToZone(shard, shard);
        if (!res.ok) {
            print("ERROR: Can not add shard <"+shard+"> to zone: " + res.errmsg);
            return false;
        }
        print("Shard <"+shard+"> added to zone: " + shard);
    });
    */
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
        print(`Removing ${db.getSiblingDB("config").tags.find({"ns": range.namespace}).count()} tag ranges for ${range.namespace}`);
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

print("\n9. Создание зон шардирования...");
if (CreateShardZones()) {
    print("✓ Зоны шардирования созданы");
} else {
    print("✗ Не удалось создать зоны шардирования");
    hasError = true;
}

print("\n=== Настройка шардирования завершена ===");
if (hasError) {
    print("✗ Возникли ошибки при настройке шардирования, смотри сообщения выше");
} else {
    print("✓ Настройка шардирования завершена успешно");
}

print("");
