// Скрипт для настройки шардирования коллекций fact и factIndex
// Использование: mongosh --file setup_sharding.js

print("=== Настройка шардирования для коллекций fact и factIndex ===");

// Конфигурация
const DATABASE_NAME = "counters"; // Замените на нужное имя базы данных
const FACTS_COLLECTION = "facts";
const FACT_INDEX_COLLECTION = "factIndex";

// Управление выполняется в admin базе данных
const adminDb = db.getSiblingDB("admin");
let hasError = false;
// print("process.argv: "+process.argv);
// print("process.env: "+JSON.stringify(process.env));

// Функция для конвертации Hex строки в Base64
function hexToBase64(hexString) {
    // Удаляем пробелы и проверяем четность длины
    const cleanHex = hexString.replace(/\s/g, '');
    if (cleanHex.length % 2 !== 0) {
        throw new Error('Hex string length must be even');
    }
    
    // Конвертируем hex в bytes
    const bytes = [];
    for (let i = 0; i < cleanHex.length; i += 2) {
        bytes.push(parseInt(cleanHex.substr(i, 2), 16));
    }
    
    // Конвертируем bytes в base64
    const base64 = Buffer.from(bytes).toString('base64');
    return base64;
}

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

    const factsSchema = {
        $jsonSchema: {
            bsonType: "object",
            title: "Схема для коллекции фактов",
            description: "Схема для коллекции фактов",
            required: ["_id", "t", "c", "d"],
            properties: {
                _id: {
                    bsonType: "string",
                    description: "Хеш функция уникального идентификатора факта"
                },
                t: {
                    bsonType: "int",
                    minimum: 1,
                    description: "Тип факта - целое число >= 1 "
                },
                c: {
                    bsonType: "date",
                    description: "Дата и время создания объекта"
                },
                d: {
                    bsonType: "object",
                    description: "JSON объект с данными факта"
                }
            },
            additionalProperties: false
        }
    };

    const factsDb = db.getSiblingDB(DATABASE_NAME);
    const factsCollections = factsDb.getCollectionInfos({ name: FACTS_COLLECTION });

    // Параметры создания коллекции для производственной среды
    const productionCreateOptions = {
        validator: factsSchema,
        /* Замедляет работу
        clusteredIndex: {
            key: { "_id": 1 },
            unique: true,
            name: "facts clustered key" 
        },
        */
        validationLevel: "off",
        validationAction: "warn"
    };
    // Тестовая среда
    const testCreateOptions = {
        validator: factsSchema,
        validationLevel: "strict",
        validationAction: "error"
    };
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
        factsDb.createCollection(FACTS_COLLECTION, productionCreateOptions);
        print("✓ Коллекция facts создана со схемой валидации");
    }
} catch (error) {
    print(`✗ Ошибка создания схемы для facts: ${error.message}`);
    hasError = true;
}

// 4.Индексы для коллекции facts
try {
    print(`\n4. Создание индексов для коллекции ${FACTS_COLLECTION}...`);
    const factsCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACTS_COLLECTION);
    const factsIndexes = factsCollection.getIndexes();
    print(`Существующие индексы для коллекции ${FACTS_COLLECTION}:`);
    factsIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
    const indexesToCreate = [
        // Вспомогательный индекс для подсчета статистики
        {
            key: { c: -1 },
            options: {
                name: 'idx_c',
                background: true,
                unique: false
            },
            shardIndex: false
        }
    ];
    for (const indexSpec of indexesToCreate) {
        if (factsIndexes.find(index => JSON.stringify(index.key) === JSON.stringify(indexSpec.key)) === undefined) {
            factsCollection.createIndex(indexSpec.key, indexSpec.options);
            print(`✓ Индекс ${indexSpec.options.name} создан`);
        } else {
            print(`✓ Индекс ${indexSpec.options.name} уже существует`);
        }
    }

    const lastFactsIndexes = factsCollection.getIndexes();
    print(`Итоговые индексы для коллекции ${FACTS_COLLECTION}:`);
    lastFactsIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
} catch (error) {
    print(`✗ Ошибка создания индексов для ${FACTS_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 5. Настройка шардирования для коллекции facts
print(`\n5. Настройка шардирования для коллекции ${FACTS_COLLECTION}...`);
const factsShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACTS_COLLECTION}`,
        key: { _id: 1 }, // Шардирование по полю _id
        unique: true
    },
    `Настройка шардирования для коллекции ${FACTS_COLLECTION} по ключу {_id: 1}`
);

if (!factsShardingResult.success) {
    print(`Ошибка: Не удалось настроить шардирование для коллекции ${FACTS_COLLECTION}`);
    quit(1);
}

// 6. Создание схемы для коллекции factIndex
try {
    print(`\n6. Создание схемы валидации для коллекции ${FACT_INDEX_COLLECTION}...`);

    const factIndexSchema = {
        $jsonSchema: {
            bsonType: "object",
            title: "Схема для коллекции индексных значений фактов",
            description: "Схема для коллекции индексных значений фактов",
            required: ["_id", "d", "c"],
            properties: {
                _id: {
                    bsonType: "object",
                    properties: {
                        h: {
                            bsonType: "string",
                            description: "Хеш значение <тип индексного значения>:<значение поля факта>"
                        },
                        f: {
                            bsonType: "string",
                            description: "Уникальный идентификатор факта в коллекции facts._id"
                        },
                    }
                },
                d: {
                    bsonType: "date",
                    description: "Дата факта"
                },
                c: {
                    bsonType: "date",
                    description: "Дата и время создания индексного значения"
                },
                // @deprecated нужно удалить после отладки
                t: {
                    bsonType: "int",
                    minimum: 1,
                    description: "Тип факта - целое число >= 1"
                },
                v: {
                    bsonType: "string",
                    description: "Индексное значение поля факта"
                },
                it: {
                    bsonType: "int",
                    minimum: 1,
                    description: "Тип индексного значения - целое число >= 1"
                }
            },
            additionalProperties: false
        }
    };

    const factIndexDb = db.getSiblingDB(DATABASE_NAME);
    const factIndexCollections = factIndexDb.getCollectionInfos({ name: FACT_INDEX_COLLECTION });

    // Параметры создания коллекции для производственной среды
    const productionCreateOptions = {
        validator: factIndexSchema,
        /* Замедляет работу
        clusteredIndex: {
            key: { "_id": 1 },
            unique: true,
            name: "factIndex clustered key"
        },
        */
        validationLevel: "off",
        validationAction: "warn"
    };
    // Тестовая среда
    const testCreateOptions = {
        validator: factIndexSchema,
        validationLevel: "strict",
        validationAction: "error"
    };
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
        factIndexDb.createCollection(FACT_INDEX_COLLECTION, productionCreateOptions);
        print(`✓ Коллекция ${FACT_INDEX_COLLECTION} создана со схемой валидации`);
    }
} catch (error) {
    print(`✗ Ошибка создания схемы для ${FACT_INDEX_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 7. Индексы для коллекции factIndex
try {
    print(`\n7. Создание индексов для коллекции ${FACT_INDEX_COLLECTION}...`);
    const factIndexCollection = db.getSiblingDB(DATABASE_NAME).getCollection(FACT_INDEX_COLLECTION);
    const factIndexIndexes = factIndexCollection.getIndexes();
    print(`Существующие индексы для коллекции ${FACT_INDEX_COLLECTION}:`);
    factIndexIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
    const indexesToCreate = [
        {
            key: { "_id.h": 1, "d": -1 },
            options: {
                name: 'idx_id_h_d',
                background: true
            }
        }
    ];
    for (const indexSpec of indexesToCreate) {
        if (factIndexIndexes.find(index => JSON.stringify(index.key) === JSON.stringify(indexSpec.key)) === undefined) {
            factIndexCollection.createIndex(indexSpec.key, indexSpec.options);
            print(`✓ Индекс ${indexSpec.options.name} создан`);
        } else {
            print(`✓ Индекс ${indexSpec.options.name} уже существует`);
        }
    }

    const lastFactIndexIndexes = factIndexCollection.getIndexes();
    print(`Итоговые индексы для коллекции ${FACT_INDEX_COLLECTION}:`);
    lastFactIndexIndexes.forEach(index => {
        print(`  ${index.name}: ${JSON.stringify(index.key)}`);
    });
} catch (error) {
    print(`✗ Ошибка создания индексов для ${FACT_INDEX_COLLECTION}: ${error.message}`);
    hasError = true;
}

// 8. Настройка шардирования для коллекции factIndex
print(`\n8. Настройка шардирования для коллекции ${FACT_INDEX_COLLECTION}...`);
const factIndexShardingResult = executeCommand(
    {
        shardCollection: `${DATABASE_NAME}.${FACT_INDEX_COLLECTION}`,
        key: { "_id.h": 1 }, // Может быть до 64Мб записей для разных фактов примерно 600000 фактов
        unique: false
    },
    `Настройка шардирования для коллекции ${FACT_INDEX_COLLECTION} по ключу {_id.h: 1}`
);

if (!factIndexShardingResult.success) {
    print(`Ошибка: Не удалось настроить шардирование для коллекции ${FACT_INDEX_COLLECTION}`);
    quit(1);
}

// 9. Создание зон шардирования
function CreateShardZones(databaseName) {
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
            print("ERROR: Can not add shard <" + shard + "> to tag: " + res.errmsg);
            return false;
        }
        print("Shard <" + shard + "> added to tag: " + shard);
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
            namespace: databaseName + ".facts",
            keys: [
                { _id: MinKey },
                { _id: hexToBase64("5555555555555555555555555555555555555555") },
                { _id: hexToBase64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") },
                { _id: MaxKey }
            ]
        },
        {
            namespace: databaseName + ".factIndex",
            keys: [
                { "_id.h": MinKey},
                { "_id.h": hexToBase64("5555555555555555555555555555555555555555")},
                { "_id.h": hexToBase64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")},
                { "_id.h": MaxKey},
            ]
        },
    ];
    print("*** TAGS ********************************************")
    ranges.forEach(range => {
        // Сначала удалим существующие диапазоны
        print(`Removing ${db.getSiblingDB("config").tags.find({ "ns": range.namespace }).count()} tag ranges for ${range.namespace}`);
        // Удаляем старые зоны коллекции
        db.getSiblingDB("config").tags.deleteMany({ "ns": range.namespace });

        // Затем добавляем новые диапазоны
        print("Adding tag ranges <" + range.namespace + ">");
        let count = 0;
        shards.forEach(shard => {
            const res = sh.addTagRange(range.namespace, range.keys[count], range.keys[count + 1], shard);
            if (!res.ok) {
                print("ERROR: Can not add tag range <" + count + "> for shard <" + shard + ">: " + res.errmsg);
                return false;
            }
            print("Tag range <" + count + "> for shard <" + shard + "> added successfully.");
            count++;
        });
    });
    sh.startBalancer();
    return true;
}

print("\n9. Создание зон шардирования...");
if (CreateShardZones(DATABASE_NAME)) {
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
