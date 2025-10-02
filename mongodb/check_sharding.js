// Скрипт для проверки статуса шардирования
// Использование: mongosh --file check_sharding.js

print("=== Проверка статуса шардирования ===");

// Конфигурация
const DATABASE_NAME = "test"; // Замените на нужное имя базы данных
const FACTS_COLLECTION = "facts";
const FACT_INDEX_COLLECTION = "factIndex";

// Функция для выполнения команд с обработкой ошибок
function executeCommand(command, description) {
    try {
        print(`Выполнение: ${description}`);
        const result = db.runCommand(command);
        if (result.ok === 1) {
            print(`✓ Успешно: ${description}`);
            return { success: true, result: result };
        } else {
            print(`✗ Ошибка: ${description} - ${result.errmsg || 'Неизвестная ошибка'}`);
            return { success: false, error: result.errmsg || 'Неизвестная ошибка' };
        }
    } catch (error) {
        print(`✗ Исключение: ${description} - ${error.message}`);
        return { success: false, error: error.message };
    }
}

// 1. Проверка списка шардов
print("\n1. Список шардов:");
const listShardsResult = executeCommand(
    { listShards: 1 },
    "Получение списка шардов"
);

if (listShardsResult.success && listShardsResult.result.shards) {
    listShardsResult.result.shards.forEach((shard, index) => {
        print(`  ${index + 1}. ${shard._id}: ${shard.host}`);
        if (shard.state) {
            print(`     Состояние: ${shard.state}`);
        }
    });
} else {
    print("  Не удалось получить список шардов");
}

// 2. Проверка состояния балансировщика
print("\n2. Состояние балансировщика:");
const balancerStateResult = executeCommand(
    { getBalancerState: 1 },
    "Проверка состояния балансировщика"
);

if (balancerStateResult.success) {
    print(`  Балансировщик: ${balancerStateResult.result.enabled ? 'включен' : 'отключен'}`);
} else {
    print("  Не удалось получить состояние балансировщика");
}

// 3. Проверка работы балансировщика
const balancerRunningResult = executeCommand(
    { isBalancerRunning: 1 },
    "Проверка работы балансировщика"
);

if (balancerRunningResult.success) {
    print(`  Балансировщик работает: ${balancerRunningResult.result.inBalancerRound ? 'да' : 'нет'}`);
} else {
    print("  Не удалось проверить работу балансировщика");
}

// 4. Общий статус шардирования
print("\n3. Общий статус шардирования:");
const shardingStatusResult = executeCommand(
    { shardingStatus: 1 },
    "Проверка общего статуса шардирования"
);

if (shardingStatusResult.success) {
    const status = shardingStatusResult.result;
    print(`  Версия: ${status.version || 'неизвестно'}`);
    print(`  Количество шардов: ${status.shards ? status.shards.length : 'неизвестно'}`);
    print(`  Количество баз данных: ${status.databases ? status.databases.length : 'неизвестно'}`);
    print(`  Количество коллекций: ${status.collections ? status.collections.length : 'неизвестно'}`);
} else {
    print("  Не удалось получить общий статус шардирования");
}

// 5. Статистика базы данных
print("\n4. Статистика базы данных:");
try {
    const dbStats = db.getSiblingDB(DATABASE_NAME).stats();
    print(`  База данных: ${DATABASE_NAME}`);
    print(`  Коллекций: ${dbStats.collections || 0}`);
    print(`  Документов: ${dbStats.objects || 0}`);
    print(`  Размер данных: ${(dbStats.dataSize / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Размер индексов: ${(dbStats.indexSize / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Размер хранилища: ${(dbStats.storageSize / 1024 / 1024).toFixed(2)} МБ`);
} catch (error) {
    print(`  Ошибка получения статистики базы данных: ${error.message}`);
}

// 6. Статистика коллекции facts
print("\n5. Статистика коллекции facts:");
try {
    const factsStats = db.getSiblingDB(DATABASE_NAME).getCollection(FACTS_COLLECTION).stats();
    print(`  Коллекция: ${FACTS_COLLECTION}`);
    print(`  Документов: ${factsStats.count || 0}`);
    print(`  Размер данных: ${(factsStats.size / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Размер индексов: ${(factsStats.totalIndexSize / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Шардирована: ${factsStats.sharded ? 'да' : 'нет'}`);
    if (factsStats.sharded && factsStats.shards) {
        print(`  Шарды:`);
        Object.keys(factsStats.shards).forEach(shardId => {
            const shardStats = factsStats.shards[shardId];
            print(`    ${shardId}: ${shardStats.count || 0} документов, ${(shardStats.size / 1024 / 1024).toFixed(2)} МБ`);
        });
    }
} catch (error) {
    print(`  Ошибка получения статистики коллекции facts: ${error.message}`);
}

// 7. Статистика коллекции factIndex
print("\n6. Статистика коллекции factIndex:");
try {
    const factIndexStats = db.getSiblingDB(DATABASE_NAME).getCollection(FACT_INDEX_COLLECTION).stats();
    print(`  Коллекция: ${FACT_INDEX_COLLECTION}`);
    print(`  Документов: ${factIndexStats.count || 0}`);
    print(`  Размер данных: ${(factIndexStats.size / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Размер индексов: ${(factIndexStats.totalIndexSize / 1024 / 1024).toFixed(2)} МБ`);
    print(`  Шардирована: ${factIndexStats.sharded ? 'да' : 'нет'}`);
    if (factIndexStats.sharded && factIndexStats.shards) {
        print(`  Шарды:`);
        Object.keys(factIndexStats.shards).forEach(shardId => {
            const shardStats = factIndexStats.shards[shardId];
            print(`    ${shardId}: ${shardStats.count || 0} документов, ${(shardStats.size / 1024 / 1024).toFixed(2)} МБ`);
        });
    }
} catch (error) {
    print(`  Ошибка получения статистики коллекции factIndex: ${error.message}`);
}

// 8. Проверка индексов
print("\n7. Индексы коллекций:");

// Индексы коллекции facts
try {
    print(`  Индексы коллекции ${FACTS_COLLECTION}:`);
    const factsIndexes = db.getSiblingDB(DATABASE_NAME).getCollection(FACTS_COLLECTION).getIndexes();
    factsIndexes.forEach((index, i) => {
        print(`    ${i + 1}. ${index.name}: ${JSON.stringify(index.key)}`);
    });
} catch (error) {
    print(`    Ошибка получения индексов facts: ${error.message}`);
}

// Индексы коллекции factIndex
try {
    print(`  Индексы коллекции ${FACT_INDEX_COLLECTION}:`);
    const factIndexIndexes = db.getSiblingDB(DATABASE_NAME).getCollection(FACT_INDEX_COLLECTION).getIndexes();
    factIndexIndexes.forEach((index, i) => {
        print(`    ${i + 1}. ${index.name}: ${JSON.stringify(index.key)}`);
    });
} catch (error) {
    print(`    Ошибка получения индексов factIndex: ${error.message}`);
}

print("\n=== Проверка статуса шардирования завершена ===");
