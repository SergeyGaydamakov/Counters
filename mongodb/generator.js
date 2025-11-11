// Скрипт для генерации случайных тестовых данных для коллекции factIndex
// Использование: mongosh --file generate_factIndex_test_data.js

print("=== Генерация тестовых данных для коллекции factIndex ===");

const DATABASE_NAME = "counters5"; // Замените на нужное имя базы данных
const COLLECTION_NAME = "factIndex";
const DOCUMENTS_COUNT = 1000; // Количество документов для генерации

// Функция для генерации случайной строки
function randomString(length = 10) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

// Функция для генерации случайной даты в диапазоне
function randomDate(start = new Date(2020, 0, 1), end = new Date()) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

// Функция для генерации случайного целого числа в диапазоне
function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Функция для генерации случайного объекта данных факта
function randomFactData() {
    const dataTypes = ['string', 'number', 'boolean', 'object'];
    const dataType = dataTypes[Math.floor(Math.random() * dataTypes.length)];
    
    switch (dataType) {
        case 'string':
            return { value: randomString(15) };
        case 'number':
            return { value: randomInt(1, 10000) };
        case 'boolean':
            return { value: Math.random() > 0.5 };
        case 'object':
            return {
                value: randomString(10),
                nested: {
                    field1: randomString(5),
                    field2: randomInt(1, 100)
                }
            };
        default:
            return { value: randomString(10) };
    }
}

// Функция для генерации хеша (имитация base64 хеша или строка формата "it:value")
function generateHash(indexType, value) {
    // С вероятностью 50% генерируем хеш, иначе строку формата "it:value"
    if (Math.random() > 0.5) {
        // Имитация base64 хеша (20 байт = 28 символов base64)
        const randomBytes = [];
        for (let i = 0; i < 20; i++) {
            randomBytes.push(Math.floor(Math.random() * 256));
        }
        return Buffer.from(randomBytes).toString('base64');
    } else {
        // Строка формата "it:value"
        return `${indexType}:${String(value)}`;
    }
}

// Подключение к базе данных
const db = db.getSiblingDB(DATABASE_NAME);
const collection = db.getCollection(COLLECTION_NAME);

print(`Подключение к базе данных: ${DATABASE_NAME}`);
print(`Коллекция: ${COLLECTION_NAME}`);
print(`Количество документов для генерации: ${DOCUMENTS_COUNT}`);

// Очистка коллекции (опционально, раскомментируйте если нужно)
// print("\nОчистка коллекции...");
// collection.deleteMany({});
// print("✓ Коллекция очищена");

// Генерация документов
print("\nГенерация документов...");
const documents = [];
const startTime = new Date();

for (let i = 0; i < DOCUMENTS_COUNT; i++) {
    const indexType = randomInt(1, 10); // Тип индексного значения (1-10)
    const factType = randomInt(1, 20); // Тип факта (1-20)
    const factId = randomString(24); // Идентификатор факта (24 символа, как ObjectId)
    const fieldValue = randomString(15); // Значение поля факта
    const factDate = randomDate(new Date(2020, 0, 1), new Date()); // Дата факта
    const createDate = new Date(); // Дата создания
    
    // Генерация хеша
    const hash = generateHash(indexType, fieldValue);
    
    // Создание документа согласно схеме
    const document = {
        _id: hash + ":"+factDate.getTime().toString(16),
        h: hash,
        f: factId,
        dt: factDate,
        c: createDate,
        // Опциональные поля
        it: indexType,
        v: fieldValue,
        t: factType
    };
    
    // С вероятностью 30% добавляем поле d с данными факта
    if (Math.random() < 0.3) {
        document.d = randomFactData();
    }
    
    documents.push(document);
    
    // Показываем прогресс каждые 100 документов
    if ((i + 1) % 100 === 0) {
        print(`  Сгенерировано ${i + 1} документов...`);
    }
}

print(`✓ Генерация завершена. Время: ${new Date() - startTime} мс`);

// Вставка документов в коллекцию
print("\nВставка документов в коллекцию...");
const insertStartTime = new Date();

try {
    // Вставляем пакетами по 100 документов для лучшей производительности
    const batchSize = 100;
    let inserted = 0;
    
    for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        const result = collection.insertMany(batch, { ordered: false });
        inserted += result.insertedIds ? Object.keys(result.insertedIds).length : batch.length;
        
        if ((i + batchSize) % 500 === 0 || i + batchSize >= documents.length) {
            print(`  Вставлено ${inserted} из ${documents.length} документов...`);
        }
    }
    
    const insertTime = new Date() - insertStartTime;
    print(`✓ Вставка завершена. Время: ${insertTime} мс`);
    print(`✓ Всего вставлено документов: ${inserted}`);
    
    // Проверка количества документов в коллекции
    const totalCount = collection.countDocuments();
    print(`✓ Всего документов в коллекции: ${totalCount}`);
    
} catch (error) {
    print(`✗ Ошибка при вставке: ${error.message}`);
    if (error.writeErrors) {
        print(`  Количество ошибок: ${error.writeErrors.length}`);
        error.writeErrors.slice(0, 5).forEach(err => {
            print(`    - ${err.errmsg}`);
        });
    }
}

print("\n=== Генерация тестовых данных завершена ===");