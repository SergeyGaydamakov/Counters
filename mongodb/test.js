function createTestCollection() {
    print("Создание тестовой коллекции...");

    const DATABASE_NAME = "test";
    const TEST_COLLECTION = "test";

    try {
        const testSchema = {
            $jsonSchema: {
                bsonType: "object",
                title: "Test Collection Schema",
                description: "Тестовая схема",
                required: ["_id", "v"],
                properties: {
                    _id: {
                        bsonType: "object",
                        description: "Уникальный идентификатор факта в коллекции facts._id",
                        properties: {
                            h: {
                                bsonType: "string",
                                description: "Хеш значение типа + поля факта"
                            },
                            f: {
                                bsonType: "string",
                                description: "Уникальный идентификатор факта в коллекции test._id"
                            }
                        }
                    },
                    v: {
                        bsonType: "string",
                        description: "значение поля факта"
                    }
                },
                additionalProperties: false
            }
        };

        const testDb = db.getSiblingDB(DATABASE_NAME);
        const testCollections = testDb.getCollectionInfos({ name: TEST_COLLECTION });
        print("testCollections: " + JSON.stringify(testCollections));

        if (testCollections.length > 0) {
            print(`✓ Коллекция ${TEST_COLLECTION} уже существует. Удалите коллекцию перед созданием.`);
            throw new Error("Коллекция уже существует");
        }
        // Коллекция не существует, создаем с валидацией
        print("Коллекция не существует, создаем с валидацией");
        print("testSchema: " + JSON.stringify(testSchema));
        testDb.createCollection(TEST_COLLECTION, {
            validator: testSchema,
            validationLevel: "moderate",
            validationAction: "warn"
        });
        print(`✓ Коллекция ${TEST_COLLECTION} создана со схемой валидации`);
        testDb.test.createIndex({ "_id.h": 1, "v": 1 }, { name: "idx_h_v", unique: false });
        print(`✓ Индекс idx_h_v создан`);
        sh.shardCollection(DATABASE_NAME + "." + TEST_COLLECTION, { "_id.h": 1 });
        print(`✓ Коллекция ${TEST_COLLECTION} шардирована`);
        // Удаляем старые зоны коллекции
        db.getSiblingDB("config").tags.deleteMany({ "ns": DATABASE_NAME + "." + TEST_COLLECTION });
        [{
            tag: "rs01",
            min: { "_id.h": MinKey },
            max: { "_id.h": "test2" }
        }, {
            tag: "rs02",
            min: { "_id.h": "test2" },
            max: { "_id.h": "test3" }
        }, {
            tag: "rs03",
            min: { "_id.h": "test3" },
            max: { "_id.h": MaxKey }
        }].forEach(range => {
            sh.addTagRange(DATABASE_NAME + "." + TEST_COLLECTION, range.min, range.max, range.tag);
        });
        print(`✓ Созданы зоны для коллекции ${TEST_COLLECTION}`);
    } catch (error) {
        print(`✗ Ошибка создания схемы для ${TEST_COLLECTION}: ${error.message}`);
        hasError = true;
    }
}

function createTestData() {
    print("Создание тестовых данных...");

    const DATABASE_NAME = "test";
    const TEST_COLLECTION = "test";

    const testDb = db.getSiblingDB(DATABASE_NAME);

    testDb.test.insertOne({
        _id: {
            h: "test1",
            f: "test1"
        },
        v: "test1-1"
    });
    testDb.test.insertOne({
        _id: {
            h: "test1",
            f: "test2"
        },
        v: "test1-2"
    });
    testDb.test.insertOne({
        _id: {
            h: "test1",
            f: "test3"
        },
        v: "test1-3"
    });
    testDb.test.insertOne({
        _id: {
            h: "test2",
            f: "test1"
        },
        v: "test2-1"
    });
    testDb.test.insertOne({
        _id: {
            h: "test2",
            f: "test2"
        },
        v: "test2-2"
    });
    testDb.test.insertOne({
        _id: {
            h: "test2",
            f: "test3"
        },
        v: "test2-3"
    });
    testDb.test.insertOne({
        _id: {
            h: "test3",
            f: "test1"
        },
        v: "test3-1"
    });
    testDb.test.insertOne({
        _id: {
            h: "test3",
            f: "test2"
        },
        v: "test3-2"
    });
    testDb.test.insertOne({
        _id: {
            h: "test3",
            f: "test3"
        },
        v: "test3-3"
    });
}

function test() {
    db.test.find({ "_id.h": "test1" }).explain();
}

