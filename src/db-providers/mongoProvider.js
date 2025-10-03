const { MongoClient, ObjectId } = require('mongodb');
const Logger = require('../utils/logger');

/**
 * Класс-провайдер для работы с MongoDB коллекциями facts и factIndex
 * Содержит методы для управления подключением, схемами, вставкой данных, запросами и статистикой
 */
class MongoProvider {
    constructor(connectionString, databaseName) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;

        // Создаем логгер для этого провайдера
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this.FACT_COLLECTION_NAME = "facts";
        this.FACT_INDEX_COLLECTION_NAME = "factIndex";
        this.factsClient = null;
        this.factsDb = null;
        this.factsCollection = null;
        this.factIndexClient = null;
        this.factIndexDb = null;
        this.factIndexCollection = null;
        this.adminClient = null;
        this.adminDb = null;
        this.isConnected = false;
    }

    // ============================================================================
    // ГРУППА 1: УПРАВЛЕНИЕ ПОДКЛЮЧЕНИЕМ
    // ============================================================================

    /**
     * Подключение к MongoDB
     * Создается 3 подключения для параллельной работы
     * @returns {Promise<boolean>} результат подключения
     */
    async connect() {
        try {
            this.logger.debug(`Три подключения к MongoDB: ${this.connectionString}`);
            this.factsClient = new MongoClient(this.connectionString);
            await this.factsClient.connect();
            this.factsDb = this.factsClient.db(this.databaseName);
            this.factsCollection = this.factsDb.collection(this.FACT_COLLECTION_NAME);

            this.factIndexClient = new MongoClient(this.connectionString);
            await this.factIndexClient.connect();
            this.factIndexDb = this.factIndexClient.db(this.databaseName);
            this.factIndexCollection = this.factIndexDb.collection(this.FACT_INDEX_COLLECTION_NAME);

            this.adminClient = new MongoClient(this.connectionString);
            await this.adminClient.connect();
            this.adminDb = this.adminClient.db('admin');

            this.isConnected = true;

            this.logger.debug(`✓ Успешно подключен к базе данных: ${this.databaseName}`);
            this.logger.debug(`✓ Используется коллекция: ${this.FACT_COLLECTION_NAME}`);
            this.logger.debug(`✓ Используется коллекция индексных значений: ${this.FACT_INDEX_COLLECTION_NAME}`);
        } catch (error) {
            this.logger.error('✗ Ошибка подключения к MongoDB:', error.message);
            this.isConnected = false;
            return false;
        }
        try {
            await this.createDatabase();
        } catch (error) {
            this.logger.error('✗ Ошибка при создании базы данных:', error.message);
            this.isConnected = false;
            return false;
        }
        return true;
    }

    /**
     * Отключение от MongoDB
     */
    async disconnect() {
        try {
            if (this.factsClient && this.isConnected) {
                await this.factsClient.close();
                this.factsClient = null;
                this.factsDb = null;
                this.factsCollection = null;
            }
            if (this.factIndexClient && this.isConnected) {
                await this.factIndexClient.close();
                this.factIndexClient = null;
                this.factIndexDb = null;
                this.factIndexCollection = null;
            }
            if (this.adminClient && this.isConnected) {
                await this.adminClient.close();
                this.adminClient = null;
                this.adminDb = null;
            }
            this.isConnected = false;
            this.logger.debug('✓ Соединение с MongoDB закрыто');
        } catch (error) {
            this.logger.error('✗ Ошибка при закрытии соединения:', error.message);
        }
    }

    /**
     * Проверка подключения
     * @returns {boolean} статус подключения
     */
    checkConnection() {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB. Вызовите connect() перед использованием методов.');
        }
        return true;
    }

    // ============================================================================
    // ГРУППА 2: УПРАВЛЕНИЕ СХЕМАМИ И ВАЛИДАЦИЕЙ
    // ============================================================================

    /**
     * Создает схему валидации для коллекции facts на основе структуры JSON
     * Схема определяет структуру документов и типы полей
     * @param {number} maxFieldCount - максимальное количество динамических полей f1, f2, ... (по умолчанию 23)
     * @returns {Promise<boolean>} результат создания схемы
     */
    async createFactsCollectionSchema(maxFieldCount = 23) {
        this.checkConnection();

        try {
            // Создаем объект для динамических полей f1, f2, ..., fN
            const dynamicFieldsProperties = {};
            for (let i = 1; i <= maxFieldCount; i++) {
                dynamicFieldsProperties[`f${i}`] = {
                    bsonType: "string",
                    description: `Динамическое поле f${i} - строка`
                };
            }

            // Определяем схему валидации JSON для коллекции facts
            const schema = {
                $jsonSchema: {
                    bsonType: "object",
                    title: "Facts Collection Schema",
                    description: "Схема для коллекции фактов с автоматически генерируемой структурой",
                    required: ["i", "t", "a", "c", "d"],
                    properties: {
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
                        },
                        ...dynamicFieldsProperties
                    },
                    additionalProperties: false
                }
            };

            // Проверяем, существует ли коллекция
            const collections = await this.factsDb.listCollections({ name: this.FACT_COLLECTION_NAME }).toArray();

            if (collections.length > 0) {
                // Коллекция существует, обновляем схему валидации
                this.logger.debug(`Обновление схемы валидации для существующей коллекции ${this.FACT_COLLECTION_NAME}...`);
                await this.factsDb.command({
                    collMod: this.FACT_COLLECTION_NAME,
                    validator: schema,
                    validationLevel: "moderate", // moderate - валидация только для новых документов и обновлений
                    validationAction: "warn" // warn - предупреждения вместо ошибок при нарушении схемы
                });
            } else {
                // Коллекция не существует, создаем с валидацией
                this.logger.debug(`Создание новой коллекции ${this.FACT_COLLECTION_NAME} со схемой валидации...`);
                await this.factsDb.createCollection(this.FACT_COLLECTION_NAME, {
                    validator: schema,
                    validationLevel: "moderate",
                    validationAction: "warn"
                });
            }

            this.logger.debug(`✓ Схема валидации для коллекции ${this.FACT_COLLECTION_NAME} успешно создана/обновлена`);
            this.logger.debug(`✓ Поддерживается до ${maxFieldCount} динамических полей (f1-f${maxFieldCount})`);

            return true;
        } catch (error) {
            console.error('✗ Ошибка при создании схемы коллекции:', error.message);
            return false;
        }
    }

    /**
     * Получает информацию о схеме коллекции facts
     * @returns {Promise<Object|null>} информация о схеме валидации или null
     */
    async getFactsCollectionSchema() {
        this.checkConnection();

        try {
            const collectionInfo = await this.factsDb.listCollections({ name: this.FACT_COLLECTION_NAME }).toArray();

            if (collectionInfo.length === 0) {
                this.logger.debug(`Коллекция ${this.FACT_COLLECTION_NAME} не существует`);
                return null;
            }

            const schema = collectionInfo[0].options?.validator;
            if (schema) {
                this.logger.debug(`✓ Схема валидации для коллекции ${this.FACT_COLLECTION_NAME} найдена`);
                return schema;
            } else {
                this.logger.warn(`⚠ Коллекция ${this.FACT_COLLECTION_NAME} существует, но схема валидации не настроена`);
                return null;
            }
        } catch (error) {
            console.error('✗ Ошибка при получении схемы коллекции:', error.message);
            throw error;
        }
    }


    /**
     * Получает схему коллекции индексных значений
     * @returns {Promise<Object>} схема коллекции
     */
    async getFactIndexCollectionSchema() {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const sample = await this.factIndexCollection.findOne({});

            if (!sample) {
                this.logger.debug(`В коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME} пусто!`);
                return {
                    collectionName: this.FACT_INDEX_COLLECTION_NAME,
                    isEmpty: true,
                    fields: [],
                    message: 'Коллекция пуста'
                };
            }

            const fields = Object.keys(sample).map(fieldName => {
                const value = sample[fieldName];
                return {
                    name: fieldName,
                    type: typeof value,
                    example: value,
                    isArray: Array.isArray(value),
                    isDate: value instanceof Date,
                    isObjectId: value instanceof ObjectId
                };
            });

            const schema = {
                collectionName: this.FACT_INDEX_COLLECTION_NAME,
                isEmpty: false,
                totalFields: fields.length,
                fields: fields,
                sampleDocument: sample
            };

            this.logger.debug(`\n=== Схема коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME} ===`);
            this.logger.debug(`Поля (${fields.length}):`);
            fields.forEach(field => {
                this.logger.debug(`  - ${field.name}: ${field.type}${field.isArray ? '[]' : ''}${field.isDate ? ' (Date)' : ''}${field.isObjectId ? ' (ObjectId)' : ''}`);
            });

            return schema;
        } catch (error) {
            console.error('✗ Ошибка при получении схемы коллекции индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Создает схему валидации для коллекции индексных значений
     * @returns {Promise<boolean>} результат создания схемы
     */
    async createFactIndexCollectionSchema() {
        this.checkConnection();

        try {
            // Определяем схему валидации JSON для коллекции factIndex
            const schema = {
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
                        // @deprecated нужно удалить после отладки
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

            // Проверяем, существует ли коллекция
            const collections = await this.factIndexDb.listCollections({ name: this.FACT_INDEX_COLLECTION_NAME }).toArray();

            if (collections.length > 0) {
                // Коллекция существует, обновляем схему валидации
                this.logger.debug(`Обновление схемы валидации для существующей коллекции ${this.FACT_INDEX_COLLECTION_NAME}...`);
                await this.factIndexDb.command({
                    collMod: this.FACT_INDEX_COLLECTION_NAME,
                    validator: schema,
                    validationLevel: "moderate",
                    validationAction: "warn"
                });
            } else {
                // Коллекция не существует, создаем с валидацией
                this.logger.debug(`Создание новой коллекции ${this.FACT_INDEX_COLLECTION_NAME} со схемой валидации...`);
                await this.factIndexDb.createCollection(this.FACT_INDEX_COLLECTION_NAME, {
                    validator: schema,
                    validationLevel: "moderate",
                    validationAction: "warn"
                });
            }

            this.logger.debug(`✓ Схема валидации для коллекции ${this.FACT_INDEX_COLLECTION_NAME} успешно создана/обновлена`);

            return true;
        } catch (error) {
            console.error('✗ Ошибка при создании схемы коллекции индексных значений:', error.message);
            return false;
        }
    }

    // ============================================================================
    // ГРУППА 3: ВСТАВКА ДАННЫХ
    // ============================================================================

    /**
     * Сохраняет один JSON факт в коллекцию facts используя updateOne в режиме upsert
     * @param {Object} fact - объект факта для сохранения
     * @returns {Promise<Object>} результат сохранения
     */
    async saveFact(fact) {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        if (!fact || typeof fact !== 'object') {
            throw new Error('fact должен быть объектом');
        }

        try {
            const filter = { i: fact.i };
            const updateOperation = { $set: fact };

            // Используем updateOne с upsert для оптимальной производительности
            const result = await this.factsCollection.updateOne(
                filter,
                updateOperation,
                { upsert: true }
            );

            // Определяем тип операции на основе результата updateOne
            const wasInserted = result.upsertedCount > 0;
            const wasUpdated = result.modifiedCount > 0;
            const wasIgnored = result.matchedCount > 0 && result.modifiedCount === 0;

            // Получаем ID документа
            let factId = null;
            if (wasInserted && result.upsertedId) {
                factId = result.upsertedId;
            } else if (wasUpdated || wasIgnored) {
                // Для обновленных или проигнорированных документов получаем ID через дополнительный запрос
                // Это единственный способ получить ID существующего документа
                const doc = await this.factsCollection.findOne(filter, { projection: { _id: 1 } });
                factId = doc?._id;
            }

            if (wasInserted) {
                this.logger.debug(`✓ Вставлен новый факт: ${factId}`);
            } else if (wasUpdated) {
                this.logger.debug(`✓ Обновлен существующий факт: ${factId}`);
            } else if (wasIgnored) {
                this.logger.debug(`✓ Проигнорирован дубликат факта: ${factId}`);
            }

            return {
                success: true,
                factId: factId,
                factInserted: wasInserted ? 1 : 0,
                factUpdated: wasUpdated ? 1 : 0,
                factIgnored: wasIgnored ? 1 : 0,
                result: result
            };

        } catch (error) {
            console.error('✗ Ошибка при upsert операции факта:', error.message);

            return {
                success: false,
                factId: null,
                factInserted: 0,
                factUpdated: 0,
                factIgnored: 0,
                error: error.message,
                result: null
            };
        }
    }

    /**
     * Сохраняет индексные значения в коллекцию используя bulk операции
     * @param {Array<Object>} factIndexValues - массив индексных значений
     * @returns {Promise<Object>} результат сохранения
     */
    async saveFactIndexList(factIndexValues) {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        if (!Array.isArray(factIndexValues) || factIndexValues.length === 0) {
            throw new Error('factIndexValues должен быть непустым массивом');
        }

        try {
            this.logger.debug(`Начинаем обработку ${factIndexValues.length} индексных значений...`);

            // Bulk вставка индексных значений с обработкой дубликатов
            const indexBulk = this.factIndexCollection.initializeUnorderedBulkOp();

            factIndexValues.forEach(indexValue => {
                const indexFilter = {
                    h: indexValue.h,
                    i: indexValue.i
                };
                indexBulk.find(indexFilter).upsert().updateOne({ $set: indexValue });
            });

            const indexResult = await indexBulk.execute();

            const inserted = indexResult.upsertedCount || 0;
            const updated = indexResult.modifiedCount || 0;
            const duplicatesIgnored = factIndexValues.length - inserted - updated;

            this.logger.debug(`✓ Обработано ${factIndexValues.length} индексных значений в коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);
            this.logger.debug(`  - Вставлено новых: ${inserted}`);
            this.logger.debug(`  - Обновлено существующих: ${updated}`);
            this.logger.debug(`  - Проигнорировано дубликатов: ${duplicatesIgnored}`);

            if (indexResult.writeErrors && indexResult.writeErrors.length > 0) {
                console.warn(`⚠ Ошибок при обработке: ${indexResult.writeErrors.length}`);
            }

            return {
                success: true,
                totalProcessed: factIndexValues.length,
                inserted: inserted,
                updated: updated,
                duplicatesIgnored: duplicatesIgnored,
                errors: indexResult.writeErrors || [],
                uniqueFields: ['h', 'i']
            };

        } catch (error) {
            console.error('✗ Критическая ошибка при вставке индексных значений:', error.message);

            return {
                success: false,
                totalProcessed: factIndexValues.length,
                inserted: 0,
                updated: 0,
                duplicatesIgnored: 0,
                errors: [{ error: error.message }],
                error: error.message
            };
        }
    }

    // ============================================================================
    // ГРУППА 5: УПРАВЛЕНИЕ ИНДЕКСАМИ
    // ============================================================================

    /**
     * Создание зон шардирования
     * @returns {Promise<boolean>} результат создания зон шардирования  
     */
    async createShardZones() {
        this.checkConnection();
        try {
            // Получение списка шардов
            const result = await this.adminDb.command({ createShardZones: this.databaseName });
            this.logger.debug('✓ Зоны шардирования созданы: ' + result.ok);
            return result.ok;
        } catch (error) {
            this.logger.error('✗ Ошибка при создании зон шардирования:', error.message);
            return false;
        }
        return true;
    }

    /**
     * Проверяет работает ли база в режиме шардирования
     * @returns {Promise<boolean>} результат проверки
     */
    async isShardingMode() {
        try {
            var result = this.adminDb.command({ listShards: 1 });
            return result.ok == 1;
        } catch (e) {
            return false;
        }
    }

    /**
     * Проверяет является ли база данных шардированной
     * @returns {Promise<boolean>} результат проверки
     */
    isShardingEnabled(databaseName) {
        return false;
    }


    /**
     * Включает шардирование для базы данных
     * @returns {Promise<boolean>} результат включения шардирования
     */
    async enableSharding(databaseName) {
        /*
        if (this.isShardingEnabled(databaseName)) {
            return true;
        }
        */
        try {
            if (!this.isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }
            const result = await this.adminDb.command({ enableSharding: databaseName });
            this.logger.debug('✓ Включено шардирование для базы данных: ' + databaseName + ' ' + result.ok);
            return result.ok;
        } catch (error) {
            this.logger.error('✗ Ошибка при включении шардирования для базы данных:', error.message);
            return false;
        }
    }


    /**
     * Шардирует коллекцию
     * @param {string} collectionName - имя коллекции
     * @param {Object} shardKey - ключ шардирования
     * @param {boolean} unique - уникален ли ключ шардирования
     * @param {Object} options - дополнительные опции
     * @returns {Promise<boolean>} результат создания индексов
     */
    async shardCollection(collectionName, shardKey, unique, options) {
        try {
            if (!this.isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }
            const shardCollectionCommand = {
                shardCollection: `${this.databaseName}.${collectionName}`,
                key: shardKey
            };

            if (unique !== undefined) {
                Object.assign(shardCollectionCommand, { unique: !!unique });
            }
            if (options) {
                Object.assign(shardCollectionCommand, options);
            }

            const result = await this.adminDb.command(shardCollectionCommand);

        } catch (error) {
            this.logger.error('✗ Ошибка при шардировании коллекции ${collectionName}: ', error.message);
            return false;
        }
        return true;
    }

    /**
     * Создает индексы для коллекции индексных значений
     * @returns {Promise<boolean>} результат создания индексов
     */
    async createFactIndexIndexes() {
        try {
            if (!this.isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }

            const indexesToCreate = [
                // Уникальный составной индекс по h, i
                {
                    key: { h: 1, i: 1 },
                    options: {
                        name: 'idx_h_i_unique',
                        background: true,
                        unique: true
                    },
                    shardIndex: true
                },
                // Составной индекс по h (хешированный), -d, i (для всех основных запросов)
                {
                    key: { h: 1, d: -1, i: 1 },
                    options: {
                        name: 'idx_h_d_i',
                        background: true
                    }
                }
            ];

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME}...`);

            for (const indexSpec of indexesToCreate) {
                try {
                    await this.factIndexCollection.createIndex(indexSpec.key, indexSpec.options);
                    if (indexSpec.shardIndex) {
                        await this.shardCollection(this.FACT_INDEX_COLLECTION_NAME, indexSpec.key, indexSpec.options.unique);
                        this.logger.info(`✓ Для коллекции ${this.FACT_INDEX_COLLECTION_NAME} создан шардированный индекс: ${indexSpec.options.name}`);
                    } else {
                        this.logger.debug(`✓ Создан индекс: ${indexSpec.options.name}`);
                    }
                    successCount++;
                } catch (error) {
                    // Если индекс уже существует, это не ошибка
                    if (error.code === 85 || error.message.includes('already exists')) {
                        this.logger.warn(`⚠ Индекс ${indexSpec.options.name} уже существует`);
                        successCount++;
                    } else {
                        console.error(`✗ Ошибка создания индекса ${indexSpec.options.name}:`, error.message);
                        errors.push({ index: indexSpec.options.name, error: error.message });
                    }
                }
            }

            this.logger.debug(`\n=== Результат создания индексов для индексных значений ===`);
            this.logger.debug(`✓ Успешно создано/проверено: ${successCount}/${indexesToCreate.length} индексов`);

            if (errors.length > 0) {
                this.logger.error(`✗ Ошибок: ${errors.length}`);
                errors.forEach(err => this.logger.error(`  - ${err.index}: ${err.error}`));
            }

            return errors.length === 0;
        } catch (error) {
            console.error('✗ Ошибка при создании индексов для индексных значений:', error.message);
            return false;
        }
    }

    /**
     * Создает индексы для коллекции facts
     * @returns {Promise<boolean>} результат создания индексов
     */
    async createFactIndexes() {
        try {
            if (!this.isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }

            const indexesToCreate = [
                // Уникальный индекс по полю i (GUID)
                {
                    key: { i: 1 },
                    options: {
                        name: 'idx_i_unique',
                        background: true,
                        unique: true
                    },
                    shardIndex: true
                },
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

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции фактов ${this.FACT_COLLECTION_NAME}...`);

            for (const indexSpec of indexesToCreate) {
                try {
                    await this.factsCollection.createIndex(indexSpec.key, indexSpec.options);
                    if (indexSpec.shardIndex) {
                        await this.shardCollection(this.FACT_COLLECTION_NAME, indexSpec.key, indexSpec.options.unique);
                        this.logger.info(`✓ Для коллекции ${this.FACT_COLLECTION_NAME} создан шардированный индекс: ${indexSpec.options.name}`);
                    } else {
                        this.logger.debug(`✓ Создан индекс: ${indexSpec.options.name}`);
                    }
                    successCount++;
                } catch (error) {
                    // Если индекс уже существует, это не ошибка
                    if (error.code === 85 || error.message.includes('already exists')) {
                        this.logger.warn(`⚠ Индекс ${indexSpec.options.name} уже существует`);
                        successCount++;
                    } else {
                        console.error(`✗ Ошибка создания индекса ${indexSpec.options.name}:`, error.message);
                        errors.push({ index: indexSpec.options.name, error: error.message });
                    }
                }
            }

            this.logger.debug(`\n=== Результат создания индексов для фактов ===`);
            this.logger.debug(`✓ Успешно создано/проверено: ${successCount}/${indexesToCreate.length} индексов`);

            if (errors.length > 0) {
                this.logger.error(`✗ Ошибок: ${errors.length}`);
                errors.forEach(err => this.logger.error(`  - ${err.index}: ${err.error}`));
            }

            return errors.length === 0;
        } catch (error) {
            console.error('✗ Ошибка при создании индексов для фактов:', error.message);
            return false;
        }
    }

    // ============================================================================
    // ГРУППА 6: СТАТИСТИКА И МОНИТОРИНГ
    // ============================================================================

    /**
     * Получает статистику использования коллекции facts
     * @returns {Promise<Object>} статистика коллекции
     */
    async getFactsCollectionStats() {
        this.checkConnection();

        try {
            const stats = await this.factsDb.command({ collStats: this.FACT_COLLECTION_NAME });

            const result = {
                namespace: stats.ns,
                documentCount: stats.count,
                avgDocumentSize: Math.round(stats.avgObjSize),
                totalSize: stats.size,
                storageSize: stats.storageSize,
                indexCount: stats.nindexes,
                totalIndexSize: stats.totalIndexSize,
                capped: stats.capped || false
            };

            this.logger.debug(`\n=== Статистика коллекции ${this.FACT_COLLECTION_NAME} ===`);
            this.logger.debug(`Документов: ${result.documentCount.toLocaleString()}`);
            this.logger.debug(`Средний размер документа: ${result.avgDocumentSize} байт`);
            this.logger.debug(`Общий размер данных: ${(result.totalSize / 1024 / 1024).toFixed(2)} МБ`);
            this.logger.debug(`Размер хранилища: ${(result.storageSize / 1024 / 1024).toFixed(2)} МБ`);
            this.logger.debug(`Количество индексов: ${result.indexCount}`);
            this.logger.debug(`Размер индексов: ${(result.totalIndexSize / 1024 / 1024).toFixed(2)} МБ`);

            return result;
        } catch (error) {
            console.error('✗ Ошибка при получении статистики коллекции:', error.message);
            throw error;
        }
    }

    /**
     * Получает статистику коллекции индексных значений
     * @returns {Promise<Object>} статистика коллекции
     */
    async getFactIndexStats() {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            // Используем один запрос collStats вместо двух отдельных запросов
            const stats = await this.factIndexDb.command({ collStats: this.FACT_INDEX_COLLECTION_NAME });

            const result = {
                collectionName: this.FACT_INDEX_COLLECTION_NAME,
                documentCount: stats.count || 0,
                avgDocumentSize: Math.round(stats.avgObjSize) || 0,
                totalSize: stats.size || 0,
                storageSize: stats.storageSize || 0,
                indexCount: stats.nindexes || 0,
                totalIndexSize: stats.totalIndexSize || 0
            };

            this.logger.debug(`\n=== Статистика коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME} ===`);
            this.logger.debug(`Документов: ${result.documentCount.toLocaleString()}`);
            this.logger.debug(`Средний размер документа: ${result.avgDocumentSize} байт`);
            this.logger.debug(`Общий размер данных: ${(result.totalSize / 1024 / 1024).toFixed(2)} МБ`);
            this.logger.debug(`Размер хранилища: ${(result.storageSize / 1024 / 1024).toFixed(2)} МБ`);
            this.logger.debug(`Количество индексов: ${result.indexCount}`);
            this.logger.debug(`Размер индексов: ${(result.totalIndexSize / 1024 / 1024).toFixed(2)} МБ`);

            return result;
        } catch (error) {
            console.error('✗ Ошибка при получении статистики коллекции индексных значений:', error.message);
            throw error;
        }
    }

    // ============================================================================
    // ГРУППА 7: ОЧИСТКА И УПРАВЛЕНИЕ ДАННЫМИ
    // ============================================================================

    /**
     * Очищает коллекцию фактов
     * @returns {Promise<Object>} результат очистки
     */
    async clearFactsCollection() {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const result = await this.factsCollection.deleteMany({});
            this.logger.debug(`✓ Удалено ${result.deletedCount} фактов из коллекции ${this.FACT_COLLECTION_NAME}`);
            return result;
        } catch (error) {
            console.error('✗ Ошибка при очистке коллекции фактов:', error.message);
            throw error;
        }
    }

    /**
     * Очищает коллекцию индексных значений
     * @returns {Promise<Object>} результат очистки
     */
    async clearFactIndexCollection() {
        if (!this.isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const result = await this.factIndexCollection.deleteMany({});
            this.logger.debug(`✓ Удалено ${result.deletedCount} индексных значений из коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);
            return result;
        } catch (error) {
            console.error('✗ Ошибка при очистке коллекции индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Проверка, что база данных создана
     * @returns {Promise<boolean>} результат проверки
     */
    async isDatabaseCreated(databaseName) {
        this.checkConnection();

        try {
            const listDatabases = await this.adminDb.command({ listDatabases: 1 });
            const database = listDatabases.databases.find(db => db.name === databaseName);
            // this.logger.debug(`База данных ${databaseName} ${database ? 'найдена' : 'не найдена'} ${database ? JSON.stringify(database) : ''}`);
            const result = database !== undefined;
            this.logger.debug(`База данных ${databaseName} ${result ? 'создана' : 'не создана'}`);
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при проверке, что база данных создана:', error.message);
            return false;
        }
    }

    /**
     * Создает полную схему базы данных с коллекциями, схемами валидации и индексами
     * @returns {Promise<Object>} результат создания базы данных
     */
    async createDatabase() {
        this.checkConnection();

        const results = {
            success: true,
            factsSchema: false,
            factIndexSchema: false,
            factsIndexes: false,
            factIndexIndexes: false,
            errors: []
        };

        if (await this.isDatabaseCreated(this.databaseName)) {
            this.logger.debug(`База данных ${this.databaseName} уже создана`);
            return results;
        }

        try {
            this.logger.debug('\n=== Создание базы данных ===');
            this.logger.debug(`База данных: ${this.databaseName}`);
            this.logger.debug(`Коллекция facts: ${this.FACT_COLLECTION_NAME}`);
            this.logger.debug(`Коллекция factIndex: ${this.FACT_INDEX_COLLECTION_NAME}`);

            // 0. Подготовка к созданию базы данных
            this.logger.debug('\n0. Подготовка к созданию базы данных...');

            // 1. Создание схемы для коллекции facts
            this.logger.debug('\n1. Создание схемы для коллекции facts...');
            try {
                results.factsSchema = await this.createFactsCollectionSchema(this.maxFieldCount);
                if (results.factsSchema) {
                    this.logger.debug('✓ Схема для коллекции facts создана успешно');
                } else {
                    results.errors.push('Не удалось создать схему для коллекции facts');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания схемы facts: ${error.message}`);
                console.error('✗ Ошибка создания схемы facts:', error.message);
            }

            // 2. Создание схемы для коллекции factIndex
            this.logger.debug('\n2. Создание схемы для коллекции factIndex...');
            try {
                results.factIndexSchema = await this.createFactIndexCollectionSchema();
                if (results.factIndexSchema) {
                    this.logger.debug('✓ Схема для коллекции factIndex создана успешно');
                } else {
                    results.errors.push('Не удалось создать схему для коллекции factIndex');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания схемы factIndex: ${error.message}`);
                console.error('✗ Ошибка создания схемы factIndex:', error.message);
            }

            // 3. Создание индексов для коллекции facts
            this.logger.debug('\n3. Создание индексов для коллекции facts...');
            try {
                results.factsIndexes = await this.createFactIndexes();
                if (results.factsIndexes) {
                    this.logger.debug('✓ Индексы для коллекции facts созданы успешно');
                } else {
                    results.errors.push('Не удалось создать индексы для коллекции facts');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания индексов facts: ${error.message}`);
                console.error('✗ Ошибка создания индексов facts:', error.message);
            }

            // 4. Создание индексов для коллекции factIndex
            this.logger.debug('\n4. Создание индексов для коллекции factIndex...');
            try {
                results.factIndexIndexes = await this.createFactIndexIndexes();
                if (results.factIndexIndexes) {
                    this.logger.debug('✓ Индексы для коллекции factIndex созданы успешно');
                } else {
                    results.errors.push('Не удалось создать индексы для коллекции factIndex');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания индексов factIndex: ${error.message}`);
                console.error('✗ Ошибка создания индексов factIndex:', error.message);
            }

            // Определяем общий успех
            results.success = results.errors.length === 0;

            this.logger.debug('\n=== Результат создания базы данных ===');
            this.logger.debug(`✓ Схема facts: ${results.factsSchema ? 'создана' : 'не создана'}`);
            this.logger.debug(`✓ Схема factIndex: ${results.factIndexSchema ? 'создана' : 'не создана'}`);
            this.logger.debug(`✓ Индексы facts: ${results.factsIndexes ? 'созданы' : 'не созданы'}`);
            this.logger.debug(`✓ Индексы factIndex: ${results.factIndexIndexes ? 'созданы' : 'не созданы'}`);

            if (results.errors.length > 0) {
                this.logger.warn(`⚠ Ошибок: ${results.errors.length}`);
                results.errors.forEach(error => this.logger.error(`  - ${error}`));
            } else {
                this.logger.info('✓ База данных создана успешно');
            }

            return results;

        } catch (error) {
            console.error('✗ Критическая ошибка при создании базы данных:', error.message);
            results.success = false;
            results.errors.push(`Критическая ошибка: ${error.message}`);
            return results;
        }
    }

    /**
     * Поиск фактов по заданному фильтру (для тестов)
     * @param {Object} filter - фильтр для поиска
     * @param {Object} options - опции поиска
     * @returns {Promise<Array>} найденные факты
     */
    async findFacts(filter = {}, options = {}) {
        try {
            const facts = await this.factsCollection.find(filter, options).toArray();
            this.logger.debug(`Найдено ${facts.length} фактов по фильтру:`, JSON.stringify(filter));
            return facts;
        } catch (error) {
            this.logger.error('✗ Ошибка при поиске фактов:', error.message);
            throw error;
        }
    }

    /**
     * Получает релевантные факты для заданного факта с целью вычисления счетчиков
     * @param {Object} fact - факт
     * @returns {Promise<Array>} релевантные факты
     */
    async getRelevantFacts(indexHashValues, factId = undefined, depthLimit = 1000, depthFromDate = undefined) {
        this.checkConnection();

        this.logger.debug(`Получение релевантных фактов для факта ${factId} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);
        // Сформировать агрегирующий запрос к коллекции factIndex,
        // получить уникальные значения поля i
        // и результат объединить с фактом из коллекции facts
        const matchQuery = {
            "h": {
                "$in": indexHashValues
            }
        };
        if (factId) {
            matchQuery.i = {
                "$ne": factId
            };
        }
        if (depthFromDate) {
            matchQuery.d = {
                "$gte": depthFromDate
            };
        }
        const factIndexResult = await this.factIndexCollection.find(matchQuery, { projection: { _id: 0, i: 1 } }).sort({ d: -1 }).limit(depthLimit).toArray();
        // Сформировать агрегирующий запрос к коллекции facts,
        const aggregateQuery = [
            {
                "$match": {
                    "i": {
                        "$in": factIndexResult.map(item => item.i)
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "i": 1
                }
            }
        ];

        // this.logger.debug(`Агрегационный запрос: ${JSON.stringify(aggregateQuery, null, 2)}`);

        // Выполнить агрегирующий запрос
        const result = await this.factsCollection.aggregate(aggregateQuery).toArray();
        this.logger.debug(`✓ Получено ${result.length} фактов`);
        // this.logger.debug(JSON.stringify(result, null, 2));
        // Возвращаем массив фактов
        return result;
    }

    /**
     * Получает счетчики по фактам для заданного факта
     * @param {Object} fact - факт
     * @returns {Promise<Array>} счетчики по фактам
     */
    async getRelevantFactCounters(indexHashValues, factId = undefined, depthLimit = 1000, depthFromDate = undefined) {
        this.checkConnection();

        this.logger.debug(`Получение релевантных фактов для факта ${factId} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);
        // Сформировать агрегирующий запрос к коллекции factIndex,
        // получить уникальные значения поля i
        // и результат объединить с фактом из коллекции facts
        const matchQuery = {
            "h": {
                "$in": indexHashValues
            }
        };
        if (factId) {
            matchQuery.i = {
                "$ne": factId
            };
        }
        if (depthFromDate) {
            matchQuery.d = {
                "$gte": depthFromDate
            };
        }
        const factIndexResult = await this.factIndexCollection.find(matchQuery, { projection: { _id: 0, i: 1 } }).sort({ d: -1 }).limit(depthLimit).toArray();
        
        // Если нет релевантных индексных значений, возвращаем пустую статистику
        if (factIndexResult.length === 0) {
            return [{
                total: [{ count: 0, sumA: 0 }],
                lastWeek: [{ count: 0, sumA: 0 }],
                lastHour: [{ count: 0, sumA: 0 }],
                lastDay: [{ count: 0, sumA: 0 }],
                conditionLastHour: [{ totalSum: 0 }]
            }];
        }
        
        // Сформировать агрегирующий запрос к коллекции facts,
        const queryFacts = {
            "$match": {
                "i": {
                    "$in": factIndexResult.map(item => item.i)
                }
            }
        };
        const statisticStageFacts = {
            "$facet": {
                "total": [
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$a" }
                        }
                    }
                ],
                "lastWeek": [
                    {
                        "$match": {
                            "d": {
                                "$gte": new Date(Date.now() - 1000 * 3600 * 24 * 7)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$a" }
                        }
                    }
                ],
                "lastHour": [
                    {
                        "$match": {
                            "d": {
                                "$gte": new Date(Date.now() - 1000 * 3600)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$a" }
                        }
                    }
                ],
                "lastDay": [
                    {
                        "$match": {
                            "d": {
                                "$gte": new Date(Date.now() - 1000 * 3600 * 24)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$a" }
                        }
                    }
                ],
                "conditionLastHour": [
                    {
                        "$match": {
                            "d": {
                                "$gte": new Date(Date.now() - 1000 * 3600)
                            },
                            "a": {
                                "$gt": 100000
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "totalSum": {
                                "$sum": "$a"
                            }
                        }
                    }
                ]
            }
        };

        const aggregateQuery = [queryFacts, statisticStageFacts];

        // this.logger.debug(`Агрегационный запрос: ${JSON.stringify(aggregateQuery, null, 2)}`);

        // Выполнить агрегирующий запрос
        const result = await this.factsCollection.aggregate(aggregateQuery).toArray();
        this.logger.debug(`✓ Получена статистика по фактам: ${JSON.stringify(result)} `);
        
        // Если результат пустой, возвращаем пустую статистику
        if (result.length === 0) {
            return [{
                total: [{ count: 0, sumA: 0 }],
                lastWeek: [{ count: 0, sumA: 0 }],
                lastHour: [{ count: 0, sumA: 0 }],
                lastDay: [{ count: 0, sumA: 0 }],
                conditionLastHour: [{ totalSum: 0 }]
            }];
        }
        
        // Возвращаем массив статистики
        return result;
    }
}

module.exports = MongoProvider;