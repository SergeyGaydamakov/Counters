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
        this._counterClient = null;
        this._counterDb = null;
        // Разные коллекции для работы в разных потоках
        this._updateFactsCollection = null;
        this._updateFactIndexCollection = null;
        this._findFactsCollection = null;
        this._findFactIndexCollection = null;
        //
        this._isConnected = false;
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
            this.logger.debug(`Подключение к MongoDB: ${this.connectionString}`);

            // Опции для подключения по умолчанию
            const defaultOptions = {
                readConcern: { level: "local" },
                readPreference: "primary",
                writeConcern: {
                    w: "majority",
                    j: true,
                    wtimeout: 5000
                },
                appName: "CounterTest",
                // monitorCommands: true,
                // minPoolSize: 10,
                // maxPoolSize: 100,
                // maxIdleTimeMS: 10000,
                // maxConnecting: 10,
                serverSelectionTimeoutMS: 30000,
            };
            this._counterClient = new MongoClient(this.connectionString, defaultOptions);
            await this._counterClient.connect();
            this._counterDb = this._counterClient.db(this.databaseName);
            this._updateFactsCollection = this._counterDb.collection(this.FACT_COLLECTION_NAME);
            this._updateFactIndexCollection = this._counterDb.collection(this.FACT_INDEX_COLLECTION_NAME);
            this._findFactsCollection = this._counterDb.collection(this.FACT_COLLECTION_NAME);
            this._findFactIndexCollection = this._counterDb.collection(this.FACT_INDEX_COLLECTION_NAME);

            this._isConnected = true;

            this.logger.debug(`✓ Успешное подключение к базе данных: ${this.databaseName}`);
            this.logger.debug(`✓ Коллекция фактов: ${this.FACT_COLLECTION_NAME}`);
            this.logger.debug(`✓ Коллекция индексных значений: ${this.FACT_INDEX_COLLECTION_NAME}`);
        } catch (error) {
            this.logger.error('✗ Ошибка подключения к MongoDB:', error.message);
            this._isConnected = false;
            return false;
        }
        try {
            await this.createDatabase();
        } catch (error) {
            this.logger.error('✗ Ошибка при создании базы данных:', error.message);
            this._isConnected = false;
            return false;
        }
        return true;
    }

    /**
     * Отключение от MongoDB
     */
    async disconnect() {
        try {
            if (this._counterClient && this._isConnected) {
                await this._counterClient.close();
                this.logger.debug('✓ Основное соединение с MongoDB закрыто');
            }

            // Очищаем все ссылки на объекты
            this._counterClient = null;
            this._counterDb = null;
            this._updateFactsCollection = null;
            this._updateFactIndexCollection = null;
            this._findFactsCollection = null;
            this._findFactIndexCollection = null;
            this._isConnected = false;

            this.logger.debug('✓ Все соединения с MongoDB закрыты');
        } catch (error) {
            this.logger.error('✗ Ошибка при закрытии соединения:', error.message);
            // Даже при ошибке сбрасываем флаги
            this._isConnected = false;
        }
    }

    /**
     * Проверка подключения
     * @returns {boolean} статус подключения
     */
    checkConnection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB. Вызовите connect() перед использованием методов.');
        }
        return true;
    }


    // ============================================================================
    // ГРУППА 2: РАБОТА С ДАННЫМИ ДЛЯ ПРИКЛАДНОГО КОДА
    // ============================================================================

    /**
     * Сохраняет один JSON факт в коллекцию facts используя updateOne в режиме upsert
     * @param {Object} fact - объект факта для сохранения
     * @returns {Promise<Object>} результат сохранения
     */
    async saveFact(fact) {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        if (!fact || typeof fact !== 'object') {
            throw new Error('fact должен быть объектом');
        }

        try {
            const filter = { _id: fact._id };
            const updateOperation = { $set: fact };

            // Используем updateOne с upsert для оптимальной производительности
            const updateOptions = {
                readConcern: { level: "local" },
                readPreference: "primary",
                writeConcern: {
                    w: "majority",
                    j: true,
                    wtimeout: 5000
                },
                comment: "saveFact",
                upsert: true,
            };
            const result = await this._updateFactsCollection.updateOne(
                filter,
                updateOperation,
                updateOptions
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
                const findOptions = {
                    readConcern: { level: "local" },
                    readPreference: "primary",
                    comment: "saveFact - find",
                    projection: { _id: 1 }
                };
                const doc = await this._updateFactsCollection.findOne(filter, findOptions);
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
            this.logger.error('✗ Ошибка при upsert операции факта:', error.message);

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
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        if (!Array.isArray(factIndexValues)) {
            throw new Error('factIndexValues должен быть массивом');
        }

        if (factIndexValues.length === 0) {
            return {
                success: true,
                totalProcessed: 0,
                inserted: 0,
                updated: 0,
                duplicatesIgnored: 0,
                errors: []
            };

        }

        try {
            this.logger.debug(`Начинаем обработку ${factIndexValues.length} индексных значений...`);

            // Bulk вставка индексных значений с обработкой дубликатов
            const bulkWriteOptions = {
                readConcern: { level: "local" },
                readPreference: "primary",
                writeConcern: {
                    w: "majority",
                    j: true,
                    wtimeout: 5000
                },
                comment: "saveFactIndexList",
                ordered: false,
                upsert: true,
            };
            const indexBulk = this._updateFactIndexCollection.initializeUnorderedBulkOp(bulkWriteOptions);

            factIndexValues.forEach(indexValue => {
                const indexFilter = {
                    _id: indexValue._id 
                };
                indexBulk.find(indexFilter).upsert().updateOne({ $set: indexValue });
                this.logger.debug("   indexValue: "+JSON.stringify(indexValue));
            });

            const indexResult = await indexBulk.execute(bulkWriteOptions);

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
                errors: indexResult.writeErrors || []
            };

        } catch (error) {
            this.logger.error('✗ Критическая ошибка при вставке индексных значений:', error.message);
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
        // получить уникальные значения поля _id
        // и результат объединить с фактом из коллекции facts
        const matchQuery = {
            "_id.h": {
                "$in": indexHashValues
            }
        };
        if (factId) {
            matchQuery["_id.f"] = {
                "$ne": factId
            };
        }
        if (depthFromDate) {
            matchQuery.d = {
                "$gte": depthFromDate
            };
        }
        const findOptions = {
            batchSize: 5000,
            readConcern: { level: "local" },
            readPreference: "secondaryPreferred",
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1 }
        };
        // this.logger.debug("   matchQuery: "+JSON.stringify(matchQuery));
        const factIndexResult = await this._findFactIndexCollection.find(matchQuery, findOptions).sort({ d: -1 }).batchSize(5000).limit(depthLimit).toArray();
        // Сформировать агрегирующий запрос к коллекции facts,
        const aggregateQuery = [
            {
                "$match": {
                    "_id": {
                        "$in": factIndexResult.map(item => item._id.f)
                    }
                }
            }
        ];

        // this.logger.debug(`Агрегационный запрос: ${JSON.stringify(aggregateQuery, null, 2)}`);

        // Выполнить агрегирующий запрос
        const aggregateOptions = {
            batchSize: 5000,
            readConcern: { level: "local" },
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };
        const result = await this._findFactsCollection.aggregate(aggregateQuery, aggregateOptions).batchSize(5000).toArray();
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
        // получить уникальные значения поля _id
        // и результат объединить с фактом из коллекции facts
        const matchQuery = {
            "_id.h": {
                "$in": indexHashValues
            }
        };
        if (factId) {
            matchQuery._id = {
                "$ne": factId
            };
        }
        if (depthFromDate) {
            matchQuery.d = {
                "$gte": depthFromDate
            };
        }
        const findOptions = {
            batchSize: 5000,
            readConcern: { level: "local" },
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1 }
        };
        const factIndexResult = await this._findFactIndexCollection.find(matchQuery, findOptions).sort({ d: -1 }).limit(depthLimit).toArray();

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
                "_id": {
                    "$in": factIndexResult.map(item => item._id.f)
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
                            "sumA": { "$sum": "$d.amount" }
                        }
                    }
                ],
                "lastWeek": [
                    {
                        "$match": {
                            "dt": {
                                "$gte": new Date(Date.now() - 1000 * 3600 * 24 * 7)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$d.amount" }
                        }
                    }
                ],
                "lastHour": [
                    {
                        "$match": {
                            "dt": {
                                "$gte": new Date(Date.now() - 1000 * 3600)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$d.amount" }
                        }
                    }
                ],
                "lastDay": [
                    {
                        "$match": {
                            "dt": {
                                "$gte": new Date(Date.now() - 1000 * 3600 * 24)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": null,
                            "count": { "$sum": 1 },
                            "sumA": { "$sum": "$d.amount" }
                        }
                    }
                ],
                "conditionLastHour": [
                    {
                        "$match": {
                            "dt": {
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
                                "$sum": "$d.amount"
                            }
                        }
                    }
                ]
            }
        };

        const aggregateQuery = [queryFacts, statisticStageFacts];

        // this.logger.debug(`Агрегационный запрос: ${JSON.stringify(aggregateQuery, null, 2)}`);

        // Выполнить агрегирующий запрос
        const aggregateOptions = {
            batchSize: 5000,
            readConcern: { level: "local" },
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };
        const result = await this._findFactsCollection.aggregate(aggregateQuery, aggregateOptions).toArray();
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



    // ============================================================================
    // ГРУППА 3: РАБОТА С ДАННЫМИ ДЛЯ ТЕСТОВ
    // ============================================================================

    /**
     * Очищает коллекцию фактов
     * @returns {Promise<Object>} результат очистки
     */
    async clearFactsCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const deleteOptions = {
                readConcern: { level: "local" },
                readPreference: "primary",
                writeConcern: {
                    w: "majority",
                    j: true,
                    wtimeout: 5000
                },
                comment: "clearFactCollection",
            };
            const result = await this._updateFactsCollection.deleteMany({}, deleteOptions);
            this.logger.debug(`✓ Удалено ${result.deletedCount} фактов из коллекции ${this.FACT_COLLECTION_NAME}`);
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при очистке коллекции фактов:', error.message);
            throw error;
        }
    }

    /**
     * Подсчитывает количество документов в коллекции фактов
     * @returns {Promise<number>} количество документов
     */
    async countFactsCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const result = await this._updateFactsCollection.countDocuments();
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при подсчете числа документов в коллекции фактов:', error.message);
            throw error;
        }
    }

    /**
     * Поиск фактов по заданному фильтру (для тестов)
     * @param {Object} filter - фильтр для поиска
     * @returns {Promise<Array>} найденные факты
     */
    async findFacts(filter = {}) {
        try {
            const findOptions = {
                batchSize: 5000,
                readConcern: { level: "local" },
                readPreference: { mode: "secondaryPreferred" },
                comment: "findFacts"
            };
            const facts = await this._findFactsCollection.find(filter, findOptions).toArray();
            this.logger.debug(`Найдено ${facts.length} фактов по фильтру:`, JSON.stringify(filter));
            return facts;
        } catch (error) {
            this.logger.error('✗ Ошибка при поиске фактов:', error.message);
            throw error;
        }
    }

    /**
     * Очищает коллекцию индексных значений
     * @returns {Promise<Object>} результат очистки
     */
    async clearFactIndexCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const deleteOptions = {
                readConcern: { level: "local" },
                readPreference: "primary",
                writeConcern: {
                    w: "majority",
                    j: true,
                    wtimeout: 5000
                },
                comment: "clearFactIndexCollection",
            };
            const result = await this._updateFactIndexCollection.deleteMany({}, deleteOptions);
            this.logger.debug(`✓ Удалено ${result.deletedCount} индексных значений из коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при очистке коллекции индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Подсчитывает количество документов в коллекции фактов
     * @returns {Promise<number>} количество документов
     */
    async countFactIndexCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const result = await this._updateFactIndexCollection.countDocuments();
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при подсчете числа документов в коллекции индексных значений:', error.message);
            throw error;
        }
    }

    // ============================================================================
    // ГРУППА 4: СОЗДАНИЕ И УПРАВЛЕНИЕ БАЗОЙ ДАННЫХ
    // ============================================================================

    /**
     * Проверяет работает ли база в режиме шардирования
     * @returns {Promise<boolean>} результат проверки
     */
    async _isShardingMode(adminDb) {
        try {
            var result = await adminDb.command({ listShards: 1 });
            if (result.ok == 1) {
                this.logger.debug(`✓ База данных ${this.databaseName} работает в режиме шардирования. Шарды: ${result.shards.map(shard => shard._id).join(', ')}`);
                return result.shards.length > 0;
            } else {
                throw new Error(result.message);
            }
        } catch (e) {
            this.logger.error(`✗ Ошибка при проверке, что база данных работает в режиме шардирования: ${e.message}. Считаем, что режим шардирования не включен.`);
            throw e;
        }
    }

    /**
     * Включает шардирование для базы данных
     * @returns {Promise<boolean>} результат включения шардирования
     */
    async _enableSharding(adminDb, databaseName) {
        if (!this._isShardingMode(adminDb)) {
            return true;
        }
        try {
            if (!this._isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }
            const result = await adminDb.command({ enableSharding: databaseName });
            this.logger.debug('✓ Включено шардирование для базы данных: ' + databaseName + ' ' + result.ok);
            return result.ok;
        } catch (error) {
            this.logger.error('✗ Ошибка при включении шардирования для базы данных:', error.message);
            throw error;
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
    async _shardCollection(adminDb, collectionName, shardKey, unique, options) {
        try {

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

            const result = await adminDb.command(shardCollectionCommand);
            return true;
        } catch (error) {
            this.logger.error(`✗ Ошибка при шардировании коллекции ${collectionName}: ${error.message}`);
            throw error;
        }
    }


    /**
     * Создает схему валидации для коллекции facts на основе структуры JSON
     * Схема определяет структуру документов и типы полей
     * @returns {Promise<boolean>} результат создания схемы
     */
    async _createFactsCollectionSchema() {
        this.checkConnection();

        try {

            // Определяем схему валидации JSON для коллекции facts
            const schema = {
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

            // Проверяем, существует ли коллекция
            const collections = await this._counterDb.listCollections({ name: this.FACT_COLLECTION_NAME }).toArray();

            if (collections.length > 0) {
                // Коллекция существует, обновляем схему валидации
                this.logger.debug(`Обновление схемы валидации для существующей коллекции ${this.FACT_COLLECTION_NAME}...`);
                await this._counterDb.command({
                    collMod: this.FACT_COLLECTION_NAME,
                    validator: schema,
                    validationLevel: "moderate", // moderate - валидация только для новых документов и обновлений
                    validationAction: "warn" // warn - предупреждения вместо ошибок при нарушении схемы
                });
            } else {
                // Коллекция не существует, создаем с валидацией
                this.logger.debug(`Создание новой коллекции ${this.FACT_COLLECTION_NAME} со схемой валидации...`);
                // Параметры создания коллекции для производственной среды
                const productionCreateOptions = {
                    validator: schema,
                    clusteredIndex: {
                        key: { "_id": 1 },
                        unique: true
                    },
                    validationLevel: "off",
                    validationAction: "warn"
                };
                // Тестовая среда
                const testCreateOptions = {
                    validator: schema,
                    validationLevel: "strict",
                    validationAction: "error"
                };
                await this._counterDb.createCollection(this.FACT_COLLECTION_NAME, testCreateOptions);
            }

            this.logger.debug(`✓ Схема валидации для коллекции ${this.FACT_COLLECTION_NAME} успешно создана/обновлена`);

            return true;
        } catch (error) {
            this.logger.error('✗ Ошибка при создании схемы коллекции:', error.message);
            return false;
        }
    }

    /**
     * Создает индексы для коллекции facts
     * @returns {Promise<boolean>} результат создания индексов
     */
    async _createFactIndexes(adminDb) {
        try {
            if (!this._isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }

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

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции фактов ${this.FACT_COLLECTION_NAME}...`);

            for (const indexSpec of indexesToCreate) {
                try {
                    await this._updateFactsCollection.createIndex(indexSpec.key, indexSpec.options);
                    this.logger.debug(`✓ Создан индекс: ${indexSpec.options.name}`);
                    successCount++;
                } catch (error) {
                    // Если индекс уже существует, это не ошибка
                    if (error.code === 85 || error.message.includes('already exists')) {
                        this.logger.warn(`⚠ Индекс ${indexSpec.options.name} уже существует`);
                        successCount++;
                    } else {
                        this.logger.error(`✗ Ошибка создания индекса ${indexSpec.options.name}:`, error.message);
                        errors.push({ index: indexSpec.options.name, error: error.message });
                    }
                }
            }

            await this._shardCollection(adminDb, this.FACT_COLLECTION_NAME, { _id: 1 }, true);
            this.logger.info(`✓ Выполнено шардирование коллекции ${this.FACT_COLLECTION_NAME}`);

            this.logger.debug(`\n=== Результат создания индексов для фактов ===`);
            this.logger.debug(`✓ Успешно создано/проверено: ${successCount}/${indexesToCreate.length} индексов`);

            if (errors.length > 0) {
                this.logger.error(`✗ Ошибок: ${errors.length}`);
                errors.forEach(err => this.logger.error(`  - ${err.index}: ${err.error}`));
            }

            return errors.length === 0;
        } catch (error) {
            this.logger.error('✗ Ошибка при создании индексов для фактов:', error.message);
            return false;
        }
    }

    /**
     * Получает информацию о схеме коллекции facts
     * @returns {Promise<Object|null>} информация о схеме валидации или null
     */
    async _getFactsCollectionSchema() {
        this.checkConnection();

        try {
            const collectionInfo = await this._counterDb.listCollections({ name: this.FACT_COLLECTION_NAME }).toArray();

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
            this.logger.error('✗ Ошибка при получении схемы коллекции:', error.message);
            throw error;
        }
    }



    /**
     * Создает схему валидации для коллекции индексных значений
     * @returns {Promise<boolean>} результат создания схемы
     */
    async _createFactIndexCollectionSchema() {
        this.checkConnection();

        try {
            // Определяем схему валидации JSON для коллекции factIndex
            const schema = {
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

            // Проверяем, существует ли коллекция
            const collections = await this._counterDb.listCollections({ name: this.FACT_INDEX_COLLECTION_NAME }).toArray();

            if (collections.length > 0) {
                // Коллекция существует, обновляем схему валидации
                this.logger.debug(`Обновление схемы валидации для существующей коллекции ${this.FACT_INDEX_COLLECTION_NAME}...`);
                await this._counterDb.command({
                    collMod: this.FACT_INDEX_COLLECTION_NAME,
                    validator: schema,
                    validationLevel: "moderate",
                    validationAction: "warn"
                });
            } else {
                // Коллекция не существует, создаем с валидацией
                this.logger.debug(`Создание новой коллекции ${this.FACT_INDEX_COLLECTION_NAME} со схемой валидации...`);
                // Параметры создания коллекции для производственной среды
                const productionCreateOptions = {
                    validator: schema,
                    clusteredIndex: {
                        key: { "_id": 1, "d": 1 },
                        unique: true
                    },
                    validationLevel: "off",
                    validationAction: "warn"
                };
                // Тестовая среда
                const testCreateOptions = {
                    validator: schema,
                    validationLevel: "strict",
                    validationAction: "error"
                };
                await this._counterDb.createCollection(this.FACT_INDEX_COLLECTION_NAME, testCreateOptions);
            }

            this.logger.debug(`✓ Схема валидации для коллекции ${this.FACT_INDEX_COLLECTION_NAME} успешно создана/обновлена`);

            return true;
        } catch (error) {
            this.logger.error('✗ Ошибка при создании схемы коллекции индексных значений:', error.message);
            return false;
        }
    }

    /**
     * Создает индексы для коллекции индексных значений
     * @returns {Promise<boolean>} результат создания индексов
     */
    async _createFactIndexIndexes(adminDb) {
        try {
            if (!this._isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }

            const indexesToCreate = [
                {
                    key: { "_id.h": 1, "d": -1 },
                    options: {
                        name: 'idx_id_h_d',
                        background: true
                    }
                }
            ];

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME}...`);

            for (const indexSpec of indexesToCreate) {
                try {
                    await this._updateFactIndexCollection.createIndex(indexSpec.key, indexSpec.options);
                    this.logger.debug(`✓ Создан индекс: ${indexSpec.options.name}`);
                    successCount++;
                } catch (error) {
                    // Если индекс уже существует, это не ошибка
                    if (error.code === 85 || error.message.includes('already exists')) {
                        this.logger.warn(`⚠ Индекс ${indexSpec.options.name} уже существует`);
                        successCount++;
                    } else {
                        this.logger.error(`✗ Ошибка создания индекса ${indexSpec.options.name}:`, error.message);
                        errors.push({ index: indexSpec.options.name, error: error.message });
                    }
                }
            }
            // Шардирование после создания индексов, чтобы не создавался шардированный индекс
            await this._shardCollection(adminDb, this.FACT_INDEX_COLLECTION_NAME, { "_id.h": 1 }, false);
            this.logger.info(`✓ Выполнено шардирование коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);

            this.logger.debug(`\n=== Результат создания индексов для индексных значений ===`);
            this.logger.debug(`✓ Успешно создано/проверено: ${successCount}/${indexesToCreate.length} индексов`);

            if (errors.length > 0) {
                this.logger.error(`✗ Ошибок: ${errors.length}`);
                errors.forEach(err => this.logger.error(`  - ${err.index}: ${err.error}`));
            }

            return errors.length === 0;
        } catch (error) {
            this.logger.error('✗ Ошибка при создании индексов для индексных значений:', error.message);
            return false;
        }
    }

    /**
     * Получает схему коллекции индексных значений
     * @returns {Promise<Object>} схема коллекции
     */
    async _getFactIndexCollectionSchema() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            const sample = await this._updateFactIndexCollection.findOne({});

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
            this.logger.error('✗ Ошибка при получении схемы коллекции индексных значений:', error.message);
            throw error;
        }
    }


    /**
     * Проверка, что база данных создана
     * @returns {Promise<boolean>} результат проверки
     */
    async _isDatabaseCreated(adminDb, databaseName) {
        this.checkConnection();

        try {
            const listDatabases = await adminDb.command({ listDatabases: 1 });
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

        let adminClient = null;
        try {
            adminClient = new MongoClient(this.connectionString);
            await adminClient.connect();
            const adminDb = adminClient.db('admin');


            const results = {
                success: true,
                factsSchema: false,
                factIndexSchema: false,
                factsIndexes: false,
                factIndexIndexes: false,
                errors: []
            };

            if (await this._isDatabaseCreated(adminDb, this.databaseName)) {
                this.logger.debug(`База данных ${this.databaseName} уже создана`);
                return results;
            }

            this.logger.debug('\n=== Создание базы данных ===');
            this.logger.debug(`База данных: ${this.databaseName}`);
            this.logger.debug(`Коллекция facts: ${this.FACT_COLLECTION_NAME}`);
            this.logger.debug(`Коллекция factIndex: ${this.FACT_INDEX_COLLECTION_NAME}`);

            // 0. Подготовка к созданию базы данных
            this.logger.debug('\n0. Подготовка к созданию базы данных...');
            this._enableSharding(adminDb, this.databaseName);

            // 1. Создание схемы для коллекции facts
            this.logger.debug('\n1. Создание схемы для коллекции facts...');
            try {
                results.factsSchema = await this._createFactsCollectionSchema();
                if (results.factsSchema) {
                    this.logger.debug('✓ Схема для коллекции facts создана успешно');
                } else {
                    results.errors.push('Не удалось создать схему для коллекции facts');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания схемы facts: ${error.message}`);
                this.logger.error('✗ Ошибка создания схемы facts:', error.message);
            }

            // 2. Создание схемы для коллекции factIndex
            this.logger.debug('\n2. Создание схемы для коллекции factIndex...');
            try {
                results.factIndexSchema = await this._createFactIndexCollectionSchema();
                if (results.factIndexSchema) {
                    this.logger.debug('✓ Схема для коллекции factIndex создана успешно');
                } else {
                    results.errors.push('Не удалось создать схему для коллекции factIndex');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания схемы factIndex: ${error.message}`);
                this.logger.error('✗ Ошибка создания схемы factIndex:', error.message);
            }

            // 3. Создание индексов для коллекции facts
            this.logger.debug('\n3. Создание индексов для коллекции facts...');
            try {
                results.factsIndexes = await this._createFactIndexes(adminDb);
                if (results.factsIndexes) {
                    this.logger.debug('✓ Индексы для коллекции facts созданы успешно');
                } else {
                    results.errors.push('Не удалось создать индексы для коллекции facts');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания индексов facts: ${error.message}`);
                this.logger.error('✗ Ошибка создания индексов facts:', error.message);
            }

            // 4. Создание индексов для коллекции factIndex
            this.logger.debug('\n4. Создание индексов для коллекции factIndex...');
            try {
                results.factIndexIndexes = await this._createFactIndexIndexes(adminDb);
                if (results.factIndexIndexes) {
                    this.logger.debug('✓ Индексы для коллекции factIndex созданы успешно');
                } else {
                    results.errors.push('Не удалось создать индексы для коллекции factIndex');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания индексов factIndex: ${error.message}`);
                this.logger.error('✗ Ошибка создания индексов factIndex:', error.message);
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
            this.logger.error('✗ Критическая ошибка при создании базы данных:', error.message);
            results.success = false;
            results.errors.push(`Критическая ошибка: ${error.message}`);
            return results;
        } finally {
            if (adminClient) {
                try {
                    await adminClient.close();
                } catch (closeError) {
                    this.logger.error('✗ Ошибка при закрытии adminClient:', closeError.message);
                }
            }
        }
    }

    // ============================================================================
    // ГРУППА 5: СТАТИСТИКА И МОНИТОРИНГ
    // ============================================================================

    /**
     * Получает статистику использования коллекции facts
     * @returns {Promise<Object>} статистика коллекции
     */
    async getFactsCollectionStats() {
        this.checkConnection();

        try {
            const stats = await this._counterDb.command({ collStats: this.FACT_COLLECTION_NAME });

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
            this.logger.error('✗ Ошибка при получении статистики коллекции:', error.message);
            throw error;
        }
    }

    /**
     * Получает статистику коллекции индексных значений
     * @returns {Promise<Object>} статистика коллекции
     */
    async getFactIndexStats() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            // Используем один запрос collStats вместо двух отдельных запросов
            const stats = await this._counterDb.command({ collStats: this.FACT_INDEX_COLLECTION_NAME });

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
            this.logger.error('✗ Ошибка при получении статистики коллекции индексных значений:', error.message);
            throw error;
        }
    }


}

module.exports = MongoProvider;