/**
 * MongoDB Node Driver:
 * https://www.mongodb.com/docs/drivers/node/current/
 */
const { MongoClient, ObjectId } = require('mongodb');
const Logger = require('../utils/logger');

let _mongoClient = null;

/**
 * Класс-провайдер для работы с MongoDB коллекциями facts и factIndex
 * Содержит методы для управления подключением, схемами, вставкой данных, запросами и статистикой
 */
class MongoProvider {
    // Читаем всегда локальную копию данных
    READ_CONCERN = { level: "local" };
    // Журнал сбрасывается на диск в соответствии с политикой журналирования сервера (раз в 100 мс)
    // https://www.mongodb.com/docs/manual/core/journaling/#std-label-journal-process
    // Параметр на сервере: storage.journal.commitIntervalMs
    // https://www.mongodb.com/docs/manual/reference/configuration-options/#mongodb-setting-storage.journal.commitIntervalMs
    WRITE_CONCERN = { w: "majority", j: false, wtimeout: 5000 };

    /**
     * Конструктор MongoProvider
     * @param {string} connectionString - Строка подключения к MongoDB
     * @param {string} databaseName - Имя базы данных
     * @param {Object} counterProducer - Объект для создания счетчиков, должен иметь метод make(fact)
     * @param {boolean} includeFactDataToIndex - Включать данные факта в индексное значение
     * @throws {Error} если mongoCounters не соответствует требуемому интерфейсу
     * 
     * Требования к mongoCounters:
     * - Должен быть объектом (не null/undefined)
     * - Должен иметь метод make(fact)
     * - Метод make должен принимать объект факта и возвращать объект
     * - Возвращаемый объект должен содержать только массивы в качестве значений (для $facet)
     */
    constructor(connectionString, databaseName, counterProducer, includeFactDataToIndex) {
        // Создаем логгер для этого провайдера
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

        this.connectionString = connectionString;
        this.databaseName = databaseName;
        this.includeFactDataToIndex = includeFactDataToIndex; // Включать данные факта в индексное значение

        // Проверяем интерфейс mongoCounters перед присваиванием
        this._counterProducer = this._validateMongoCountersInterface(counterProducer);

        this.FACT_COLLECTION_NAME = "facts";
        this.FACT_INDEX_COLLECTION_NAME = "factIndex";
        this.LOG_COLLECTION_NAME = "log";
        this._counterClient = null;
        this._counterDb = null;
        this._isConnected = false;
    }

    /**
     * Проверяет интерфейс класса mongoCounters на наличие требуемых методов
     * @param {Object} mongoCounters - Объект mongoCounters для проверки
     * @throws {Error} если mongoCounters не соответствует требуемому интерфейсу
     */
    _validateMongoCountersInterface(mongoCounters) {
        if (!mongoCounters) {
            // Будем работать по умолчанию
            this.logger.warn('mongoProvider.mongoCounters не заданы. Счетчики не будут создаваться.');
            return null;
        }

        if (typeof mongoCounters !== 'object') {
            throw new Error('mongoCounters должен быть объектом');
        }

        // Проверяем наличие метода make
        if (typeof mongoCounters.make !== 'function') {
            throw new Error('mongoCounters должен иметь метод make(fact)');
        }

        return mongoCounters;
    }

    // ============================================================================
    // ГРУППА 1: УПРАВЛЕНИЕ ПОДКЛЮЧЕНИЕМ
    // ============================================================================

    _getMongoClient(connectionString) {
        if (_mongoClient) {
            return _mongoClient;
        }
        // Опции для подключения по умолчанию
        const defaultOptions = {
            readConcern: this.READ_CONCERN,
            readPreference: "primary",
            writeConcern: this.WRITE_CONCERN,
            appName: "CounterTest",
            // monitorCommands: true,
            minPoolSize: 100,
            maxPoolSize: 800,
            maxIdleTimeMS: 60000,
            maxConnecting: 10,
            serverSelectionTimeoutMS: 60000,
        };
        try {
            _mongoClient = new MongoClient(connectionString, defaultOptions);
            return _mongoClient;
        } catch (error) {
            _mongoClient = null;
            throw error;
        }
    }

    /**
     * Подключение к MongoDB
     * Создается 3 подключения для параллельной работы
     * @returns {Promise<boolean>} результат подключения
     */
    async connect() {
        try {
            this.logger.debug(`Подключение к MongoDB: ${this.connectionString}`);

            this._counterClient = await this._getMongoClient(this.connectionString);
            await this._counterClient.connect();
            this._counterDb = this._counterClient.db(this.databaseName);

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

    /**
     * Получение объекта Collection для коллекции facts
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactsCollection() {
        if (!this._factsCollection) {
            this._factsCollection = this._counterDb.collection(this.FACT_COLLECTION_NAME);
        }
        return this._factsCollection;
    }

    /**
     * Получение объекта Collection для коллекции factIndex
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactIndexCollection() {
        if (!this._factIndexCollection) {
            this._factIndexCollection = this._counterDb.collection(this.FACT_INDEX_COLLECTION_NAME);
        }
        return this._factIndexCollection;
    }

    /**
     * Получение объекта Collection для коллекции log
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getLogCollection() {
        if (!this._logCollection) {
            this._logCollection = this._counterDb.collection(this.LOG_COLLECTION_NAME);
        }
        return this._logCollection;
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

        const startTime = Date.now();
        try {
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factsCollection = this._getFactsCollection();

            const filter = { _id: fact._id };
            const updateOperation = { $set: fact };

            // Используем updateOne с upsert для оптимальной производительности
            const updateOptions = {
                readConcern: this.READ_CONCERN,
                readPreference: "primary",
                writeConcern: this.WRITE_CONCERN,
                comment: "saveFact",
                upsert: true,
            };
            const result = await factsCollection.updateOne(
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
                    readConcern: this.READ_CONCERN,
                    readPreference: "primary",
                    comment: "saveFact - find",
                    projection: { _id: 1 }
                };
                const doc = await factsCollection.findOne(filter, findOptions);
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
                processingTime: Date.now() - startTime,
                result: result
            };

        } catch (error) {
            this.logger.error('✗ Ошибка при upsert операции факта:', error.message);
            throw error;
        }
    }

    /**
     * Сохраняет индексные значения в коллекцию используя bulk операции
     * @param {Array<Object>} factIndexValues - массив индексных значений
     * @returns {Promise<Object>} результат сохранения
     */
    async saveFactIndexList(factIndexValues, bulkUpdate = false) {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        if (!Array.isArray(factIndexValues)) {
            throw new Error('factIndexValues должен быть массивом');
        }

        const startTime = Date.now();
        if (factIndexValues.length === 0) {
            return {
                success: true,
                totalProcessed: 0,
                inserted: 0,
                updated: 0,
                duplicatesIgnored: 0,
                errors: [],
                processingTime: Date.now() - startTime,
                metrics: {
                    indexValuesCount: 0,
                }
            };

        }

        try {
            this.logger.debug(`Начинаем обработку ${factIndexValues.length} индексных значений...`);

            // Создаем локальную ссылку на коллекцию для этого запроса
            const factIndexCollection = this._getFactIndexCollection();

            let inserted = 0;
            let updated = 0;
            let duplicatesIgnored = 0;
            let indexResult;
            let writeErrors = [];
            let detailedMetrics = [];

            if (bulkUpdate) {
                // Bulk вставка индексных значений с обработкой дубликатов
                const bulkWriteOptions = {
                    readConcern: this.READ_CONCERN,
                    readPreference: "primary",
                    writeConcern: this.WRITE_CONCERN,
                    comment: "saveFactIndexList - bulk",
                    ordered: false,
                    upsert: true,
                };
                const indexBulk = factIndexCollection.initializeUnorderedBulkOp(bulkWriteOptions);

                factIndexValues.forEach(indexValue => {
                    const indexFilter = {
                        _id: indexValue._id
                    };
                    indexBulk.find(indexFilter).upsert().updateOne({ $set: indexValue });
                    this.logger.debug("   indexValue: " + JSON.stringify(indexValue));
                });

                indexResult = await indexBulk.execute(bulkWriteOptions);
                writeErrors = indexResult.writeErrors || [];

                inserted = indexResult.upsertedCount || 0;
                updated = indexResult.modifiedCount || 0;
                duplicatesIgnored = factIndexValues.length - inserted - updated;
            } else {
                const updateOptions = {
                    readConcern: this.READ_CONCERN,
                    readPreference: "primary",
                    writeConcern: this.WRITE_CONCERN,
                    comment: "saveFactIndexList - update",
                    upsert: true,
                };
                // Обновление индексных значений параллельно через Promises
                const updatePromises = factIndexValues.map(async (indexValue) => {
                    const startTime = Date.now();
                    try {
                        const result = await factIndexCollection.updateOne({ _id: indexValue._id }, { $set: indexValue }, updateOptions);
                        return {
                            writeError: result.writeErrors,
                            upsertedCount: result.upsertedCount || 0,
                            modifiedCount: result.modifiedCount || 0,
                            processingTime: Date.now() - startTime,
                            _id: indexValue._id
                        };
                    } catch (error) {
                        this.logger.error(`✗ Ошибка при обновлении индексного значения: ${error.message}`);
                        return {
                            writeError: error.message,
                            upsertedCount: 0,
                            modifiedCount: 0,
                            processingTime: Date.now() - startTime,
                            _id: indexValue._id
                        };
                    }
                });

                const updateResults = await Promise.all(updatePromises);
                writeErrors = updateResults.filter(result => result.writeError).map(result => result.writeError) || [];

                // Подсчитываем результаты
                updateResults.forEach(result => {
                    inserted += result.upsertedCount || 0;
                    updated += result.modifiedCount || 0;
                });
                detailedMetrics = updateResults.map(result => { return { _id: result._id, processingTime: result.processingTime } });
                duplicatesIgnored = factIndexValues.length - inserted - updated;
            }

            this.logger.debug(`✓ Обработано ${factIndexValues.length} индексных значений в коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);
            this.logger.debug(`  - Вставлено новых: ${inserted}`);
            this.logger.debug(`  - Обновлено существующих: ${updated}`);
            this.logger.debug(`  - Проигнорировано дубликатов: ${duplicatesIgnored}`);
            if (writeErrors && writeErrors.length > 0) {
                this.logger.warn(`⚠ Ошибок при обработке: ${writeErrors.length}: ${writeErrors.join(', \n')}`);
            }

            return {
                success: true,
                inserted: inserted,
                updated: updated,
                duplicatesIgnored: duplicatesIgnored,
                errors: writeErrors || [],
                processingTime: Date.now() - startTime,
                metrics: {
                    factIndexValuesCount: factIndexValues.length,
                    detailedMetrics: detailedMetrics,
                },
                debug: {
                    bulkUpdate: bulkUpdate,
                    factIndexValues: factIndexValues.map(item => item._id),
                }
            };

        } catch (error) {
            this.logger.error('✗ Критическая ошибка при вставке индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Выдает счетчики применительно к факту, с разбивкой по типам индексов
     * @param {Object} fact - факт
     * @returns {Promise<Array>} выражение для вычисления группы счетчиков для факта и типа индексов
     */
    getFactIndexCountersInfo(fact) {
        if (!this._counterProducer) {
            this.logger.warn('mongoProvider.mongoCounters не заданы. Счетчики будут создаваться по умолчанию.');
            return this.getDefaultFactIndexCountersInfo();
        }
        // Получение счетчиков, которые подходят для факта по условию computationConditions
        const factCounters = this._counterProducer.getFactCounters(fact);
        if (!factCounters) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }
        // this.logger.debug(`factCounters: ${JSON.stringify(factCounters)}`);
        // Список условий по каждому типу индекса
        const indexFacetStages = {};
        // Подставляем параметры для каждого счетчика
        factCounters.forEach(counter => {
            const matchStage = counter.evaluationConditions ? { "$match": counter.evaluationConditions } : null;
            const groupStage = { "$group": counter.attributes };
            // Всегда добавлять идентификатор, по которому выполняется группировка - _id = null
            groupStage["$group"]["_id"] = null;
            if (!indexFacetStages[counter.indexTypeName]) {
                indexFacetStages[counter.indexTypeName] = {};
            }
            indexFacetStages[counter.indexTypeName][counter.name] = [];
            if (matchStage) {
                indexFacetStages[counter.indexTypeName][counter.name].push(matchStage);
            }
            indexFacetStages[counter.indexTypeName][counter.name].push(groupStage);
        });

        // Если в выражении счетчиков есть параметры, то заменить их на значения атрибутов из факта
        // Например, если в выражении счетчиков есть параметр "$$f2", то он будет заменен на значение атрибута "f2" из факта
        const indexFacetStagesString = JSON.stringify(indexFacetStages);
        if (!indexFacetStagesString.includes('$$')) {
            return indexFacetStages;
        }
        // Создаем глубокую копию выражения счетчиков
        const parameterizedFacetStages = JSON.parse(indexFacetStagesString);
        // Рекурсивно заменяем переменные в объекте
        this._replaceParametersRecursive(parameterizedFacetStages, fact.d);

        return parameterizedFacetStages;
    }

    /**
     * Выдает выражение для вычисления счетчиков по умолчанию
     * @returns {Promise<Array>} выражение для вычисления счетчиков по умолчанию
     */
    getDefaultFactIndexCountersInfo() {
        return {
            "test_type_1": {
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
            },
            "test_type_2": {
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
    }

    /**
     * Выдает выражение для вычисления счетчиков по фактам
     * @param {Object} fact - факт
     * @returns {Promise<Array>} выражение для вычисления счетчиков по фактам
     */
    oldGetCountersInfo(fact) {
        if (!this._counterProducer) {
            this.logger.warn('mongoProvider.mongoCounters не заданы. Счетчики будут создаваться по умолчанию.');
            return this.getOldDefaultCountersInfo();
        }
        const countersInfo = this._counterProducer.make(fact);
        if (!countersInfo) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }

        // Замена параметров в выражении счетчиков на значения атрибутов из факта
        // Например, если в выражении счетчиков есть параметр "$$f2", то он будет заменен на значение атрибута "f2" из факта
        const facetStagesString = JSON.stringify(countersInfo.facetStages);
        if (!facetStagesString.includes('$$')) {
            return {
                facetStages: countersInfo.facetStages,
                indexTypeNames: countersInfo.indexTypeNames
            };
        }
        // Создаем глубокую копию выражения счетчиков
        const parameterizedFacetStages = JSON.parse(facetStagesString);
        // Рекурсивно заменяем переменные в объекте
        this._replaceParametersRecursive(parameterizedFacetStages, fact.d);

        return {
            facetStages: parameterizedFacetStages,
            indexTypeNames: countersInfo.indexTypeNames
        };
    }

    /**
     * Выдает выражение для вычисления счетчиков по умолчанию
     * @returns {Promise<Array>} выражение для вычисления счетчиков по умолчанию
     */
    getOldDefaultCountersInfo() {
        return {
            "facetStages": {
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
            },
            "indexTypeNames": ["test_type_1", "test_type_2", "test_type_3", "test_type_4", "test_type_5", "test_type_6", "test_type_7", "test_type_8", "test_type_9", "test_type_10"]
        };
    }

    /**
     * Заменяет переменные в выражении счетчиков на значения из факта
     * 
     * Пример использования:
     * Конфигурация счетчика с переменными:
     * {
     *   "name": "user_transactions",
     *   "condition": { "userId": "$$userId" },
     *   "aggregation": [
     *     { "$match": { "userId": "$$userId" } },
     *     { "$group": { "_id": null, "count": { "$sum": 1 } } }
     *   ]
     * }
     * 
     * Факт: { "d": { "userId": "12345", "amount": 100 } }
     * 
     * Результат замены:
     * {
     *   "condition": { "userId": "12345" },
     *   "aggregation": [
     *     { "$match": { "userId": "12345" } },
     *     { "$group": { "_id": null, "count": { "$sum": 1 } } }
     *   ]
     * }
     * 
     * Рекурсивно заменяет переменные в объекте MongoDB агрегации
     * @param {*} obj - объект для обработки
     * @param {Object} factData - данные факта для замены переменных
     */
    _replaceParametersRecursive(obj, factData) {
        if (obj === null || obj === undefined) {
            return;
        }

        const nowDate = new Date();

        if (Array.isArray(obj)) {
            // Если это массив, обрабатываем каждый элемент
            obj.forEach(item => this._replaceParametersRecursive(item, factData));
        } else if (typeof obj === 'object') {
            // Если это объект, обрабатываем каждое свойство
            for (const key in obj) {
                if (obj.hasOwnProperty(key)) {
                    const value = obj[key];

                    if (typeof value === 'string' && value.startsWith('$$')) {
                        // Если значение начинается с $$, это переменная
                        const variableName = value.substring(2); // убираем $$
                        if (variableName.toUpperCase() === 'NOW') {
                            obj[key] = nowDate;
                            this.logger.debug(`Заменена переменная ${value} на значение: ${nowDate}`);
                        } else if (factData.hasOwnProperty(variableName)) {
                            // Заменяем переменную на значение из факта
                            obj[key] = factData[variableName];
                            this.logger.debug(`Заменена переменная ${value} на значение: ${factData[variableName]}`);
                        } else {
                            this.logger.warn(`Переменная ${value} не найдена в данных факта`);
                        }
                    } else {
                        // Рекурсивно обрабатываем вложенные объекты
                        this._replaceParametersRecursive(value, factData);
                    }
                }
            }
        }
    }


    async _getRelevantFactsByIndex(indexNameQuery, debugMode = false) {
        const factIndexCollection = this._getFactIndexCollection();
        const startFindFactIndexTime = Date.now();
        const factIndexResult = await factIndexCollection.find(indexNameQuery.factIndexFindQuery, indexNameQuery.factIndexFindOptions).sort(indexNameQuery.factIndexFindSort).limit(indexNameQuery.depthLimit).toArray();
        const stopFindFactIndexTime = Date.now();
        const relevantFactsQuerySize = debugMode ? JSON.stringify(indexNameQuery.factIndexFindQuery).length : undefined;
        const relevantFactsSize = debugMode ? JSON.stringify(factIndexResult).length : undefined;
        const factIds = factIndexResult.map(item => item._id.f);
        this.logger.debug(`✓ Получены списки ИД фактов: ${JSON.stringify(factIds)} \n`);
        return {
            factIds: factIds,
            metrics: {
                relevantFactsQuerySize: relevantFactsQuerySize,
                relevantFactsTime: stopFindFactIndexTime - startFindFactIndexTime,
                relevantFactsCount: factIndexResult.length,
                relevantFactsSize: relevantFactsSize,
            },
            debug: {
                indexQuery: indexNameQuery.factIndexFindQuery,
                indexSort: indexNameQuery.factIndexFindSort,
                depthLimit: indexNameQuery.depthLimit,
            }
        };
    }

    async _getCounters(indexTypeName, factIds, indexCounterList, debugMode = false) {
        if (!factIds || !factIds.length) {
            return {
                counters: null,
                error: "No relevant facts found",
                metrics: {
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    countersQuerySize: 0,
                    countersSize: 0,
                },
                debug: {
                    countersQuery: null,
                }
            };
        }
        // Финальный этап агрегации для улучшения формата результатов: преобразование из массивов в объект
        const projectState = {};
        Object.keys(indexCounterList).forEach(counterName => {
            projectState[counterName] = { "$arrayElemAt": ["$" + counterName, 0] };
        });
        const factFacetStage = [
            {
                "$match": {
                    "_id": { "$in": factIds }
                }
            },
            { "$facet": indexCounterList },
            { "$project": projectState }
        ];
        const countersQuerySize = debugMode ? JSON.stringify(factFacetStage).length : undefined;
        // Выполнить агрегирующий запрос
        const aggregateFactOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(factFacetStage)}`);
        const factsCollection = this._getFactsCollection();
        const startCountersQueryTime = Date.now();
        try {
            this.logger.debug(`Агрегационный запрос для индекса ${indexTypeName}: ${JSON.stringify(factFacetStage)}`);
            const countersResult = await factsCollection.aggregate(factFacetStage, aggregateFactOptions).toArray();
            const countersSize = debugMode ? JSON.stringify(countersResult[0]).length : undefined;
            return {
                counters: countersResult[0],
                metrics: {
                    countersQuerySize: countersQuerySize,
                    countersQueryTime: Date.now() - startCountersQueryTime,
                    countersQueryCount: 1,
                    countersSize: countersSize,
                },
                debug: {
                    countersQuery: factFacetStage,
                }
            };
        } catch (error) {
            this.logger.error(`Ошибка при выполнении запроса для ключа ${indexTypeName}: ${error.message}`);
            return {
                counters: null,
                error: error.message,
                metrics: {
                    countersQueryTime: Date.now() - startCountersQueryTime,
                    countersQueryCount: 1,
                    countersQuerySize: countersQuerySize,
                    countersSize: 0,
                },
                debug: {
                    countersQuery: factFacetStage,
                }
            };
        }
    }

    /**
     * Получает счетчики по фактам для заданного факта
     * @param {Array<Object>} factIndexInfos - массив объектов с информацией об индексных значениях, применимых к факту
     *          структура объекта:
     *          {
     *              index: {
     *                  fieldName: string,      // Имя индексируемого поля в факте, значение которого включается в первичный ключ индекса
     *                  dateName: string,       // Имя поля даты факта, значение которого включается в индекс
     *                  indexType: number,      // Тип индекса (идентификатор типа индекса), который включается в индексном значении
     *                  indexValue: number,     // Тип кодирования значения индекса (1 - хеш, 2 - само значение поля fieldName), который включается в индексном значении
     *                  indexTypeName: string,  // Имя типа индекса, которое используется для фильтрации счетчиков
     *                  limit: number           // Максимальное количество получаемых по индексу фактов для расчета счетчиков
     *              },
     *              hashValue: string
     *          }
     * @param {Object} fact - факт
     * @param {number} depthLimit - максимальное количество фактов для получения
     * @param {Date} depthFromDate - дата, с которой начинать поиск фактов
     * @returns {Promise<Array>} счетчики по фактам
     */
    async getRelevantFactCounters(factIndexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined, debugMode = false) {
        if (this.includeFactDataToIndex) {
            return this.getRelevantFactCountersFromIndex(factIndexInfos, fact, depthLimit, depthFromDate, debugMode);
        } else {
            return this.getRelevantFactCountersFromFact(factIndexInfos, fact, depthLimit, depthFromDate, debugMode);
        }
    }

    
    async getRelevantFactCountersFromFact(factIndexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined, debugMode = false) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        const indexCountersInfo = this.getFactIndexCountersInfo(fact);
        if (!indexCountersInfo) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);

            return {
                result: {},
                processingTime: Date.now() - startTime,
            };
        }

        // Перебираем все индексы, по которым нужно построить счетчики и формируем агрегационный запрос
        const queriesByIndexName = {};
        // this.logger.info(`*** Индексы счетчиков: ${JSON.stringify(indexCountersInfo)}`);
        // this.logger.info(`*** Получено ${Object.keys(indexCountersInfo).length} типов индексов счетчиков: ${Object.keys(indexCountersInfo).join(', ')}`);
        Object.keys(indexCountersInfo).forEach((indexTypeName) => {
            const counters = indexCountersInfo[indexTypeName] ? indexCountersInfo[indexTypeName] : {};
            this.logger.debug(`Обрабатываются счетчики (${Object.keys(counters).length}) для типа индекса ${indexTypeName} для факта ${fact?._id}: ${Object.keys(counters).join(', ')}`);
            const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
            if (!indexInfo) {
                this.logger.warn(`Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
                return;
            }
            const factIndexFindQuery = {
                "_id.h": indexInfo.hashValue
            };
            if (depthFromDate) {
                factIndexFindQuery["dt"] = {
                    "$gte": depthFromDate
                };
            }
            if (fact) {
                factIndexFindQuery["_id.f"] = {
                    "$ne": fact._id
                };
            }
            queriesByIndexName[indexTypeName] = {
                factIndexFindQuery: factIndexFindQuery,
                factIndexFindOptions: {
                    batchSize: 5000,
                    readConcern: this.READ_CONCERN,
                    readPreference: { mode: "secondaryPreferred" },
                    comment: "getRelevantFactsByIndex - find",
                    projection: { "_id.f": 1 }
                },
                factIndexFindSort: {
                    "_id.h": 1,
                    "dt": -1
                },
                depthLimit: Math.min(indexInfo.index.limit ?? 100, depthLimit),
            };
        });

        if (Object.keys(queriesByIndexName).length === 0) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные счетчики. Счетчики не будут вычисляться.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    includeFactDataToIndex: false,
                    totalIndexCount: factIndexInfos?.length,
                    counterIndexCount: Object.keys(indexCountersInfo).length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    relevantIndexCount: 0,
                    relevantFactsCount: 0,
                    relevantFactsSize: 0,
                    relevantFactsTime: 0,
                    prepareCountersQueryTime: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    detailMetrics: null
                },
                debug: {
                    relevantFactsQuerySize: 0,
                    countersQueryTotalSize: 0,
                    indexQuery: null,
                    countersQuery: null,
                }
            };
        }

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareQueriesTime = Date.now();
        const queryPromises = Object.keys(queriesByIndexName).map(async (indexTypeName) => {
            const indexNameQuery = queriesByIndexName[indexTypeName];
            const startQuery = Date.now();
            const indexNameResult = await this._getRelevantFactsByIndex(indexNameQuery, debugMode);
            const emptyRelevantFacts = !indexNameResult.factIds || !indexNameResult.factIds.length;
            if (emptyRelevantFacts) {
                const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
                const fieldName = indexInfo.index.fieldName;
                const fieldValue = fact?.d[fieldName] ?? "undefined";
                this.logger.warn(`Для типа индекса ${indexTypeName} не найден список релевантных фактов для значения индекса: ${indexInfo.hashValue}. (поле "${fieldName}": "${fieldValue}")`);
            }
            const countersResult = await this._getCounters(indexTypeName, indexNameResult.factIds, indexCountersInfo[indexTypeName], debugMode);
            return {
                indexTypeName: indexTypeName,
                counters: countersResult.counters,
                error: countersResult.error,
                processingTime: Date.now() - startQuery,
                metrics: { ...indexNameResult.metrics, ...countersResult.metrics },
                debug: { ...indexNameResult.debug, ...countersResult.debug },
            };
        });

        // Ждем выполнения всех запросов
        const startQueriesTime = Date.now();
        const queryResults = await Promise.all(queryPromises);
        const stopQueriesTime = Date.now();

        // Объединяем результаты в один JSON объект
        const mergedCounters = {};
        const detailMetrics = {};
        let relevantFactsQuerySize = 0;
        let relevantFactsTime = 0;
        let relevantFactsCount = 0;
        let relevantFactsSize = 0;
        let countersQuerySize = 0;
        let countersQueryTime = 0;
        let countersQueryCount = 0;
        let countersSize = 0;
        const indexQuery = {};
        const countersQuery = {};
        queryResults.forEach((result) => {
            if (result.counters) {
                Object.assign(mergedCounters, result.counters);
            }
            detailMetrics[result.indexTypeName] = {
                processingTime: result.processingTime,
                metrics: result.metrics
            };
            relevantFactsQuerySize += result.metrics.relevantFactsQuerySize ?? 0;
            relevantFactsTime = Math.max(relevantFactsTime, result.metrics.relevantFactsTime ?? 0);
            relevantFactsCount += result.metrics.relevantFactsCount ?? 0;
            relevantFactsSize += result.metrics.relevantFactsSize ?? 0;
            countersQuerySize += result.metrics.countersQuerySize ?? 0;
            countersQueryTime = Math.max(countersQueryTime, result.metrics.countersQueryTime ?? 0);
            countersQueryCount += result.metrics.countersQueryCount ?? 0;
            countersSize += result.metrics.countersSize ?? 0;
            indexQuery[result.indexTypeName] = {
                query: result.debug.indexQuery,
                sort: result.debug.indexSort,
                depthLimit: result.debug.depthLimit,
            };
            countersQuery[result.indexTypeName] = result.debug.countersQuery;
        });

        this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * counterIndexCount - количество индексов с счетчиками, применимых к факту (после фильтрации по условию computationConditions)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * prepareCountersQueryTime - время подготовки параллельных запросов на вычисление счетчиков
         * processingQueriesTime - время выполнения параллельных запросов на вычисление счетчиков
         * relevantFactsQuerySize - размер запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsTime - время выполнения запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsCount - количество релевантных фактов (факты, которые попали после поиска по индексам)
         * relevantFactsSize - размер массива релевантных фактов (факты, которые попали после поиска по индексам)
         * countersQuerySize - размер запросов вычисления счетчиков по релевантным фактам
         * countersQueryTime - время выполнения параллельных запросов на вычисление счетчиков
         * countersQueryCount - число одновременных запросов на вычисление счетчиков
         * countersSize - размер полученных всех счетчиков по всем индексам,
         * detailMetrics - метрики выполнения запросов в разрезе индексов
         * 
         * indexQuery - запрос по индексам для поиска ИД релевантных фактов
         * countersQuery - запрос на вычисление счетчиков по релевантным фактам
         */

        // Возвращаем массив статистики
        return {
            result: Object.keys(mergedCounters).length ? mergedCounters : {},
            processingTime: Date.now() - startTime,
            metrics: {
                includeFactDataToIndex: false,
                totalIndexCount: factIndexInfos?.length,
                counterIndexCount: Object.keys(indexCountersInfo).length,
                factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                relevantIndexCount: Object.keys(queriesByIndexName).length,
                prepareQueriesTime: startQueriesTime - startPrepareQueriesTime,
                processingQueriesTime: stopQueriesTime - startQueriesTime,
                relevantFactsQuerySize: relevantFactsQuerySize,
                relevantFactsTime: relevantFactsTime,
                relevantFactsCount: relevantFactsCount,
                relevantFactsSize: relevantFactsSize,
                countersQuerySize: countersQuerySize,
                countersQueryTime: countersQueryTime,
                countersQueryCount: countersQueryCount,
                countersSize: countersSize,
                detailMetrics: detailMetrics,
            },
            debug: {
                indexQuery: indexQuery,
                countersQuery: countersQuery,
            }
        };
    }

    async getRelevantFactCountersFromIndex(factIndexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined, debugMode = false) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        const indexCountersInfo = this.getFactIndexCountersInfo(fact);
        if (!indexCountersInfo) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);

            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        // Перебираем все индексы, по которым нужно построить счетчики и формируем агрегационный запрос
        const queriesByIndexName = {};
        // this.logger.info(`*** Индексы счетчиков: ${JSON.stringify(indexCountersInfo)}`);
        // this.logger.info(`*** Получено ${Object.keys(indexCountersInfo).length} типов индексов счетчиков: ${Object.keys(indexCountersInfo).join(', ')}`);
        Object.keys(indexCountersInfo).forEach((indexTypeName) => {
            const counters = indexCountersInfo[indexTypeName] ? indexCountersInfo[indexTypeName] : {};
            this.logger.debug(`Обрабатываются счетчики (${Object.keys(counters).length}) для типа индекса ${indexTypeName} для факта ${fact?._id}: ${Object.keys(counters).join(', ')}`);
            const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
            if (!indexInfo) {
                this.logger.warn(`Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
                return;
            }
            const match = {
                "_id.h": indexInfo.hashValue
            };
            if (depthFromDate) {
                match["dt"] = {
                    "$gte": depthFromDate
                };
            }
            if (fact) {
                match["_id.f"] = {
                    "$ne": fact._id
                };
            }
            const sort = {
                "_id.h": 1,
                "dt": -1
            };
            const limit = Math.min(indexInfo.index.limit ?? 100, depthLimit);
            const aggregateIndexQuery = [
                { "$match": match },
                { "$sort": sort },
                { "$limit": limit }
            ];

            const indexCounterList = indexCountersInfo[indexTypeName];

            const projectState = {};
            Object.keys(indexCounterList).forEach(counterName => {
                projectState[counterName] = { "$arrayElemAt": ["$" + counterName, 0] };
            });
            aggregateIndexQuery.push(... [
                { "$facet": indexCounterList },
                { "$project": projectState }
            ]);

            queriesByIndexName[indexTypeName] = {
                query: aggregateIndexQuery,
            };
        });

        if (Object.keys(queriesByIndexName).length === 0) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные счетчики. Счетчики не будут вычисляться.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    includeFactDataToIndex: true,
                    totalIndexCount: factIndexInfos?.length,
                    counterIndexCount: Object.keys(indexCountersInfo).length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    prepareCountersQueryTime: 0,
                    relevantIndexCount: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryCount: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryTime: 0,
                    countersQuerySize: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    countersSize: 0,
                    detailMetrics: null
                },
                debug: {
                    indexQuery: null,
                    countersQuery: null,
                }
            };
        }

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareQueriesTime = Date.now();
        const queryPromises = Object.keys(queriesByIndexName).map(async (indexTypeName) => {
            const indexNameQuery = queriesByIndexName[indexTypeName].query;
            const startQuery = Date.now();

            const aggregateOptions = {
                batchSize: 5000,
                readConcern: this.READ_CONCERN,
                readPreference: { mode: "secondaryPreferred" },
                comment: "getRelevantFactCounters - aggregate",
            };
            // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(indexNameQuery)}`);
            const factIndexCollection = this._getFactIndexCollection();
            const startCountersQueryTime = Date.now();
            const countersQuerySize = debugMode ? JSON.stringify(indexNameQuery).length : undefined;
            try {
                this.logger.debug(`Агрегационный запрос для индекса ${indexTypeName}: ${JSON.stringify(indexNameQuery)}`);
                const countersResult = await factIndexCollection.aggregate(indexNameQuery, aggregateOptions).toArray();
                const countersSize = debugMode ? JSON.stringify(countersResult[0]).length : undefined;
                return {
                    indexTypeName: indexTypeName,
                    counters: countersResult[0],
                    metrics: {
                        countersQuerySize: countersQuerySize,
                        countersQueryTime: Date.now() - startCountersQueryTime,
                        countersQueryCount: 1,
                        countersSize: countersSize,
                    },
                    debug: {
                        countersQuery: indexNameQuery,
                    }
                };
            } catch (error) {
                this.logger.error(`Ошибка при выполнении запроса для ключа ${indexTypeName}: ${error.message}`);
                return {
                    indexTypeName: indexTypeName,
                    counters: null,
                    error: error.message,
                    processingTime: Date.now() - startQuery,
                    metrics: {
                        countersQueryTime: Date.now() - startCountersQueryTime,
                        countersQueryCount: 1,
                        countersQuerySize: countersQuerySize,
                        countersSize: 0,
                    },
                    debug: {
                        countersQuery: indexNameQuery,
                    }
                };
            }
        });

        // Ждем выполнения всех запросов
        const startQueriesTime = Date.now();
        const queryResults = await Promise.all(queryPromises);
        const stopQueriesTime = Date.now();

        // Объединяем результаты в один JSON объект
        const mergedCounters = {};
        const detailMetrics = {};
        let countersQuerySize = 0;
        let countersQueryTime = 0;
        let countersCount = 0;
        let countersSize = 0;
        const countersQuery = {};
        queryResults.forEach((result) => {
            if (result.counters) {
                Object.assign(mergedCounters, result.counters);
            }
            detailMetrics[result.indexTypeName] = {
                processingTime: result.processingTime,
                metrics: result.metrics
            };
            countersQuerySize += result.metrics.countersQuerySize ?? 0;
            countersQueryTime = Math.max(countersQueryTime, result.metrics.countersQueryTime ?? 0);
            countersCount += result.metrics.countersQueryCount ?? 0;
            countersSize += result.metrics.countersSize ?? 0;
            countersQuery[result.indexTypeName] = result.debug.countersQuery;
        });

        this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * counterIndexCount - количество индексов с счетчиками, применимых к факту (после фильтрации по условию computationConditions)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * prepareCountersQueryTime - время подготовки параллельных запросов на вычисление счетчиков
         * processingQueriesTime - время выполнения параллельных запросов на вычисление счетчиков
         * relevantFactsQuerySize - размер запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsQueryTime - время выполнения запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsCount - количество релевантных фактов (факты, которые попали после поиска по индексам)
         * relevantFactsSize - размер массива релевантных фактов (факты, которые попали после поиска по индексам)
         * countersQuerySize - размер запросов вычисления счетчиков по релевантным фактам
         * countersQueryTime - время выполнения параллельных запросов на вычисление счетчиков
         * countersQueryCount - число одновременных запросов на вычисление счетчиков
         * countersSize - размер полученных всех счетчиков по всем индексам,
         * detailMetrics - метрики выполнения запросов в разрезе индексов
         * 
         * indexQuery - запрос по индексам для поиска ИД релевантных фактов
         * countersQuery - запрос на вычисление счетчиков по релевантным фактам
         */

        // Возвращаем массив статистики
        return {
            result: Object.keys(mergedCounters).length ? mergedCounters : {},
            processingTime: Date.now() - startTime,
            metrics: {
                includeFactDataToIndex: true,
                totalIndexCount: factIndexInfos?.length,
                counterIndexCount: Object.keys(indexCountersInfo).length,
                factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                relevantIndexCount: Object.keys(queriesByIndexName).length,
                prepareQueriesTime: startQueriesTime - startPrepareQueriesTime,
                processingQueriesTime: stopQueriesTime - startQueriesTime,
                relevantFactsQuerySize: undefined,
                relevantFactsQueryTime: undefined,
                relevantFactsCount: undefined,
                relevantFactsSize: undefined,
                countersQuerySize: countersQuerySize,
                countersQueryTime: countersQueryTime,
                countersQueryCount: countersCount,
                countersSize: countersSize,
                detailMetrics: detailMetrics,
            },
            debug: {
                indexQuery: undefined,
                countersQuery: countersQuery,
            }
        };
    }

    // Старая реализация получения счетчиков по релевантным фактам
    async oldGetRelevantFactCounters(factIndexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined, debugMode = false) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        const indexCountersInfo = this.getFactIndexCountersInfo(fact);
        if (!indexCountersInfo) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);

            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        /**
         * 
         * Подготовка первого агрегационного запроса поиска по индексным значениям для получения списка ИД релевантных фактов
         * 
         */
        // Перебираем все индексы, по которым нужно построить счетчики и формируем агрегационный запрос
        const indexFacetStage = {};
        const indexHashValues = [];
        // this.logger.info(`*** Индексы счетчиков: ${JSON.stringify(indexCountersInfo)}`);
        // this.logger.info(`*** Получено ${Object.keys(indexCountersInfo).length} типов индексов счетчиков: ${Object.keys(indexCountersInfo).join(', ')}`);
        Object.keys(indexCountersInfo).forEach((indexTypeName) => {
            const counters = indexCountersInfo[indexTypeName] ? indexCountersInfo[indexTypeName] : {};
            this.logger.info(`Обрабатываются счетчики (${Object.keys(counters).length}) для типа индекса ${indexTypeName} для факта ${fact?._id}: ${Object.keys(counters).join(', ')}`);
            const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
            if (!indexInfo) {
                this.logger.warn(`Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
                return;
            }
            indexHashValues.push(indexInfo.hashValue);
            const findFactMatchQuery = {
                "_id.h": indexInfo.hashValue
            };
            if (fact) {
                findFactMatchQuery["_id.f"] = {
                    "$ne": fact._id
                };
            }
            const findFactGroupQuery = {
                "_id": null,
                "factIds": {
                    "$push": "$_id.f"
                }
            };
            const findFactProjectQuery = {
                "_id": 0,
                "factIds": 1
            };

            indexFacetStage[indexTypeName] = [
                { "$match": findFactMatchQuery },
                { "$limit": Math.min(indexInfo.index.limit ?? 100, depthLimit) },
                //                { "$project": { "_id.f": 1 } },
                { "$group": findFactGroupQuery },
                { "$project": findFactProjectQuery }
            ];
        });

        if (Object.keys(indexFacetStage).length === 0) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные счетчики. Счетчики не будут вычисляться.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    totalIndexCount: factIndexInfos?.length,
                    counterIndexCount: Object.keys(indexCountersInfo).length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    relevantIndexCount: 0,
                    relevantFactsCount: 0,
                    relevantFactsSize: 0,
                    relevantFactsTime: 0,
                    prepareCountersQueryTime: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                },
                debug: {
                    relevantFactsQuerySize: 0,
                    countersQueryTotalSize: 0,
                    indexQuery: null,
                    countersQuery: null,
                }
            };
        }

        // Предварительная фильтрация данных
        const matchIndexQuery = {
            "_id.h": {
                "$in": indexHashValues
            },
        };
        if (depthFromDate) {
            matchIndexQuery["dt"] = {
                "$gte": depthFromDate
            };
        }

        const sortIndexQuery = {
            "_id.h": 1,
            "dt": -1
        };

        const projectIndexQuery = {
            "_id": 1
        };

        const aggregateIndexQuery = [
            { "$match": matchIndexQuery },
            { "$sort": sortIndexQuery },
            { "$project": projectIndexQuery },
            { "$facet": indexFacetStage }
        ];
        const relevantFactsQuerySize = debugMode ? JSON.stringify(aggregateIndexQuery).length : undefined;

        // Выполняем первый агрегирующий запрос на список идентификаторов фактов в разрезе по индексов
        const aggregateIndexOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - index aggregate",
        };

        this.logger.debug(`Агрегационный запрос на список ИД фактов в разрезе индексов: ${JSON.stringify(aggregateIndexQuery)}\n`);
        const factIndexCollection = this._getFactIndexCollection();
        const startrelevantFactsTime = Date.now();
        const factIndexResult = await factIndexCollection.aggregate(aggregateIndexQuery, aggregateIndexOptions).toArray();
        const stoprelevantFactsTime = Date.now();
        const relevantFactsSize = debugMode ? JSON.stringify(factIndexResult).length : undefined;
        const factIdsByIndexName = factIndexResult[0];
        this.logger.debug(`✓ Получены списки ИД фактов: ${JSON.stringify(factIdsByIndexName)} \n`);

        /**
         * 
         * Подготовка набора агрегационных запросов на получение счетчков по каждому виду индексов
         * 
         */
        const factFacetStage = {};
        let relevantFactsCount = 0;
        Object.keys(indexCountersInfo).forEach((indexTypeName) => {
            // Старый способ получения списка ИД фактов: {indexName1: [{_id: {f: "хххх"}}...], indexName2: [{_id: {f: "хххх"}}...]}
            // const factIds = factIdsByIndexName[indexTypeName];
            // Новый способ: {indexName1: [{ factIds: [...]}], indexName2: [{ factIds: [...]}]}
            const factIds = factIdsByIndexName[indexTypeName] ? factIdsByIndexName[indexTypeName][0]?.factIds : [];
            if (!factIds || !factIds.length) {
                this.logger.warn(`Для типа индекса ${indexTypeName} не найден список релевантных фактов.`);
                return;
            }
            relevantFactsCount += factIds.length;
            // Финальный этап агрегации для улучшения формата результатов: преобразование из массивов в объект
            const projectState = {};
            Object.keys(indexCountersInfo[indexTypeName]).forEach(counterName => {
                projectState[counterName] = { "$arrayElemAt": ["$" + counterName, 0] };
            });
            factFacetStage[indexTypeName] = [
                {
                    "$match": {
                        // "_id": { "$in": factIds.map(item => item._id.f) }
                        "_id": { "$in": factIds }
                    }
                },
                { "$facet": indexCountersInfo[indexTypeName] },
                { "$project": projectState }
            ];
        });
        const countersQueryTotalSize = debugMode ? JSON.stringify(factFacetStage).length : undefined;

        if (!Object.keys(factFacetStage).length) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные факты.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    totalIndexCount: factIndexInfos?.length,
                    counterIndexCount: Object.keys(indexCountersInfo).length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    relevantIndexCount: Object.keys(factIdsByIndexName).length,
                    relevantFactsCount: relevantFactsCount,
                    relevantFactsSize: relevantFactsSize,
                    relevantFactsTime: stoprelevantFactsTime - startrelevantFactsTime,
                    prepareCountersQueryTime: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                },
                debug: {
                    relevantFactsQuerySize: relevantFactsQuerySize,
                    countersQueryTotalSize: 0,
                    indexQuery: aggregateIndexQuery,
                    countersQuery: null,
                }
            };
        }

        // Выполнить агрегирующий запрос
        const aggregateFactOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(factFacetStage)}`);
        //
        // Запускаем параллельно запросы в factFacetStage:
        //
        const factsCollection = this._getFactsCollection();

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareCountersQueryTime = Date.now();
        const queryPromises = Object.keys(factFacetStage).map(async (indexName) => {
            try {
                this.logger.debug(`Агрегационный запрос для индекса ${indexName}: ${JSON.stringify(factFacetStage[indexName])}`);
                const countersResult = await factsCollection.aggregate(factFacetStage[indexName], aggregateFactOptions).toArray();
                return { indexName: indexName, counters: countersResult[0] };
            } catch (error) {
                this.logger.error(`Ошибка при выполнении запроса для ключа ${indexName}: ${error.message}`);
                return { indexName: indexName, counters: null, error: error.message };
            }
        });

        // Ждем выполнения всех запросов
        const startCountersQueryTime = Date.now();
        const queryResults = await Promise.all(queryPromises);
        const stopCountersQueryTime = Date.now();

        // Объединяем результаты в один JSON объект
        const mergedCounters = {};
        queryResults.forEach(({ indexName, counters, error }) => {
            if (error) {
                this.logger.warn(`Запрос для индекса ${indexName} завершился с ошибкой: ${error}`);
            } else {
                if (counters) {
                    Object.assign(mergedCounters, counters);
                }
            }
        });

        // Преобразуем в формат, ожидаемый тестами - массив из одного объекта
        this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * counterIndexCount - количество индексов с счетчиками, применимых к факту (после фильтрации по условию computationConditions)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * relevantFactsCount - количество релевантных фактов (факты, которые попали после поиска по индексам)
         * relevantFactsSize - размер массива релевантных фактов (факты, которые попали после поиска по индексам)
         * relevantFactsTime - время выполнения запроса по индексам для поиска ИД релевантных фактов
         * prepareCountersQueryTime - время подготовки параллельных запросов на вычисление счетчиков
         * countersQueryTime - время выполнения параллельных запросов на вычисление счетчиков
         * relevantFactsQuerySize - размер запроса по индексам для поиска ИД релевантных фактов
         * countersQueryCount - число одновременных запросов на вычисление счетчиков
         * countersQueryTotalSize - размер всех запросов вычисления счетчиков
         * indexQuery - запрос по индексам для поиска ИД релевантных фактов
         * countersQuery - запрос на вычисление счетчиков по релевантным фактам
         */

        // Возвращаем массив статистики
        return {
            result: Object.keys(mergedCounters).length ? mergedCounters : {},
            processingTime: Date.now() - startTime,
            metrics: {
                totalIndexCount: factIndexInfos?.length,
                counterIndexCount: Object.keys(indexCountersInfo).length,
                factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                relevantIndexCount: Object.keys(factIdsByIndexName).length,
                relevantFactsCount: relevantFactsCount,
                relevantFactsTime: stoprelevantFactsTime - startrelevantFactsTime,
                prepareCountersQueryTime: startCountersQueryTime - startPrepareCountersQueryTime,
                countersQueryTime: stopCountersQueryTime - startCountersQueryTime,
                countersQueryCount: Object.keys(factFacetStage).length,
                countersQueryTotalSize: countersQueryTotalSize,
            },
            debug: {
                relevantFactsSize: relevantFactsSize,
                relevantFactsQuerySize: relevantFactsQuerySize,
                indexQuery: aggregateIndexQuery,
                countersQuery: factFacetStage,
            }
        };
    }


    // Старая реализация
    async oldGetRelevantFactCounters(indexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        const countersInfo = this.oldGetCountersInfo(fact);
        if (!countersInfo) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);
            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        // Убираем из поиска лишние индексы, поэтому получаем список хешей значений индексов факта для индексов из счетчиков
        const indexHashValues = [];
        countersInfo.indexTypeNames.forEach(item => {
            const index = indexInfos.find(info => info.index.indexTypeName === item);
            if (!index) {
                this.logger.warn(`Тип индекса ${item} не найден в списке индексных значений.`);
            } else {
                indexHashValues.push(index.hashValue);
            }
        });

        // Сформировать агрегирующий запрос к коллекции factIndex,
        // получить уникальные значения поля _id
        // и результат объединить с фактом из коллекции facts
        const findFactMatchQuery = {
            "_id.h": {
                "$in": indexHashValues
            }
        };
        if (fact) {
            findFactMatchQuery["_id.f"] = {
                "$ne": fact._id
            };
        }
        if (depthFromDate) {
            findFactMatchQuery.dt = {
                "$gte": depthFromDate
            };
        }
        const findOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1, "it": 1 }
        };
        // Создаем локальные ссылки на коллекции для этого запроса
        const factIndexCollection = this._getFactIndexCollection();
        const factsCollection = this._getFactsCollection();

        const relevantFactIds = await factIndexCollection.find(findFactMatchQuery, findOptions).sort({ dt: -1 }).limit(depthLimit).toArray();
        // this.logger.info(`Поисковый запрос:\n${JSON.stringify(matchQuery)}`);

        // Если нет релевантных индексных значений, возвращаем пустую статистику
        if (relevantFactIds.length === 0) {
            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        // Сформировать агрегирующий запрос к коллекции facts 
        const queryFacts = {
            "$match": {
                "_id": {
                    "$in": relevantFactIds.map(item => item._id.f)
                }
            }
        };

        const aggregateQuery = [
            queryFacts
        ];
        if (countersInfo && countersInfo.facetStages) {
            aggregateQuery.push({ "$facet": countersInfo.facetStages });
        }

        // Опции агрегирующего запроса
        const aggregateOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };

        // Выполнить агрегирующий запрос
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос: ${JSON.stringify(aggregateQuery)}`);
        const result = await factsCollection.aggregate(aggregateQuery, aggregateOptions).toArray();
        this.logger.debug(`✓ Получена статистика по фактам: ${JSON.stringify(result)} `);

        // Если результат пустой, возвращаем пустую статистику
        if (result.length === 0) {
            return {
                result: [],
                processingTime: 0,
                debug: {
                    totalIndexCount: indexInfos?.length,
                    countersFactCount: Object.keys(countersInfo?.facetStages).length,
                    countersIndexCount: countersInfo?.indexTypeNames?.length,
                    filteredIndexCount: indexHashValues?.length,
                    relevantFactsCount: relevantFactIds?.length,
                    findFactMatchQuery: findFactMatchQuery,
                    aggregateQuery: aggregateQuery,
                }
            };
        }

        // Возвращаем массив статистики
        return {
            result: result,
            processingTime: Date.now() - startTime,
            debug: {
                totalIndexCount: indexInfos?.length,
                countersFactCount: Object.keys(countersInfo?.facetStages).length,
                countersIndexCount: countersInfo?.indexTypeNames?.length,
                filteredIndexCount: indexHashValues?.length,
                relevantFactsCount: relevantFactIds?.length,
                findFactMatchQuery: findFactMatchQuery,
                aggregateQuery: aggregateQuery,
            }
        };
    }

    /**
     * (Не используется) Получает релевантные факты для заданного факта с целью вычисления счетчиков
     * @param {Array<Object>} indexInfos - массив объектов с информацией об индексных значениях
     * @param {Object} fact - факт
     * @param {number} depthLimit - максимальное количество фактов для получения
     * @param {Date} depthFromDate - дата, с которой начинать поиск фактов
     * @returns {Promise<Array>} релевантные факты
     */
    async getRelevantFacts(indexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`Получение релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        const countersInfo = this.oldGetCountersInfo(fact);
        if (!countersInfo) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);
            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        // Убираем из поиска лишние индексы, поэтому получаем список хешей значений индексов факта для индексов из счетчиков
        const indexHashValues = [];
        countersInfo.indexTypeNames.forEach(item => {
            const index = indexInfos.find(info => info.index.indexTypeName === item);
            if (!index) {
                this.logger.warn(`Тип индекса ${item} не найден в списке индексных значений.`);
            } else {
                indexHashValues.push(index.hashValue);
            }
        });

        // Сформировать агрегирующий запрос к коллекции factIndex,
        // получить уникальные значения поля _id
        // и результат объединить с фактом из коллекции facts
        const matchQuery = {
            "_id.h": {
                "$in": indexHashValues
            }
        };
        if (fact) {
            matchQuery["_id.f"] = {
                "$ne": fact._id
            };
        }
        if (depthFromDate) {
            matchQuery.dt = {
                "$gte": depthFromDate
            };
        }
        const findOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: "secondaryPreferred",
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1 }
        };
        // Создаем локальные ссылки на коллекции для этого запроса
        const factIndexCollection = this._getFactIndexCollection();
        const factsCollection = this._getFactsCollection();

        // this.logger.debug("   matchQuery: "+JSON.stringify(matchQuery));
        const relevantFactIds = await factIndexCollection.find(matchQuery, findOptions).sort({ dt: -1 }).batchSize(5000).limit(depthLimit).toArray();
        // Сформировать агрегирующий запрос к коллекции facts,
        const aggregateQuery = [
            {
                "$match": {
                    "_id": {
                        "$in": relevantFactIds.map(item => item._id.f)
                    }
                }
            }
        ];

        // this.logger.debug(`Агрегационный запрос: ${JSON.stringify(aggregateQuery, null, 2)}`);

        // Выполнить агрегирующий запрос
        const aggregateOptions = {
            batchSize: 5000,
            readConcern: this.READ_CONCERN,
            readPreference: { mode: "secondaryPreferred" },
            comment: "getRelevantFactCounters - aggregate",
        };
        const result = await factsCollection.aggregate(aggregateQuery, aggregateOptions).batchSize(5000).toArray();
        this.logger.debug(`✓ Получено ${result.length} фактов`);
        // this.logger.debug(JSON.stringify(result, null, 2));
        // Возвращаем массив фактов
        return {
            result: result,
            processingTime: Date.now() - startTime,
            debug: {
                totalIndexCount: indexInfos?.length,
                countersFactCount: Object.keys(countersInfo?.facetStages).length,
                countersIndexCount: countersInfo?.indexTypeNames?.length,
                filteredIndexCount: indexHashValues?.length,
                relevantFactsCount: relevantFactIds.length,
                aggregateQuery: aggregateQuery,
            }
        };
    }

    /**
     * Сохраняет запись в коллекцию логов
     * @param {string} processId - идентификатор процесса обработки (process id)
     * @param {Object} processingTime - JSON объект со временем выполнения запроса (processing time)
     * @param {Object} metrics - JSON объект с метриками обработки (metrics)
     * @param {Object} debugInfo - JSON объект с отладочной информацией (debug info)
     */
    async saveLog(processId, message, processingTime, metrics, debugInfo) {
        this.checkConnection();
        const logCollection = this._getLogCollection();
        try {
            await logCollection.insertOne({
                _id: new ObjectId(),
                c: new Date(),
                p: String(processId),
                msg: message,
                t: processingTime,
                m: metrics,
                di: debugInfo
            });
        } catch (error) {
            this.logger.error(`✗ Ошибка при сохранении записи в коллекцию логов ${this.LOG_COLLECTION_NAME}: ${error.message}`);
        }
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factsCollection = this._getFactsCollection();

            const deleteOptions = {
                readConcern: this.READ_CONCERN,
                readPreference: "primary",
                writeConcern: this.WRITE_CONCERN,
                comment: "clearFactCollection",
            };
            const result = await factsCollection.deleteMany({}, deleteOptions);
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factsCollection = this._getFactsCollection();

            const result = await factsCollection.countDocuments();
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factsCollection = this._getFactsCollection();

            const findOptions = {
                batchSize: 5000,
                readConcern: this.READ_CONCERN,
                readPreference: { mode: "secondaryPreferred" },
                comment: "findFacts"
            };
            const facts = await factsCollection.find(filter, findOptions).toArray();
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factIndexCollection = this._getFactIndexCollection();

            const deleteOptions = {
                readConcern: this.READ_CONCERN,
                readPreference: "primary",
                writeConcern: this.WRITE_CONCERN,
                comment: "clearFactIndexCollection",
            };
            const result = await factIndexCollection.deleteMany({}, deleteOptions);
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factIndexCollection = this._getFactIndexCollection();

            const result = await factIndexCollection.countDocuments();
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при подсчете числа документов в коллекции индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Очищает коллекцию логов
     * @returns {Promise<Object>} результат очистки
     */
    async clearLogCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            // Создаем локальную ссылку на коллекцию для этого запроса
            const logCollection = this._getLogCollection();

            const deleteOptions = {
                readConcern: this.READ_CONCERN,
                readPreference: "primary",
                writeConcern: this.WRITE_CONCERN,
                comment: "clearLogCollection",
            };
            const result = await logCollection.deleteMany({}, deleteOptions);
            this.logger.debug(`✓ Удалено ${result.deletedCount} записей из коллекции ${this.LOG_COLLECTION_NAME}`);
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при очистке коллекции логов:', error.message);
            throw error;
        }
    }

    /**
     * Подсчитывает количество документов в коллекции логов
     * @returns {Promise<number>} количество документов
     */
    async countLogCollection() {
        if (!this._isConnected) {
            throw new Error('Нет подключения к MongoDB');
        }

        try {
            // Создаем локальную ссылку на коллекцию для этого запроса
            const logCollection = this._getLogCollection();

            const result = await logCollection.countDocuments();
            return result;
        } catch (error) {
            this.logger.error('✗ Ошибка при подсчете числа документов в коллекции логов:', error.message);
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
                    /* Замедляет работу
                    clusteredIndex: {
                        key: { "_id": 1 },
                        unique: true
                    },
                    */
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

            // Создаем локальную ссылку на коллекцию для этого запроса
            const factsCollection = this._getFactsCollection();

            for (const indexSpec of indexesToCreate) {
                try {
                    await factsCollection.createIndex(indexSpec.key, indexSpec.options);
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
                    required: ["_id", "dt", "c"],
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
                        dt: {
                            bsonType: "date",
                            description: "Дата факта"
                        },
                        d: {
                            bsonType: "object",
                            description: "JSON объект с данными факта"
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
                    /* Замедляет работу
                    clusteredIndex: {
                        key: { "_id": 1, "dt": 1 },
                        unique: true
                    },
                    */
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
                    key: { "_id.h": 1, "dt": -1 },
                    options: {
                        name: 'idx_id_h_dt',
                        background: true
                    }
                }
            ];

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции индексных значений ${this.FACT_INDEX_COLLECTION_NAME}...`);

            // Создаем локальную ссылку на коллекцию для этого запроса
            const factIndexCollection = this._getFactIndexCollection();

            for (const indexSpec of indexesToCreate) {
                try {
                    await factIndexCollection.createIndex(indexSpec.key, indexSpec.options);
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
            // Создаем локальную ссылку на коллекцию для этого запроса
            const factIndexCollection = this._getFactIndexCollection();

            const sample = await factIndexCollection.findOne({});

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
     * Создает схему валидации для коллекции логов
     * @returns {Promise<boolean>} результат создания схемы
     */
    async _createLogCollectionSchema() {
        this.checkConnection();

        try {
            // Определяем схему валидации JSON для коллекции factIndex
            const schema = {
                $jsonSchema: {
                    bsonType: "object",
                    title: "Схема для коллекции логов",
                    description: "Схема для коллекции логов",
                    required: ["_id", "c"],
                    properties: {
                        _id: {
                            bsonType: "objectId",
                            description: "первичный ключ"
                        },
                        c: {
                            bsonType: "date",
                            description: "Дата и время создания записи в журнал"
                        },
                        p: {
                            bsonType: "string",
                            description: "идентификатор процесса обработки (process id)"
                        },
                        msg: {
                            bsonType: "object",
                            description: "Исходное сообщение"
                        },
                        t: {
                            bsonType: "object",
                            description: "JSON объект со временем выполнения запроса"
                        },
                        m: {
                            bsonType: "object",
                            description: "JSON объект с метриками обработки (metrics)"
                        },
                        di: {
                            bsonType: "object",
                            description: "JSON объект с отладочной информацией (debug info)"
                        }
                    },
                    additionalProperties: false
                }
            };

            // Проверяем, существует ли коллекция
            const collections = await this._counterDb.listCollections({ name: this.LOG_COLLECTION_NAME }).toArray();

            if (collections.length > 0) {
                // Коллекция существует, обновляем схему валидации
                this.logger.debug(`Обновление схемы валидации для существующей коллекции ${this.LOG_COLLECTION_NAME}...`);
                await this._counterDb.command({
                    collMod: this.LOG_COLLECTION_NAME,
                    validator: schema,
                    validationLevel: "moderate",
                    validationAction: "warn"
                });
            } else {
                // Коллекция не существует, создаем с валидацией
                this.logger.debug(`Создание новой коллекции ${this.LOG_COLLECTION_NAME} со схемой валидации...`);
                // Параметры создания коллекции для производственной среды
                const productionCreateOptions = {
                    validator: schema,
                    /* Замедляет работу
                    clusteredIndex: {
                        key: { "_id": 1 },
                        unique: true
                    },
                    */
                    validationLevel: "off",
                    validationAction: "warn"
                };
                // Тестовая среда
                const testCreateOptions = {
                    validator: schema,
                    validationLevel: "strict",
                    validationAction: "error"
                };
                await this._counterDb.createCollection(this.LOG_COLLECTION_NAME, testCreateOptions);
            }

            this.logger.debug(`✓ Схема валидации для коллекции ${this.LOG_COLLECTION_NAME} успешно создана/обновлена`);

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
    async _createLogIndexes(adminDb) {
        try {
            if (!this._isConnected) {
                throw new Error('Нет подключения к MongoDB');
            }

            const indexesToCreate = [
                {
                    key: { "c": -1 },
                    options: {
                        name: 'idx_c',
                        background: true
                    }
                }
            ];

            let successCount = 0;
            let errors = [];

            this.logger.debug(`Создание индексов для коллекции индексных значений ${this.LOG_COLLECTION_NAME}...`);

            // Создаем локальную ссылку на коллекцию для этого запроса
            const logCollection = this._getLogCollection();

            for (const indexSpec of indexesToCreate) {
                try {
                    await logCollection.createIndex(indexSpec.key, indexSpec.options);
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
            await this._shardCollection(adminDb, this.LOG_COLLECTION_NAME, { "_id": "hashed" }, false);
            this.logger.info(`✓ Выполнено шардирование коллекции ${this.LOG_COLLECTION_NAME}`);

            this.logger.debug(`\n=== Результат создания индексов для логов ===`);
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

            // 3. Создание схемы для коллекции factIndex
            this.logger.debug('\n3. Создание схемы для коллекции log...');
            try {
                results.logSchema = await this._createLogCollectionSchema();
                if (results.logSchema) {
                    this.logger.debug('✓ Схема для коллекции log создана успешно');
                } else {
                    results.errors.push('Не удалось создать схему для коллекции log');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания схемы log: ${error.message}`);
                this.logger.error('✗ Ошибка создания схемы log:', error.message);
            }

            // 4. Создание индексов для коллекции facts
            this.logger.debug('\n4. Создание индексов для коллекции facts...');
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

            // 5. Создание индексов для коллекции factIndex
            this.logger.debug('\n5. Создание индексов для коллекции factIndex...');
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

            // 6. Создание индексов для коллекции log
            this.logger.debug('\n6. Создание индексов для коллекции log...');
            try {
                results.logIndexes = await this._createLogIndexes(adminDb);
                if (results.logIndexes) {
                    this.logger.debug('✓ Индексы для коллекции log созданы успешно');
                } else {
                    results.errors.push('Не удалось создать индексы для коллекции log');
                }
            } catch (error) {
                results.errors.push(`Ошибка создания индексов log: ${error.message}`);
                this.logger.error('✗ Ошибка создания индексов log:', error.message);
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