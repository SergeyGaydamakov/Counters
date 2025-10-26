/**
 * MongoDB Node Driver:
 * https://www.mongodb.com/docs/drivers/node/current/
 */
const { MongoClient, ObjectId } = require('mongodb');
const Logger = require('../utils/logger');
const fs = require('fs');
const path = require('path');
const config = require('../common/config');

// Общий объект для подключения к MongoDB
let global_mongoClient = null;
let global_mongoClientAggregate = null;

/**
 * Класс-провайдер для работы с MongoDB коллекциями facts и factIndex
 * Содержит методы для управления подключением, схемами, вставкой данных, запросами и статистикой
 */
class MongoProvider {
    // Настройки по умолчанию
    DEFAULT_OPTIONS = {
        // Признак индивидуального клиента для подключения к базе данных 
        // Достаточно создавать одного клиента на каждый процесс
        // https://www.mongodb.com/docs/drivers/go/current/connect/connection-options/connection-pools/
        individualProcessClient: true,
        // Создавать отдельный объект коллекции для каждого запроса
        individualCollectionObject: false,
        // Не сохранять данные в коллекции
        disableSave: false,
        // Читаем всегда локальную копию данных
        // https://www.mongodb.com/docs/manual/reference/read-concern/
        readConcern: { "level": "local" },
        aggregateReadPreference: {"mode": "secondaryPreferred"},
        // Журнал сбрасывается на диск в соответствии с политикой журналирования сервера (раз в 100 мс)
        // https://www.mongodb.com/docs/manual/core/journaling/#std-label-journal-process
        // Параметр на сервере: storage.journal.commitIntervalMs
        // https://www.mongodb.com/docs/manual/reference/configuration-options/#mongodb-setting-storage.journal.commitIntervalMs
        // WRITE_CONCERN = { w: "majority", j: false, wtimeout: 5000 };
        writeConcern: { "w": 1, "j": false, "wtimeout": 5000 },
        minPoolSize: 100,
        maxPoolSize: 200,
        maxIdleTimeMS: 60000,
        maxConnecting: 10,
        serverSelectionTimeoutMS: 60000,
    };


    /**
     * Конструктор MongoProvider
     * @param {string} connectionString - Строка подключения к MongoDB
     * @param {string} databaseName - Имя базы данных
     * @param {Object} counterProducer - Объект для создания счетчиков, должен иметь метод make(fact)
     * @param {boolean} includeFactDataToIndex - Включать данные факта в индексное значение
     * @param {boolean} lookupFacts - Получение данных через факты (true) или через индексы (false)
     * @param {boolean} indexBulkUpdate - Использовать bulk операции для сохранения индексных значений  
     * @throws {Error} если mongoCounters не соответствует требуемому интерфейсу
     * 
     * Требования к mongoCounters:
     * - Должен быть объектом (не null/undefined)
     * - Должен иметь метод make(fact)
     * - Метод make должен принимать объект факта и возвращать объект
     * - Возвращаемый объект должен содержать только массивы в качестве значений (для $facet)
     */
    constructor(connectionString, databaseName, databaseOptions, counterProducer, includeFactDataToIndex, lookupFacts, indexBulkUpdate) {
        // Создаем логгер для этого провайдера
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

        this._debugMode= config.logging.debugMode || config.logging.logLevel.toUpperCase() === 'DEBUG';
        this._connectionString = connectionString;
        this._databaseName = databaseName;
        this._databaseOptions = this._mergeDatabaseOptions(databaseOptions);
        this._includeFactDataToIndex = includeFactDataToIndex; // Включать данные факта в индексное значение
        this._lookupFacts = lookupFacts; // Получение данных через факты (true) или через индексы (false)
        this._indexBulkUpdate = indexBulkUpdate; // Использовать bulk операции для сохранения индексных значений
        if (this._includeFactDataToIndex && this._lookupFacts) {
            this.logger.warn('includeFactDataToIndex и lookupFacts равны true одновременно. Будет использоваться алгоритм lookupFacts.');
        }

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
     * Глубокое слияние настроек базы данных с настройками по умолчанию
     * @param {Object} userOptions - пользовательские настройки
     * @returns {Object} - объединенные настройки
     */
    _mergeDatabaseOptions(userOptions) {
        if (!userOptions || typeof userOptions !== 'object') {
            return { ...this.DEFAULT_OPTIONS };
        }

        const merged = { ...this.DEFAULT_OPTIONS };

        // Глубокое слияние для вложенных объектов
        for (const key in userOptions) {
            if (userOptions.hasOwnProperty(key)) {
                if (userOptions[key] !== null &&
                    typeof userOptions[key] === 'object' &&
                    !Array.isArray(userOptions[key]) &&
                    merged[key] !== null &&
                    typeof merged[key] === 'object' &&
                    !Array.isArray(merged[key])) {
                    // Рекурсивное слияние для объектов
                    merged[key] = { ...merged[key], ...userOptions[key] };
                } else {
                    // Простое присваивание для примитивов, массивов и null
                    merged[key] = userOptions[key];
                }
            }
        }

        return merged;
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

    /**
     * Записать сообщение в файл log.txt
     * @param {string} message - Сообщение для записи
     */
    _writeToLogFile(message) {
        if (!config.logging.writeErrorsToFile) {
            return;
        }
        try {
            const logFilePath = path.join(process.cwd(), 'log_error.txt');
            const timestamp = new Date().toISOString();
            const logEntry = `[${timestamp}] ${message}\n`;

            fs.appendFileSync(logFilePath, logEntry, 'utf8');
            this.logger.info(`Сообщение об ошибке записано в файл ${logFilePath}: ${logEntry}`);
        } catch (error) {
            // Если не удается записать в файл, выводим ошибку в консоль
            this.logger.error('Ошибка при записи в log_error.txt:', error.message);
        }
    }

    // ============================================================================
    // ГРУППА 1: УПРАВЛЕНИЕ ПОДКЛЮЧЕНИЕМ
    // ============================================================================

    _getMongoClient(connectionString, databaseOptions) {
        if (global_mongoClient && !databaseOptions.individualProcessClient) {
            // Если не требуется индивидуальное создание клиента и он уже создан
            return global_mongoClient;
        }
        // Опции для подключения по умолчанию
        const options = {
            readConcern: databaseOptions.readConcern,
            readPreference: {"mode": "primary"},
            writeConcern: databaseOptions.writeConcern,
            appName: "CounterTest",
            // monitorCommands: true,
            minPoolSize: databaseOptions.minPoolSize,
            maxPoolSize: databaseOptions.maxPoolSize,
            maxIdleTimeMS: 60000,
            maxConnecting: databaseOptions.maxConnecting,
            serverSelectionTimeoutMS: 60000,
        };
        try {
            if (databaseOptions.individualProcessClient) {
                return new MongoClient(connectionString, options);
            }
            global_mongoClient = new MongoClient(connectionString, options);
            return global_mongoClient;
        } catch (error) {
            global_mongoClient = null;
            throw error;
        }
    }

    _getMongoClientAggregate(connectionString, databaseOptions) {
        if (global_mongoClientAggregate && !databaseOptions.individualProcessClient) {
            // Если не требуется индивидуальное создание клиента и он уже создан
            return global_mongoClientAggregate;
        }
        // Опции для подключения по умолчанию
        const options = {
            readConcern: databaseOptions.readConcern,
            readPreference: databaseOptions.aggregateReadPreference,
            writeConcern: databaseOptions.writeConcern,
            appName: "CounterTest",
            // monitorCommands: true,
            minPoolSize: databaseOptions.minPoolSize,
            maxPoolSize: databaseOptions.maxPoolSize,
            maxIdleTimeMS: 60000,
            maxConnecting: databaseOptions.maxConnecting,
            serverSelectionTimeoutMS: 60000,
        };
        try {
            if (databaseOptions.individualProcessClient) {
                return new MongoClient(connectionString, options);
            }
            global_mongoClientAggregate = new MongoClient(connectionString, options);
            return global_mongoClientAggregate;
        } catch (error) {
            global_mongoClientAggregate = null;
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
            this.logger.debug(`Подключение к MongoDB: ${this._connectionString}`);

            this._counterClient = await this._getMongoClient(this._connectionString, this._databaseOptions);
            await this._counterClient.connect();
            this._aggregateClient = await this._getMongoClientAggregate(this._connectionString, this._databaseOptions);
            await this._aggregateClient.connect();
            this._counterDb = this._counterClient.db(this._databaseName);
            this._aggregateDb = this._aggregateClient.db(this._databaseName);

            this._isConnected = true;

            this.logger.debug(`✓ Успешное подключение к базе данных: ${this._databaseName}`);
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
                await this._aggregateClient.close();
                this.logger.debug('✓ Основное соединение с MongoDB закрыто');
            }

            // Очищаем все ссылки на объекты
            this._counterClient = null;
            this._aggregateClient = null;
            this._counterDb = null;
            this._aggregateDb = null;
            this._factsCollection = null;
            this._factsAggregateCollection = null;
            this._factIndexCollection = null;
            this._factIndexAggregateCollection = null;
            this._logCollection = null;
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
     * Получение объекта Collection для коллекции facts для обычных запросов
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactsCollection() {
        if (this._factsCollection && !this._databaseOptions.individualCollectionObject) {
            // Если не требуется индивидуальное создание объекта коллекции и он уже создан
            return this._factsCollection;
        }
        if (this._databaseOptions.individualCollectionObject) {
            return this._counterDb.collection(this.FACT_COLLECTION_NAME);
        }
        this._factsCollection = this._counterDb.collection(this.FACT_COLLECTION_NAME);
        return this._factsCollection;
    }

    /**
     * Получение объекта Collection для коллекции facts для агрегационных запросов
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactsAggregateCollection() {
        if (this._factsAggregateCollection && !this._databaseOptions.individualCollectionObject) {
            // Если не требуется индивидуальное создание объекта коллекции и он уже создан
            return this._factsAggregateCollection;
        }
        if (this._databaseOptions.individualCollectionObject) {
            return this._aggregateDb.collection(this.FACT_COLLECTION_NAME);
        }
        this._factsAggregateCollection = this._aggregateDb.collection(this.FACT_COLLECTION_NAME);
        return this._factsAggregateCollection;
    }

    /**
     * Получение объекта Collection для коллекции factIndex
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactIndexCollection() {
        if (this._factIndexCollection && !this._databaseOptions.individualCollectionObject) {
            // Если не требуется индивидуальное создание объекта коллекции и он уже создан
            return this._factIndexCollection;
        }
        if (this._databaseOptions.individualCollectionObject) {
            return this._counterDb.collection(this.FACT_INDEX_COLLECTION_NAME);
        }
        this._factIndexCollection = this._counterDb.collection(this.FACT_INDEX_COLLECTION_NAME);
        return this._factIndexCollection;
    }

    /**
     * Получение объекта Collection для коллекции factIndex
     * 
     * @returns {Object} объект Collection для выполнения запросов
     */
    _getFactIndexAggregateCollection() {
        if (this._factIndexAggregateCollection && !this._databaseOptions.individualCollectionObject) {
            // Если не требуется индивидуальное создание объекта коллекции и он уже создан
            return this._factIndexAggregateCollection;
        }
        if (this._databaseOptions.individualCollectionObject) {
            return this._aggregateDb.collection(this.FACT_INDEX_COLLECTION_NAME);
        }
        this._factIndexAggregateCollection = this._aggregateDb.collection(this.FACT_INDEX_COLLECTION_NAME);
        return this._factIndexAggregateCollection;
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
        if (this._databaseOptions.disableSave) {
            return {
                success: true,
                factId: null,
                factInserted: 0,
                factUpdated: 0,
                factIgnored: 0,
                processingTime: 0,
                result: null
            };
        }
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
                readConcern: this._databaseOptions.readConcern,
                readPreference: "primary",
                writeConcern: this._databaseOptions.writeConcern,
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
                    readConcern: this._databaseOptions.readConcern,
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
    async saveFactIndexList(factIndexValues) {
        if (this._databaseOptions.disableSave) {
            return {
                success: true,
                totalProcessed: 0,
                inserted: 0,
                updated: 0,
                duplicatesIgnored: 0,
                errors: [],
                processingTime: 0,
                metrics: {
                    indexValuesCount: 0,
                }
            };
        }
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
            let saveIndexMetrics = [];

            if (this._indexBulkUpdate) {
                // Bulk вставка индексных значений с обработкой дубликатов
                const bulkWriteOptions = {
                    readConcern: this._databaseOptions.readConcern,
                    readPreference: {"mode": "primary"},
                    writeConcern: this._databaseOptions.writeConcern,
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
                    if (this._debugMode) {
                        this.logger.debug("   indexValue: " + JSON.stringify(indexValue));
                    }
                });

                indexResult = await indexBulk.execute(bulkWriteOptions);
                writeErrors = indexResult.writeErrors || [];

                inserted = indexResult.upsertedCount || 0;
                updated = indexResult.modifiedCount || 0;
                duplicatesIgnored = factIndexValues.length - inserted - updated;
            } else {
                const updateOptions = {
                    readConcern: this._databaseOptions.readConcern,
                    readPreference: {"mode": "primary"},
                    writeConcern: this._databaseOptions.writeConcern,
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
                saveIndexMetrics = updateResults.map(result => { return { _id: result._id, processingTime: result.processingTime } });
                duplicatesIgnored = factIndexValues.length - inserted - updated;
            }

            this.logger.debug(`✓ Обработано ${factIndexValues.length} индексных значений в коллекции ${this.FACT_INDEX_COLLECTION_NAME}`);
            this.logger.debug(`  - Вставлено новых: ${inserted}`);
            this.logger.debug(`  - Обновлено существующих: ${updated}`);
            this.logger.debug(`  - Проигнорировано дубликатов: ${duplicatesIgnored}`);
            if (writeErrors && writeErrors.length > 0) {
                this.logger.warn(`⚠ Ошибок при обработке: ${writeErrors.length}: ${writeErrors.join(', \n')}`);
            }

            const result = {
                success: true,
                inserted: inserted,
                updated: updated,
                duplicatesIgnored: duplicatesIgnored,
                errors: writeErrors || [],
                processingTime: Date.now() - startTime,
                metrics: {
                    factIndexValuesCount: factIndexValues.length,
                    saveIndexBulkUpdate: this._indexBulkUpdate,
                }
            };
            if (saveIndexMetrics && saveIndexMetrics.length > 0){
                // Чтобы никого не смущали пустые метрики
                result.metrics.saveIndexMetrics = saveIndexMetrics;
            }

            return result;

        } catch (error) {
            this.logger.error('✗ Критическая ошибка при вставке индексных значений:', error.message);
            throw error;
        }
    }

    /**
     * Выдает счетчики применительно к факту, с разбивкой по типам индексов с разбивкой на группы счетчиков для ограничения количества счетчиков в запросе.
     * Формат названия индекса: indexTypeName#groupNumber
     * @param {Object} fact - факт
     * @param {string} timeFieldName - имя поля даты факта, по которому будут вычисляться счетчики: либо "c" для факта, либо "dt" для индексных значений
     * @returns {Promise<Array>} выражение для вычисления группы счетчиков для факта и типа индексов
     */
    getFactIndexCountersInfo(fact, timeFieldName = "dt") {
        if (!this._counterProducer) {
            this.logger.warn('mongoProvider.mongoCounters не заданы. Счетчики будут создаваться по умолчанию.');
            return this.getDefaultFactIndexCountersInfo();
        }
        // Получение счетчиков, которые подходят для факта по условию computationConditions
        const factCounters = this._counterProducer.getFactCounters(fact, config.facts.allowedCountersNames);
        if (!factCounters) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }
        // this.logger.debug(`factCounters: ${JSON.stringify(factCounters)}`);
        // Список условий по каждому типу индекса.
        // К имени индекса добавляется постфикс #N, где N - номер группы счетчиков для ограничения количества счетчиков в запросе.
        const indexFacetStages = {};
        const indexLimits = {};
        // Счетчик количества counters в каждой группе {countersCount, groupNumber}
        const countersGroupCount = {};
        // Общее число счетчиков
        let totalCountersCount = 0;
        // Текущая дата и время
        const nowDate = Date.now();
        // Подставляем параметры для каждого счетчика
        factCounters.forEach(counter => {
            totalCountersCount++;
            // Ограничение на число счетчиков, заданное в настройках
            if (config.facts.maxCountersProcessing > 0 && totalCountersCount > config.facts.maxCountersProcessing) {
                return;
            }
            //
            // Условие фильтрации счетчика
            //
            const matchStageCondition = counter.evaluationConditions ? { "$match": counter.evaluationConditions } : { "$match": {} };
            // Нужно добавить условие по fromTimeMs и toTimeMs в matchStage
            const fromDateTime = counter.fromTimeMs ? nowDate - counter.fromTimeMs : null;
            const toDateTime = counter.toTimeMs ? nowDate - counter.toTimeMs : null;
            if (fromDateTime) {
                if (!matchStageCondition["$match"]) {
                    matchStageCondition["$match"] = {};
                }
                matchStageCondition["$match"][timeFieldName] = { "$gte": new Date(fromDateTime) };
            }
            if (toDateTime) {
                if (!matchStageCondition["$match"]) {
                    matchStageCondition["$match"] = {};
                }
                matchStageCondition["$match"][timeFieldName] = { "$lte": new Date(toDateTime) };
            }
            const matchStage = Object.keys(matchStageCondition["$match"]).length ? matchStageCondition : null;
            //
            // Ограничение количества записей для счетчика
            //
            const limitValue = (counter.maxEvaluatedRecords ? (counter.maxMatchingRecords ? Math.min(counter.maxEvaluatedRecords, counter.maxMatchingRecords) : counter.maxEvaluatedRecords) : (counter.maxMatchingRecords ? counter.maxMatchingRecords : null));
            const limitStage = limitValue > 0 ? { "$limit": limitValue } : null;
            //
            // Добавление условия по группировке
            //
            const groupStage = { "$group": counter.attributes };
            // Всегда добавлять идентификатор, по которому выполняется группировка - _id = null
            groupStage["$group"]["_id"] = null;
            //
            // Вычисление номера группы счетчиков в запросе и названия индекса с номером группы
            //
            if (!countersGroupCount[counter.indexTypeName]) {
                countersGroupCount[counter.indexTypeName] = {
                    countersCount: 0, // Общее количество счетчиков в текущей группе
                    groupNumber: 1, // Номер группы счетчиков в запросе
                };
            }
            countersGroupCount[counter.indexTypeName].countersCount++;
            if (config.facts.maxCountersPerRequest > 0 &&countersGroupCount[counter.indexTypeName].countersCount > config.facts.maxCountersPerRequest) {
                countersGroupCount[counter.indexTypeName].groupNumber++;
                countersGroupCount[counter.indexTypeName].countersCount = 1;
            }
            const indexTypeNameWithGroupNumber = counter.indexTypeName + "#" + countersGroupCount[counter.indexTypeName].groupNumber;
            //
            // Добавление счетчика в список счетчиков для текущего индекса
            //
            if (!indexFacetStages[indexTypeNameWithGroupNumber]) {
                indexFacetStages[indexTypeNameWithGroupNumber] = {};
            }
            indexFacetStages[indexTypeNameWithGroupNumber][counter.name] = [];
            if (matchStage) {
                indexFacetStages[indexTypeNameWithGroupNumber][counter.name].push(matchStage);
            }
            if (limitStage) {
                indexFacetStages[indexTypeNameWithGroupNumber][counter.name].push(limitStage);
            }
            indexFacetStages[indexTypeNameWithGroupNumber][counter.name].push(groupStage);
            // Дополнительно обрабатываем счетчики на получение уникальных значений, признаком является $addToSet
            const addFieldsStage = {};
            Object.keys(counter.attributes).forEach(counterName => {
                const counterEvaluation = counter.attributes[counterName];
                if (counterEvaluation && counterEvaluation["$addToSet"]) {
                    // Есть выражение для получения уникальных значений
                    addFieldsStage[counterName] = { "$size": "$" + counterName };
                }
            });
            if (Object.keys(addFieldsStage).length) {
                indexFacetStages[indexTypeNameWithGroupNumber][counter.name].push({ "$addFields": addFieldsStage });
            }
            // Добавление максимального количества записей для группы счетчиков
            if (!indexLimits[indexTypeNameWithGroupNumber]){
                indexLimits[indexTypeNameWithGroupNumber] = {};
            }
            indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords = Math.max(indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords ?? 0, counter.maxEvaluatedRecords ?? 0);
            indexLimits[indexTypeNameWithGroupNumber].fromTimeMs = Math.max(indexLimits[indexTypeNameWithGroupNumber].fromTimeMs ?? 0, counter.fromTimeMs ?? 0);
            indexLimits[indexTypeNameWithGroupNumber].toTimeMs = (counter.toTimeMs > 0 ? (indexLimits[indexTypeNameWithGroupNumber].toTimeMs > 0 ? Math.min(counter.toTimeMs, indexLimits[indexTypeNameWithGroupNumber].toTimeMs) : counter.toTimeMs) : (indexLimits[indexTypeNameWithGroupNumber].toTimeMs > 0 ? indexLimits[indexTypeNameWithGroupNumber].toTimeMs : 0));
        });
        if (config.facts.maxCountersProcessing > 0 && totalCountersCount > config.facts.maxCountersProcessing) {
            this.logger.warn(`Превышено максимальное количество счетчиков для обработки: ${config.facts.maxCountersProcessing}. Всего счетчиков: ${totalCountersCount}.`);
        }

        // Если в выражении счетчиков есть параметры, то заменить их на значения атрибутов из факта
        // Например, если в выражении счетчиков есть параметр "$$f2", то он будет заменен на значение атрибута "f2" из факта
        const indexFacetStagesString = JSON.stringify(indexFacetStages);
        if (!indexFacetStagesString.includes('$$')) {
            return {indexFacetStages: indexFacetStages, indexLimits: indexLimits};
        }
        // Создаем глубокую копию выражения счетчиков
        const parameterizedFacetStages = JSON.parse(indexFacetStagesString);
        // Рекурсивно заменяем переменные в объекте
        this._replaceParametersRecursive(parameterizedFacetStages, fact.d);

        return {indexFacetStages: parameterizedFacetStages, indexLimits: indexLimits};
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
     * Функция возвращает значение поля по пути с точками
     * 
     * @param {Object} obj - объект для обработки
     * @param {string} path - путь к полю c точками
     * @returns {any} значение поля
     */
    _getValueByPath(obj, path) {
        const fields = path.split('.');
        return fields.reduce((acc, field) => acc[field], obj);
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
                        let variableName = value.substring(2); // убираем $$
                        if (variableName.includes('d.')) {
                            variableName = variableName.substring(2); // убираем d.
                        }
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
        const factIndexCollection = this._getFactIndexAggregateCollection();
        const startFindFactIndexTime = Date.now();
        const factIndexResult = await factIndexCollection.find(indexNameQuery.factIndexFindQuery, indexNameQuery.factIndexFindOptions).sort(indexNameQuery.factIndexFindSort).limit(indexNameQuery.depthLimit).toArray();
        const stopFindFactIndexTime = Date.now();
        const relevantFactsQuerySize = debugMode ? JSON.stringify(indexNameQuery.factIndexFindQuery).length : undefined;
        const relevantFactsSize = debugMode ? JSON.stringify(factIndexResult).length : undefined;
        const factIds = factIndexResult.map(item => item._id.f);
        if (this._debugMode) {
            this.logger.debug(`✓ Получены списки ИД фактов: ${JSON.stringify(factIds)} \n`);
        }
        return {
            factIds: factIds,
            metrics: {
                relevantFactsQuerySize: relevantFactsQuerySize,
                relevantFactsTime: stopFindFactIndexTime - startFindFactIndexTime,
                relevantFactsCount: factIndexResult.length,
                relevantFactsSize: relevantFactsSize,
                relevantFactsQuery: {
                    indexQuery: indexNameQuery.factIndexFindQuery,
                    indexSort: indexNameQuery.factIndexFindSort,
                    depthLimit: indexNameQuery.depthLimit,
                }
            },
        };
    }

    async _getCounters(indexTypeNameWithGroupNumber, factIds, indexCounterList, debugMode = false) {
        if (!factIds || !factIds.length) {
            return {
                counters: null,
                error: "No relevant facts found",
                metrics: {
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    countersQuerySize: 0,
                    countersSize: 0,
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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - aggregate",
        };
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(factFacetStage)}`);
        const factsCollection = this._getFactsAggregateCollection();
        const startCountersQueryTime = Date.now();
        try {
            // this.logger.debug(`Агрегационный запрос для индекса ${indexTypeNameWithGroupNumber}: ${JSON.stringify(factFacetStage)}`);
            const countersResult = await factsCollection.aggregate(factFacetStage, aggregateFactOptions).toArray();
            const countersSize = debugMode ? JSON.stringify(countersResult[0]).length : undefined;
            return {
                counters: countersResult[0],
                metrics: {
                    countersQuerySize: countersQuerySize,
                    countersQueryTime: Date.now() - startCountersQueryTime,
                    countersQueryCount: 1,
                    countersSize: countersSize,
                    countersQuery: factFacetStage,
                }
            };
        } catch (error) {
            this.logger.error(`Ошибка при выполнении запроса для ключа ${indexTypeNameWithGroupNumber}: ${error.message}`);
            this._writeToLogFile(`Ошибка при выполнении запроса для ключа ${indexTypeNameWithGroupNumber}: ${error.message}`);
            if (this._debugMode) {
                this.logger.debug(JSON.stringify(factFacetStage, null, 2));
            }

            return {
                counters: null,
                error: error.message,
                metrics: {
                    countersQueryTime: Date.now() - startCountersQueryTime,
                    countersQueryCount: 1,
                    countersQuerySize: countersQuerySize,
                    countersSize: 0,
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
        if (this._includeFactDataToIndex && this._lookupFacts) {
            this.logger.warn(`getRelevantFactCounters: Параметры includeFactDataToIndex и lookupFacts не могут быть true одновременно.`);
        }
        if (this._includeFactDataToIndex || this._lookupFacts) {
            return await this.getRelevantFactCountersFromIndex(factIndexInfos, fact, depthLimit, depthFromDate, this._lookupFacts, debugMode);
        } else {
            return await this.getRelevantFactCountersFromFact(factIndexInfos, fact, depthLimit, depthFromDate, debugMode);
        }
    }

    async getRelevantFactCountersFromFact(factIndexInfos, fact = undefined, depthLimit = 2000, depthFromDate = undefined, debugMode = false) {
        if (!factIndexInfos || !factIndexInfos.length) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет индексных значений.`);

            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    info: "У факта нет индексных значений",
                },
            };
        }
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`getRelevantFactCountersFromFact: Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        // Так как счетчики будут по списку фактов, то используем поле "c", а не "dt"
        const info = this.getFactIndexCountersInfo(fact, "c");

        if (!info || !info.indexFacetStages || typeof info.indexFacetStages !== 'object') {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);

            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    info: "Для данных факта нет подходящих счетчиков.",
                },
            };
        }

        const indexCountersInfo = info.indexFacetStages;
        const indexLimits = info.indexLimits;

        // Перебираем все индексы, по которым нужно построить счетчики и формируем агрегационный запрос
        const queriesByIndexName = {};
        const indexTypeNames = new Set();
        const nowDate = Date.now();

        // this.logger.info(`*** Индексы счетчиков: ${JSON.stringify(indexCountersInfo)}`);
        // this.logger.info(`*** Получено ${Object.keys(indexCountersInfo).length} типов индексов счетчиков: ${Object.keys(indexCountersInfo).join(', ')}`);
        Object.keys(indexCountersInfo).forEach((indexTypeNameWithGroupNumber) => {
            const indexTypeName = indexTypeNameWithGroupNumber.split('#')[0];
            // const groupNumber = parseInt(indexTypeNameWithGroupNumber.split('#')[1] ?? 0);
            // Если тип индекса уже был обработан, то пропускаем
            if(indexTypeNames.has(indexTypeName)) {
                // Нужно обновить глубину запроса для типа индекса, если она меньше максимальной глубины для группы счетчиков
                queriesByIndexName[indexTypeName].depthLimit = Math.max(queriesByIndexName[indexTypeName].depthLimit, indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords);
                return;
            }
            const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
            if (!indexInfo) {
                this.logger.warn(`getRelevantFactCountersFromFact: Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
                return;
            }
            const factIndexFindQuery = {
                "_id.h": indexInfo.hashValue
            };
            if (depthFromDate || indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0) {
                const fromDateTime = indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0 ? nowDate - indexLimits[indexTypeNameWithGroupNumber].fromTimeMs : (depthFromDate ? depthFromDate.getTime() : nowDate);
                factIndexFindQuery["dt"] = {
                    "$gte": new Date(fromDateTime)
                };
            }
            if (indexLimits[indexTypeNameWithGroupNumber].toTimeMs > 0) {
                if (!factIndexFindQuery["dt"]) {
                    factIndexFindQuery["dt"] = {};
                }
                factIndexFindQuery["dt"]["$lte"] = new Date( nowDate - indexLimits[indexTypeNameWithGroupNumber].toTimeMs);
            }
            if (fact) {
                factIndexFindQuery["_id.f"] = {
                    "$ne": fact._id
                };
            }
            indexTypeNames.add(indexTypeName);
            const limitValue = indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords ? (indexInfo.index.limit ? Math.max(indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords, indexInfo.index.limit) : indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords) : (indexInfo.index.limit ? indexInfo.index.limit : 100);

            queriesByIndexName[indexTypeName] = {
                factIndexFindQuery: factIndexFindQuery,
                factIndexFindOptions: {
                    batchSize: config.database.batchSize,
                    readConcern: this._databaseOptions.readConcern,
                    readPreference: this._databaseOptions.aggregateReadPreference,
                    comment: "getRelevantFactsByIndex - find",
                    projection: { "_id.f": 1 }
                },
                factIndexFindSort: {
                    "_id.h": 1,
                    "dt": -1
                },
                depthLimit: Math.min(limitValue, depthLimit),
            };
        });

        if (Object.keys(queriesByIndexName).length === 0) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные счетчики. Счетчики не будут вычисляться.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    info: "Для подходящих счетчиков нет индексных значений. Вычисление счетчиков невозможно.",
                    includeFactDataToIndex: this._includeFactDataToIndex,
                    lookupFacts: this._lookupFacts,
                    totalIndexCount: factIndexInfos?.length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    maxCountersPerRequest: config.facts.maxCountersPerRequest,
                    maxCountersProcessing: config.facts.maxCountersProcessing,
                    counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                    relevantIndexCount: 0,
                    queryCountersCount: 0,
                    relevantFactsCount: 0,
                    relevantFactsSize: 0,
                    relevantFactsTime: 0,
                    prepareCountersQueryTime: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    resultCountersCount: 0,
                    countersMetrics: null
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
        // Сначала перебираем индексы и получаем факты, а потом через промисы получаем счетчики по фактам (вложенные промисы)
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

            // Получаем список групп счетчиков для данного типа индекса
            const indexTypeNamesWithGroupNumbers = Object.keys(indexCountersInfo).filter(key => key.startsWith(indexTypeName + '#'));

            const countersPromises = indexTypeNamesWithGroupNumbers.map(async (indexTypeNameWithGroupNumber) => {
                const counters = indexCountersInfo[indexTypeNameWithGroupNumber] ? indexCountersInfo[indexTypeNameWithGroupNumber] : {};
                if (debugMode) {
                    this.logger.debug(`Обрабатываются счетчики (${Object.keys(counters).length}) для типа индекса ${indexTypeNameWithGroupNumber} для факта ${fact?._id}: ${Object.keys(counters).join(', ')}`);
                }
                const countersResult = await this._getCounters(indexTypeNameWithGroupNumber, indexNameResult.factIds, indexCountersInfo[indexTypeNameWithGroupNumber], debugMode);
                return {
                    indexTypeNameWithGroupNumber: indexTypeNameWithGroupNumber,
                    queryCountersCount: Object.keys(counters).length,
                    counters: countersResult.counters,
                    error: countersResult.error,
                    metrics: countersResult.metrics,
                };
            });

            const countersResult = await Promise.all(countersPromises);
            // Объединение результатов в один объект
            const mergedResult = {
                indexTypeName: indexTypeName,
                processingTime: Date.now() - startQuery,
                factsMetrics: indexNameResult.metrics,
                queryCountersCount: countersResult ? countersResult.reduce((a, b) => a + b.queryCountersCount, 0) : 0,
                counters: {},
                countersErrors: {},
                countersMetrics: {},
            };
            countersResult.forEach(result => {
                Object.assign(mergedResult.counters, result.counters);
                mergedResult.countersErrors[result.indexTypeNameWithGroupNumber] = result.error;
                mergedResult.countersMetrics[result.indexTypeNameWithGroupNumber] = result.metrics;
            });
            return mergedResult;
        });

        // Ждем выполнения всех запросов
        const startQueriesTime = Date.now();
        const queryResults = await Promise.all(queryPromises);
        const stopQueriesTime = Date.now();

        // Объединяем результаты в один JSON объект
        const queryCountersCount = queryResults ? queryResults.reduce((a, b) => a + b.queryCountersCount, 0) : 0;
        const mergedCounters = {};
        const details = {};
        let relevantFactsQuerySize = 0;
        let relevantFactsTime = 0;
        let relevantFactsCount = 0;
        let relevantFactsSize = 0;
        let countersQuerySize = 0;
        let countersQueryTime = 0;
        let countersQueryCount = 0;
        let countersSize = 0;
        queryResults.forEach((result) => {
            if (result.counters) {
                Object.assign(mergedCounters, result.counters);
            }
            details[result.indexTypeName] = {
                processingTime: result.processingTime,
                factsMetrics: result.factsMetrics,
                factsDebug: result.factsDebug,
                countersErrors: result.countersErrors,
                countersMetrics: result.countersMetrics,
                countersDebug: result.countersDebug,
            };
            relevantFactsQuerySize += result.factsMetrics.relevantFactsQuerySize ?? 0;
            relevantFactsTime = Math.max(relevantFactsTime, result.factsMetrics.relevantFactsTime ?? 0);
            relevantFactsCount += result.factsMetrics.relevantFactsCount ?? 0;
            relevantFactsSize += result.factsMetrics.relevantFactsSize ?? 0;

            countersQuerySize += result.countersMetrics ? Object.keys(result.countersMetrics).reduce((a, b) => a + (result.countersMetrics[b].countersQuerySize ?? 0), 0) : 0;
            countersQueryTime = Math.max(countersQueryTime, Math.max(...Object.keys(result.countersMetrics).map(key => result.countersMetrics[key].countersQueryTime ?? 0)));
            countersQueryCount += result.countersMetrics ? Object.keys(result.countersMetrics).reduce((a, b) => a + (result.countersMetrics[b].countersQueryCount ?? 0), 0) : 0;
            countersSize += result.countersMetrics ? Object.keys(result.countersMetrics).reduce((a, b) => a + (result.countersMetrics[b].countersSize ?? 0), 0) : 0;
        });

        if (this._debugMode) {
            this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);
        }

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions и применения ограничений)
         * maxCountersPerRequest - максимальное количество счетчиков на запрос (из файла конфигурации)
         * maxCountersProcessing - максимальное количество счетчиков для обработки (из файла конфигурации)
         * counterIndexCountWithGroup - количество индексов с счетчиками, применимых к факту с учетом разбивки на группы (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * queryCountersCount - количество запрошенных счетчиков в запросах на вычисление счетчиков
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
         * resultCountersCount - количество полученных счетчиков, которые были вычислены
         * details - метрики выполнения запросов в разрезе индексов и групп индексов
         * 
         */

        // Возвращаем массив статистики
        return {
            result: mergedCounters,
            processingTime: Date.now() - startTime,
            metrics: {
                includeFactDataToIndex: this._includeFactDataToIndex,
                lookupFacts: this._lookupFacts,
                totalIndexCount: factIndexInfos?.length,
                factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                maxCountersPerRequest: config.facts.maxCountersPerRequest,
                maxCountersProcessing: config.facts.maxCountersProcessing,
                counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                relevantIndexCount: queriesByIndexName ? Object.keys(queriesByIndexName).length : 0,
                queryCountersCount: queryCountersCount,
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
                resultCountersCount: mergedCounters ? Object.keys(mergedCounters).length : 0,
                details: details,
            },
            debug: {
                counters: mergedCounters,
            }
        };
    }

    // lookupFacts - получение данных через факты (true) или через индексы (false)
    async getRelevantFactCountersFromIndex(factIndexInfos, fact = undefined, depthLimit = 1000, depthFromDate = undefined, lookupFacts, debugMode = false) {
        this.checkConnection();
        const startTime = Date.now();

        this.logger.debug(`getRelevantFactCountersFromIndex: Получение счетчиков релевантных фактов для факта ${fact?._id} с глубиной от даты: ${depthFromDate}, последние ${depthLimit} фактов`);
        this.logger.debug(`getRelevantFactCountersFromIndex: includeFactDataToIndex: ${this._includeFactDataToIndex}, lookupFacts: ${this._lookupFacts}`);

        // Получение выражения для вычисления счетчиков и списка уникальных типов индексов
        // Так как счетчики будут по списку фактов, то используем поле "dt", а не "c"
        const info = this.getFactIndexCountersInfo(fact, "dt");

        if (!info || !info.indexFacetStages || typeof info.indexFacetStages !== 'object') {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} нет подходящих счетчиков.`);

            return {
                result: [],
                processingTime: Date.now() - startTime,
            };
        }

        const indexCountersInfo = info.indexFacetStages;
        const indexLimits = info.indexLimits;

        // Перебираем все индексы, по которым нужно построить счетчики и формируем агрегационный запрос
        const queriesByIndexName = {};
        const nowDate = Date.now();
        
        // this.logger.info(`*** Индексы счетчиков: ${JSON.stringify(indexCountersInfo)}`);
        // this.logger.info(`*** Получено ${Object.keys(indexCountersInfo).length} типов индексов счетчиков: ${Object.keys(indexCountersInfo).join(', ')}`);
        Object.keys(indexCountersInfo).forEach((indexTypeNameWithGroupNumber) => {
            const indexTypeName = indexTypeNameWithGroupNumber.split('#')[0];
            // const groupNumber = parseInt(indexTypeNameWithGroupNumber.split('#')[1] ?? 0);
            const indexCounterList = indexCountersInfo[indexTypeNameWithGroupNumber] ? indexCountersInfo[indexTypeNameWithGroupNumber] : {};
            this.logger.debug(`Обрабатываются счетчики (${Object.keys(indexCounterList).length}) для типа индекса ${indexTypeNameWithGroupNumber} для факта ${fact?._id}: ${Object.keys(indexCounterList).join(', ')}`);
            const indexInfo = factIndexInfos.find(info => info.index.indexTypeName === indexTypeName);
            if (!indexInfo) {
                this.logger.warn(`getRelevantFactCountersFromIndex: Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
                return;
            }
            const match = {
                "_id.h": indexInfo.hashValue
            };
            if (depthFromDate || indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0) {
                const fromDateTime = indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0 ? nowDate - indexLimits[indexTypeNameWithGroupNumber].fromTimeMs : (depthFromDate ? depthFromDate.getTime() : nowDate);
                match["dt"] = {
                    "$gte": new Date(fromDateTime)
                };
            }
            if (indexLimits[indexTypeNameWithGroupNumber].toTimeMs > 0) {
                if (!match["dt"]) {
                    match["dt"] = {};
                }
                match["dt"]["$lte"] = new Date( nowDate - indexLimits[indexTypeNameWithGroupNumber].toTimeMs);
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
            const limitValue = indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords ? (indexInfo.index.limit ? Math.max(indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords, indexInfo.index.limit) : indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords) : (indexInfo.index.limit ? indexInfo.index.limit : 100);
            const limit = Math.min(limitValue, depthLimit);
            const aggregateIndexQuery = [
                { "$match": match },
                { "$sort": sort },
                { "$limit": limit }
            ];

            if (lookupFacts) {
                aggregateIndexQuery.push({
                    "$lookup": {
                        from: "facts",
                        localField: "_id.f",
                        foreignField: "_id",
                        let: { "factId": "$_id.f" },
                        pipeline: [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$_id", "$$factId"] }
                                }
                            }
                        ],
                        as: "facts"
                    }
                });
                aggregateIndexQuery.push({ $unwind: '$facts' });
                aggregateIndexQuery.push({
                    "$project": {
                        _id: "$facts._id",
                        c: "$facts.c",
                        t: "$facts.t",
                        d: "$facts.d",
                        dt: 1
                    }
                });
            }

            const projectState = {};
            Object.keys(indexCounterList).forEach(counterName => {
                projectState[counterName] = { "$arrayElemAt": ["$" + counterName, 0] };
            });
            aggregateIndexQuery.push(...[
                { "$facet": indexCounterList },
                { "$project": projectState }
            ]);

            queriesByIndexName[indexTypeNameWithGroupNumber] = {
                query: aggregateIndexQuery,
                indexTypeName: indexTypeName,
            };
        });

        if (Object.keys(queriesByIndexName).length === 0) {
            this.logger.warn(`Для указанного факта ${fact?._id} с типом ${fact?.t} не найдены релевантные счетчики. Счетчики не будут вычисляться.`);
            return {
                result: {},
                processingTime: Date.now() - startTime,
                metrics: {
                    includeFactDataToIndex: this._includeFactDataToIndex,
                    lookupFacts: this._lookupFacts,
                    totalIndexCount: factIndexInfos?.length,
                    factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                    maxCountersPerRequest: config.facts.maxCountersPerRequest,
                    maxCountersProcessing: config.facts.maxCountersProcessing,
                    counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                    relevantIndexCount: 0,
                    queryCountersCount: 0,
                    prepareCountersQueryTime: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryCount: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryTime: 0,
                    countersQuerySize: 0,
                    countersQueryTime: 0,
                    countersQueryCount: 0,
                    countersSize: 0,
                    resultCountersCount: 0,
                    countersMetrics: null
                },
                debug: {
                    indexQuery: null,
                    countersQuery: null,
                }
            };
        }

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareQueriesTime = Date.now();
        const queryPromises = Object.keys(queriesByIndexName).map(async (indexTypeNameWithGroupNumber) => {
            // const indexTypeName = indexTypeNameWithGroupNumber.split('#')[0];
            // const groupNumber = parseInt(indexTypeNameWithGroupNumber.split('#')[1] ?? 0);
            const indexNameQuery = queriesByIndexName[indexTypeNameWithGroupNumber].query;
            const startQuery = Date.now();

            const aggregateOptions = {
                batchSize: config.database.batchSize,
                readConcern: this._databaseOptions.readConcern,
                readPreference: this._databaseOptions.aggregateReadPreference,
                comment: "getRelevantFactCounters - aggregate",
            };
            // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(indexNameQuery)}`);
            const factIndexCollection = this._getFactIndexAggregateCollection();
            const startCountersQueryTime = Date.now();
            const countersQuerySize = debugMode ? JSON.stringify(indexNameQuery).length : undefined;
            try {
                if (this._debugMode) {
                    this.logger.debug(`Агрегационный запрос для индекса ${indexTypeNameWithGroupNumber}: ${JSON.stringify(indexNameQuery)}`);
                }
                const countersResult = await factIndexCollection.aggregate(indexNameQuery, aggregateOptions).toArray();
                const countersSize = debugMode ? JSON.stringify(countersResult[0]).length : undefined;
                return {
                    indexTypeName: indexTypeNameWithGroupNumber,
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
                this.logger.error(`Ошибка при выполнении запроса для индекса ${indexTypeNameWithGroupNumber}: ${error.message}`);
                this._writeToLogFile(`Ошибка при выполнении запроса для индекса ${indexTypeNameWithGroupNumber}: ${error.message}`);
                this._writeToLogFile(JSON.stringify(indexNameQuery, null, 2));
                return {
                    indexTypeName: indexTypeNameWithGroupNumber,
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
        const countersMetrics = {};
        let countersQuerySize = 0;
        let countersQueryTime = 0;
        let countersCount = 0;
        let countersSize = 0;
        const countersQuery = {};
        queryResults.forEach((result) => {
            if (result.counters) {
                Object.assign(mergedCounters, result.counters);
            }
            countersMetrics[result.indexTypeName] = {
                processingTime: result.processingTime,
                metrics: result.metrics
            };
            countersQuerySize += result.metrics.countersQuerySize ?? 0;
            countersQueryTime = Math.max(countersQueryTime, result.metrics.countersQueryTime ?? 0);
            countersCount += result.metrics.countersQueryCount ?? 0;
            countersSize += result.metrics.countersSize ?? 0;
            countersQuery[result.indexTypeName] = result.debug.countersQuery;
        });

        if (this._debugMode) {
            this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);
        }

        const queryCountersCount = queriesByIndexName ? Object.keys(queriesByIndexName).map(key => queriesByIndexName[key] ? Object.keys(queriesByIndexName[key].query["$facet"] ?? {})?.length : 0).reduce((a, b) => a + b, 0) : 0;

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions)
         * maxCountersPerRequest - максимальное количество счетчиков на запрос (из файла конфигурации)
         * maxCountersProcessing - максимальное количество счетчиков для обработки (из файла конфигурации)
         * counterIndexCountWithGroup - количество индексов с счетчиками, применимых к факту с учетом разбивки на группы (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * queryCountersCount - количество запрошенных счетчиков в запросах
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
         * resultCountersCount - количество полученных счетчиков, которые были вычислены
         * countersMetrics - метрики выполнения запросов в разрезе индексов
         * 
         * indexQuery - запрос по индексам для поиска ИД релевантных фактов
         * countersQuery - запрос на вычисление счетчиков по релевантным фактам
         */

        // Возвращаем массив статистики
        
        return {
            result: mergedCounters,
            processingTime: Date.now() - startTime,
            metrics: {
                includeFactDataToIndex: this._includeFactDataToIndex,
                lookupFacts: this._lookupFacts,
                totalIndexCount: factIndexInfos?.length,
                factCountersCount: indexCountersInfo ? Object.keys(indexCountersInfo).map(key => indexCountersInfo[key] ? Object.keys(indexCountersInfo[key])?.length : 0).reduce((a, b) => a + b, 0) : 0,
                maxCountersPerRequest: config.facts.maxCountersPerRequest,
                maxCountersProcessing: config.facts.maxCountersProcessing,
                counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                relevantIndexCount: queriesByIndexName ? Object.keys(queriesByIndexName).length : 0,
                queryCountersCount: queryCountersCount,
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
                resultCountersCount: Object.keys(mergedCounters).length,
                countersMetrics: countersMetrics,
            },
            debug: {
                indexQuery: undefined,
                countersQuery: countersQuery,
                counters: mergedCounters
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
                this.logger.warn(`oldGetRelevantFactCounters: Тип индекса ${indexTypeName} не найден в списке индексных значений факта ${fact?._id}.`);
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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - index aggregate",
        };

        // this.logger.debug(`Агрегационный запрос на список ИД фактов в разрезе индексов: ${JSON.stringify(aggregateIndexQuery)}\n`);
        const factIndexCollection = this._getFactIndexAggregateCollection();
        const startrelevantFactsTime = Date.now();
        const factIndexResult = await factIndexCollection.aggregate(aggregateIndexQuery, aggregateIndexOptions).toArray();
        const stoprelevantFactsTime = Date.now();
        const relevantFactsSize = debugMode ? JSON.stringify(factIndexResult).length : undefined;
        const factIdsByIndexName = factIndexResult[0];
        // this.logger.debug(`✓ Получены списки ИД фактов: ${JSON.stringify(factIdsByIndexName)} \n`);

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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - aggregate",
        };
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(factFacetStage)}`);
        //
        // Запускаем параллельно запросы в factFacetStage:
        //
        const factsCollection = this._getFactsAggregateCollection();

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareCountersQueryTime = Date.now();
        const queryPromises = Object.keys(factFacetStage).map(async (indexName) => {
            try {
                // this.logger.debug(`Агрегационный запрос для индекса ${indexName}: ${JSON.stringify(factFacetStage[indexName])}`);
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
        // this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);

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
                this.logger.warn(`oldGetRelevantFactCounters: Тип индекса ${item} не найден в списке индексных значений.`);
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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1, "it": 1 }
        };
        // Создаем локальные ссылки на коллекции для этого запроса
        const factIndexCollection = this._getFactIndexAggregateCollection();
        const factsCollection = this._getFactsAggregateCollection();

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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - aggregate",
        };

        // Выполнить агрегирующий запрос
        // this.logger.info(`Опции агрегирующего запроса: ${JSON.stringify(aggregateOptions)}`);
        // this.logger.info(`Агрегационный запрос: ${JSON.stringify(aggregateQuery)}`);
        const result = await factsCollection.aggregate(aggregateQuery, aggregateOptions).toArray();
        // this.logger.debug(`✓ Получена статистика по фактам: ${JSON.stringify(result)} `);

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
                this.logger.warn(`getRelevantFacts: Тип индекса ${item} не найден в списке индексных значений.`);
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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - find",
            projection: { "_id": 1 }
        };
        // Создаем локальные ссылки на коллекции для этого запроса
        const factIndexCollection = this._getFactIndexAggregateCollection();
        const factsCollection = this._getFactsAggregateCollection();

        // this.logger.debug("   matchQuery: "+JSON.stringify(matchQuery));
        const relevantFactIds = await factIndexCollection.find(matchQuery, findOptions).sort({ dt: -1 }).batchSize(config.database.batchSize).limit(depthLimit).toArray();
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
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - aggregate",
        };
        const result = await factsCollection.aggregate(aggregateQuery, aggregateOptions).batchSize(config.database.batchSize).toArray();
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
    async saveLog(processId, message, fact, processingTime, metrics, debugInfo) {
        this.checkConnection();
        const logCollection = this._getLogCollection();
        try {
            await logCollection.insertOne({
                _id: new ObjectId(),
                c: new Date(),
                p: String(processId),
                msg: message,
                f: fact,
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
                readConcern: this._databaseOptions.readConcern,
                readPreference: "primary",
                writeConcern: this._databaseOptions.writeConcern,
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
                batchSize: config.database.batchSize,
                readConcern: this._databaseOptions.readConcern,
                readPreference: this._databaseOptions.aggregateReadPreference,
                comment: "findFacts"
            };
            const facts = await factsCollection.find(filter, findOptions).toArray();
            // this.logger.debug(`Найдено ${facts.length} фактов по фильтру:`, JSON.stringify(filter));
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
                readConcern: this._databaseOptions.readConcern,
                readPreference: {"mode": "primary"},
                writeConcern: this._databaseOptions.writeConcern,
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
                readConcern: this._databaseOptions.readConcern,
                readPreference: {"mode": "primary"},
                writeConcern: this._databaseOptions.writeConcern,
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
                this.logger.debug(`✓ База данных ${this._databaseName} работает в режиме шардирования. Шарды: ${result.shards.map(shard => shard._id).join(', ')}`);
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
                shardCollection: `${this._databaseName}.${collectionName}`,
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
                    key: { "_id.h": 1, "dt": 1 },
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
            // Определяем схему валидации JSON для коллекции log
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
                        f: {
                            bsonType: "object",
                            description: "JSON объект с фактом"
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
            adminClient = new MongoClient(this._connectionString);
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

            if (await this._isDatabaseCreated(adminDb, this._databaseName)) {
                this.logger.debug(`База данных ${this._databaseName} уже создана`);
                return results;
            }

            this.logger.debug('\n=== Создание базы данных ===');
            this.logger.debug(`База данных: ${this._databaseName}`);
            this.logger.debug(`Коллекция facts: ${this.FACT_COLLECTION_NAME}`);
            this.logger.debug(`Коллекция factIndex: ${this.FACT_INDEX_COLLECTION_NAME}`);

            // 0. Подготовка к созданию базы данных
            this.logger.debug('\n0. Подготовка к созданию базы данных...');
            this._enableSharding(adminDb, this._databaseName);

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