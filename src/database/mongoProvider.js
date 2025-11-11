/**
 * MongoDB Node Driver:
 * https://www.mongodb.com/docs/drivers/node/current/
 */
const { MongoClient, ObjectId } = require('mongodb');
const Logger = require('../common/logger');
const fs = require('fs');
const path = require('path');
const config = require('../common/config');
const connectionPoolStatus = require('./connectionPool');
const QueryDispatcher = require('./queryDispatcher');


/**
 * Класс-провайдер для работы с MongoDB коллекциями facts и factIndex
 * Содержит методы для управления подключением, схемами, вставкой данных, запросами и статистикой
 */
class MongoProvider {
    // Настройки по умолчанию
    DEFAULT_OPTIONS = {
        // Создавать отдельный объект коллекции для каждого запроса
        individualCollectionObject: false,
        // Не сохранять данные в коллекции
        disableSave: false,
        // Читаем всегда локальную копию данных
        // https://www.mongodb.com/docs/manual/reference/read-concern/
        readConcern: { "level": "local" },
        aggregateReadPreference: { "mode": "secondaryPreferred" },
        // Журнал сбрасывается на диск в соответствии с политикой журналирования сервера (раз в 100 мс)
        // https://www.mongodb.com/docs/manual/core/journaling/#std-label-journal-process
        // Параметр на сервере: storage.journal.commitIntervalMs
        // https://www.mongodb.com/docs/manual/reference/configuration-options/#mongodb-setting-storage.journal.commitIntervalMs
        // WRITE_CONCERN = { w: "majority", j: false, wtimeout: 5000 };
        writeConcern: { "w": 1, "j": false, "wtimeout": 5000 },
        minPoolSize: 100,
        maxPoolSize: 200,
        maxIdleTimeMS: 0,
        noDelay: true,
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

        this._debugMode = config.logging.debugMode || config.logging.logLevel.toUpperCase() === 'DEBUG';
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

        // Переменные для мониторинга connection pool
        this._counterClientPoolStatus = null;
        this._aggregateClientPoolStatus = null;
        this._metricsCollector = null;

        // Диспетчер запросов для агрегаций через пул процессов
        this._queryDispatcher = null;
        this._queryDispatcherDisabled = false;

        //
        this.logger.debug("*** Создан новый MongoProvider");
    }

    /**
     * Устанавливает коллектор метрик для мониторинга connection pool
     * @param {Object} metricsCollector - коллектор метрик
     */
    setMetricsCollector(metricsCollector) {
        this._metricsCollector = metricsCollector;
        this.logger.debug('Metrics collector установлен для мониторинга connection pool');

        // Если клиенты уже подключены, переинициализируем мониторинг с новым коллектором
        if (this._isConnected && (this._counterClient || this._aggregateClient)) {
            this.logger.debug('Переинициализация мониторинга connection pool с установленным metricsCollector');

            // Очищаем старые обработчики перед переинициализацией
            if (this._counterClientPoolStatus && typeof this._counterClientPoolStatus.cleanUp === 'function') {
                this._counterClientPoolStatus.cleanUp();
            }
            if (this._aggregateClientPoolStatus && typeof this._aggregateClientPoolStatus.cleanUp === 'function') {
                this._aggregateClientPoolStatus.cleanUp();
            }

            this._initConnectionPoolMonitoring();
        }
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
        // Опции для подключения по умолчанию
        const options = {
            readConcern: databaseOptions.readConcern,
            readPreference: { "mode": "primary" },
            writeConcern: databaseOptions.writeConcern,
            appName: "CounterTest",
            // monitorCommands: true,
            minPoolSize: databaseOptions.minPoolSize,
            maxPoolSize: databaseOptions.maxPoolSize,
            maxIdleTimeMS: databaseOptions.maxIdleTimeMS,
            noDelay: databaseOptions.noDelay,
            maxConnecting: databaseOptions.maxConnecting,
            serverSelectionTimeoutMS: 60000,
        };
        if (databaseOptions.compressor) {
            options.compressors = databaseOptions.compressor;
        }
        if (databaseOptions.compressionLevel !== undefined && databaseOptions.compressionLevel !== null) {
            options.zlibCompressionLevel = databaseOptions.compressionLevel;
        }
        try {
            return new MongoClient(connectionString, options);
        } catch (error) {
            throw error;
        }
    }

    _getMongoClientAggregate(connectionString, databaseOptions) {
        // Опции для подключения по умолчанию
        const options = {
            readConcern: databaseOptions.readConcern,
            readPreference: databaseOptions.aggregateReadPreference,
            writeConcern: databaseOptions.writeConcern,
            appName: "CounterTest",
            // monitorCommands: true,
            minPoolSize: databaseOptions.minPoolSize,
            maxPoolSize: databaseOptions.maxPoolSize,
            maxIdleTimeMS: databaseOptions.maxIdleTimeMS,
            noDelay: databaseOptions.noDelay,
            maxConnecting: databaseOptions.maxConnecting,
            serverSelectionTimeoutMS: 60000,
        };
        if (databaseOptions.compressor) {
            options.compressors = databaseOptions.compressor;
        }
        if (databaseOptions.compressionLevel !== undefined && databaseOptions.compressionLevel !== null) {
            options.zlibCompressionLevel = databaseOptions.compressionLevel;
        }
        try {
            return new MongoClient(connectionString, options);
        } catch (error) {
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
            this.logger.debug(`Настройки: \n${JSON.stringify(this._databaseOptions, null, 2)}`);

            this._counterClient = await this._getMongoClient(this._connectionString, this._databaseOptions);
            await this._counterClient.connect();
            this._aggregateClient = await this._getMongoClientAggregate(this._connectionString, this._databaseOptions);
            await this._aggregateClient.connect();
            this._counterDb = this._counterClient.db(this._databaseName);
            this._aggregateDb = this._aggregateClient.db(this._databaseName);

            this._isConnected = true;

            // Инициализируем мониторинг connection pool для обоих клиентов
            this._initConnectionPoolMonitoring();

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
     * Инициализирует мониторинг connection pool для обоих клиентов
     */
    _initConnectionPoolMonitoring() {
        try {
            // Мониторинг для counter client
            this._counterClientPoolStatus = connectionPoolStatus(
                this._counterClient,
                'counter',
                this._metricsCollector,
                this.logger
            );

            // Мониторинг для aggregate client
            this._aggregateClientPoolStatus = connectionPoolStatus(
                this._aggregateClient,
                'aggregate',
                this._metricsCollector,
                this.logger
            );

            this.logger.info('✓ Мониторинг connection pool инициализирован для обоих клиентов');
        } catch (error) {
            this.logger.error('✗ Ошибка инициализации мониторинга connection pool:', error.message);
        }
    }

    /**
     * Возвращает (инициализируя при необходимости) диспетчер запросов
     * для выполнения агрегаций через пул процессов
     * @returns {QueryDispatcher|null}
     */
    _getQueryDispatcher() {
        if (this._queryDispatcher) {
            return this._queryDispatcher;
        }

        if (this._queryDispatcherDisabled) {
            return null;
        }

        const workerCount = Math.max(1, parseInt(config.queryDispatcher?.workerCount, 10) || 1);

        if (workerCount <= 1) {
            this._queryDispatcherDisabled = true;
            this.logger.debug('QueryDispatcher не используется: workerCount <= 1');
            return null;
        }

        try {
            this._queryDispatcher = new QueryDispatcher({
                workerCount,
                connectionString: this._connectionString,
                databaseName: this._databaseName,
                databaseOptions: this._databaseOptions,
                logger: this.logger
            });
            this._queryDispatcherDisabled = false;
            this.logger.debug(`✓ QueryDispatcher инициализирован (workerCount=${workerCount})`);
        } catch (error) {
            this._queryDispatcherDisabled = true;
            this.logger.error(`✗ Ошибка инициализации QueryDispatcher: ${error.message}`);
            this._writeToLogFile(`Ошибка инициализации QueryDispatcher: ${error.message}`);
            return null;
        }

        return this._queryDispatcher;
    }

    /**
     * Отключение от MongoDB
     */
    async disconnect() {
        try {
            if (this._queryDispatcher) {
                try {
                    await this._queryDispatcher.shutdown();
                    this.logger.debug('✓ QueryDispatcher остановлен');
                } catch (dispatcherError) {
                    this.logger.error(`✗ Ошибка при остановке QueryDispatcher: ${dispatcherError.message}`);
                } finally {
                    this._queryDispatcher = null;
                }
            }
            this._queryDispatcherDisabled = false;

            // Очищаем мониторинг connection pool
            if (this._counterClientPoolStatus && typeof this._counterClientPoolStatus.cleanUp === 'function') {
                this._counterClientPoolStatus.cleanUp();
                this._counterClientPoolStatus = null;
            }
            if (this._aggregateClientPoolStatus && typeof this._aggregateClientPoolStatus.cleanUp === 'function') {
                this._aggregateClientPoolStatus.cleanUp();
                this._aggregateClientPoolStatus = null;
            }

            // Закрываем клиенты
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
            return this._counterClient.db(this._databaseName).collection(this.FACT_COLLECTION_NAME);
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
            return this._aggregateClient.db(this._databaseName).collection(this.FACT_COLLECTION_NAME);
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
            return this._counterClient.db(this._databaseName).collection(this.FACT_INDEX_COLLECTION_NAME);
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
            return this._aggregateClient.db(this._databaseName).collection(this.FACT_INDEX_COLLECTION_NAME);
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
        if (this._logCollection && !this._databaseOptions.individualCollectionObject) {
            return this._logCollection;
        }
        if (this._databaseOptions.individualCollectionObject) {
            return this._counterClient.db(this._databaseName).collection(this.LOG_COLLECTION_NAME);
        }
        this._logCollection = this._counterDb.collection(this.LOG_COLLECTION_NAME);
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
                const doc = await factsCollection.findOne(filter, {}, findOptions);
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
                    readPreference: { "mode": "primary" },
                    writeConcern: this._databaseOptions.writeConcern,
                    comment: "saveFactIndexList - bulk",
                    ordered: false,
                    upsert: true,
                };
                const indexBulk = factIndexCollection.initializeUnorderedBulkOp(bulkWriteOptions);

                factIndexValues.forEach(indexValue => {
                    // Используем уникальный индекс {h: 1, f: 1} для поиска и обновления
                    const indexFilter = {
                        h: indexValue.h,
                        f: indexValue.f
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
                    readPreference: { "mode": "primary" },
                    writeConcern: this._databaseOptions.writeConcern,
                    comment: "saveFactIndexList - update",
                    upsert: true,
                };
                // Обновление индексных значений параллельно через Promises
                const updatePromises = factIndexValues.map(async (indexValue) => {
                    const startTime = Date.now();
                    try {
                        // Используем уникальный индекс {h: 1, f: 1} для поиска и обновления
                        const result = await factIndexCollection.updateOne({ h: indexValue.h, f: indexValue.f }, { $set: indexValue }, updateOptions);
                        return {
                            writeError: result.writeErrors,
                            upsertedCount: result.upsertedCount || 0,
                            modifiedCount: result.modifiedCount || 0,
                            processingTime: Date.now() - startTime,
                            h: indexValue.h,
                            f: indexValue.f
                        };
                    } catch (error) {
                        this.logger.error(`✗ Ошибка при обновлении индексного значения: ${error.message}`);
                        return {
                            writeError: error.message,
                            upsertedCount: 0,
                            modifiedCount: 0,
                            processingTime: Date.now() - startTime,
                            h: indexValue.h,
                            f: indexValue.f
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
                saveIndexMetrics = updateResults.map(result => { return { h: result.h, f: result.f, processingTime: result.processingTime } });
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
            if (saveIndexMetrics && saveIndexMetrics.length > 0) {
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
     * Поиск факта по заданному условию
     * @param {number} messageTypeId - тип сообщения
     * @param {number} indexType - тип индекса
     * @param {string} indexValue - значение индекса
     * @param {Object} conditions - условия поиска
     * @returns {Promise<Object>} факт
     */
    async findFact(factId, messageTypeId, indexType, factIndexValues, conditions = {}, lastDays = 10) {
        // Находим индексное значение по типу индекса
        const indexValue = factIndexValues.find(value => value.it === indexType);
        if (!indexValue) {
            this.logger.warn(`findFact: Для факта ${factId} индексное значение для типа индекса ${indexType} не найдено.`);
            return null;
        }
        if (!conditions || Object.keys(conditions).length === 0) {
            this.logger.warn(`findFact: Для факта ${factId} нет условий поиска.`);
            return null;
        }
        const findCondition = {
            "h": indexValue.h,
            "dt": {
                "$gte": new Date(Date.now() - lastDays * 24 * 60 * 60 * 1000),
            },
            "t": messageTypeId,
            "it": indexType,
            ...conditions
        };
        const factIndexCollection = this._getFactIndexCollection();
        const startFindTime = Date.now();
        const fact = await factIndexCollection.findOne(findCondition).sort({ dt: -1 });
        return {
            fact: fact,
            findTime: Date.now() - startFindTime
        };
    }

    /**
     * Вычисление facet условия для одного счетчика
     */
    _getCounterFacetStage(counter, timeFieldName = "dt", nowDate) {
        //
        // Условие фильтрации счетчика
        //
        const matchStageCondition = counter.evaluationConditions ? { "$match": counter.evaluationConditions } : { "$match": {} };
        // Нужно добавить условие по fromTimeMs и toTimeMs в matchStage
        if (counter.fromTimeMs > 0) {
            const fromDateTime = nowDate - counter.fromTimeMs;
            if (!matchStageCondition["$match"]) {
                matchStageCondition["$match"] = {};
            }
            if (!matchStageCondition["$match"][timeFieldName]) {
                matchStageCondition["$match"][timeFieldName] = {};
            }
            matchStageCondition["$match"][timeFieldName]["$gte"] = new Date(fromDateTime);
        }
        if (counter.toTimeMs > 0) {
            const toDateTime = nowDate - counter.toTimeMs;
            if (!matchStageCondition["$match"]) {
                matchStageCondition["$match"] = {};
            }
            if (!matchStageCondition["$match"][timeFieldName]) {
                matchStageCondition["$match"][timeFieldName] = {};
            }
            matchStageCondition["$match"][timeFieldName]["$lt"] = new Date(toDateTime);
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
        // Формирование facet pipeline
        //
        const facet = [];
        if (matchStage) {
            facet.push(matchStage);
        }
        if (limitStage) {
            facet.push(limitStage);
        }
        facet.push(groupStage);
        if (config.facts.splitIntervals) {
            // Добавление подсчета уникальных значений для счетчика, но только если нет сплитования интервалов, иначе не сможем потом выполнить подсчет
            const addFieldsStage = {};
            Object.keys(counter.attributes).forEach(counterName => {
                const counterEvaluation = counter.attributes[counterName];
                if (counterEvaluation && counterEvaluation["$addToSet"]) {
                    // Есть выражение для получения уникальных значений
                    addFieldsStage[counterName] = { "$size": "$" + counterName };
                }
            });
            if (Object.keys(addFieldsStage).length) {
                facet.push({ "$addFields": addFieldsStage });
            }
        }

        return facet;
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
        // которые отсортированы по возрастанию времени в прошлое от текущего времени
        const countersInfo = this._counterProducer.getFactCounters(fact, config.facts.allowedCountersNames);
        if (!countersInfo) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }
        const factCounters = countersInfo.computationConditionsCounters;
        if (!factCounters) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }
        // this.logger.debug(`factCounters: ${JSON.stringify(factCounters)}`);
        const maxDepthLimit = config.facts.maxDepthLimit ?? 2000;
        // Интервалы разбивки счетчиков на группы по возрастанию времени в прошлое от текущего времени
        const splitIntervals = config.facts.splitIntervals;
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
            // Вычисление номера группы счетчиков в запросе и названия индекса с номером группы
            //
            if (!countersGroupCount[counter.indexTypeName]) {
                countersGroupCount[counter.indexTypeName] = {
                    countersCount: 0, // Общее количество счетчиков в текущей группе
                    groupNumber: 1, // Номер группы счетчиков в запросе
                    intervalNumber: 0, // Номер интервала в запросе
                    fromTimeMs: splitIntervals ? splitIntervals[0] : 0, // Время начала интервала в прошлом
                    toTimeMs: 0, // Время конца интервала в прошлом
                };
            }
            countersGroupCount[counter.indexTypeName].countersCount++;
            let groupNumberAlreadyIncremented = false;
            if (config.facts.maxCountersPerRequest > 0 && countersGroupCount[counter.indexTypeName].countersCount > config.facts.maxCountersPerRequest) {
                countersGroupCount[counter.indexTypeName].groupNumber++;
                groupNumberAlreadyIncremented = true;
                countersGroupCount[counter.indexTypeName].countersCount = 1;
            }
            // Если счетчик пересекается с предыдущим интервалом, то увеличиваем номер группы и счетчик счетчиков в группе
            if (splitIntervals &&
                countersGroupCount[counter.indexTypeName].fromTimeMs < counter.fromTimeMs
            ) {
                if (!groupNumberAlreadyIncremented) {
                    countersGroupCount[counter.indexTypeName].groupNumber++;
                }
                countersGroupCount[counter.indexTypeName].countersCount = 1;
                countersGroupCount[counter.indexTypeName].intervalNumber++;
                countersGroupCount[counter.indexTypeName].toTimeMs = countersGroupCount[counter.indexTypeName].fromTimeMs;
                countersGroupCount[counter.indexTypeName].fromTimeMs = splitIntervals[countersGroupCount[counter.indexTypeName].intervalNumber];
            }
            //
            // Добавление счетчика в список счетчиков для текущего индекса
            //
            const indexTypeNameWithGroupNumber = counter.indexTypeName + "#" + countersGroupCount[counter.indexTypeName].groupNumber;
            if (!indexFacetStages[indexTypeNameWithGroupNumber]) {
                indexFacetStages[indexTypeNameWithGroupNumber] = {};
            }
            indexFacetStages[indexTypeNameWithGroupNumber][counter.name] = this._getCounterFacetStage(counter, timeFieldName, nowDate);
            // Добавление максимального количества записей для группы счетчиков
            if (!indexLimits[indexTypeNameWithGroupNumber]) {
                indexLimits[indexTypeNameWithGroupNumber] = {
                    fromTimeMs: counter.fromTimeMs,
                    toTimeMs: counter.toTimeMs,
                };
            }
            indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords = Math.max(indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords ?? 0, counter.maxEvaluatedRecords ?? 0);
            // Если максимальное количество записей для группы счетчиков превышает максимальную глубину запроса, то устанавливаем максимальную глубину запроса
            if (maxDepthLimit > 0 && indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords > maxDepthLimit) {
                indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords = maxDepthLimit;
            }
            indexLimits[indexTypeNameWithGroupNumber].fromTimeMs = Math.max(indexLimits[indexTypeNameWithGroupNumber].fromTimeMs ?? 0, counter.fromTimeMs ?? 0);
            indexLimits[indexTypeNameWithGroupNumber].toTimeMs = Math.min(indexLimits[indexTypeNameWithGroupNumber].toTimeMs ?? 0, counter.toTimeMs ?? 0);
        });
        if (config.facts.maxCountersProcessing > 0 && totalCountersCount > config.facts.maxCountersProcessing) {
            this.logger.warn(`Превышено максимальное количество счетчиков для обработки: ${config.facts.maxCountersProcessing}. Всего счетчиков: ${totalCountersCount}.`);
        }

        // Если в выражении счетчиков есть параметры, то заменить их на значения атрибутов из факта
        // Например, если в выражении счетчиков есть параметр "$$f2", то он будет заменен на значение атрибута "f2" из факта
        const indexFacetStagesString = JSON.stringify(indexFacetStages);
        if (!indexFacetStagesString.includes('$$')) {
            return { indexFacetStages: indexFacetStages, indexLimits: indexLimits };
        }
        // Создаем глубокую копию выражения счетчиков
        const parameterizedFacetStages = JSON.parse(indexFacetStagesString);
        // Рекурсивно заменяем переменные в объекте
        this._replaceParametersRecursive(parameterizedFacetStages, fact.d);

        return {
            indexFacetStages: parameterizedFacetStages,
            indexLimits: indexLimits,
            // Количество счетчиков, которые будут меняться фактом по условиям оценки (для метрик)
            evaluationCountersCount: countersInfo.evaluationCountersCount
        };
    }

    // Старый метод, не используется
    getFactIndexCountersInfo_Old(fact, timeFieldName = "dt") {
        if (!this._counterProducer) {
            this.logger.warn('mongoProvider.mongoCounters не заданы. Счетчики будут создаваться по умолчанию.');
            return this.getDefaultFactIndexCountersInfo();
        }
        // Получение счетчиков, которые подходят для факта по условию computationConditions
        const countersInfo = this._counterProducer.getFactCounters(fact, config.facts.allowedCountersNames);
        if (!countersInfo) {
            this.logger.warn(`Для факта ${fact?._id} нет подходящих счетчиков.`);
            return null;
        }
        const factCounters = countersInfo.computationConditionsCounters;
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
            if (config.facts.maxCountersPerRequest > 0 && countersGroupCount[counter.indexTypeName].countersCount > config.facts.maxCountersPerRequest) {
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
            if (!indexLimits[indexTypeNameWithGroupNumber]) {
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
            return { indexFacetStages: indexFacetStages, indexLimits: indexLimits };
        }
        // Создаем глубокую копию выражения счетчиков
        const parameterizedFacetStages = JSON.parse(indexFacetStagesString);
        // Рекурсивно заменяем переменные в объекте
        this._replaceParametersRecursive(parameterizedFacetStages, fact.d);

        return {
            indexFacetStages: parameterizedFacetStages,
            indexLimits: indexLimits,
            // Количество счетчиков, которые будут меняться фактом по условиям оценки (для метрик)
            evaluationCountersCount: countersInfo.evaluationCountersCount
        };
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
        const factIndexResult = await factIndexCollection.find(indexNameQuery.factIndexFindQuery, {}, indexNameQuery.factIndexFindOptions).sort(indexNameQuery.factIndexFindSort).limit(indexNameQuery.depthLimit).toArray();
        const stopFindFactIndexTime = Date.now();
        const relevantFactsQuerySize = debugMode ? JSON.stringify(indexNameQuery.factIndexFindQuery).length : undefined;
        const relevantFactsSize = debugMode ? JSON.stringify(factIndexResult).length : undefined;
        const factIds = factIndexResult.map(item => item.f);
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
            if (indexTypeNames.has(indexTypeName)) {
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
                "h": indexInfo.hashValue
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
                factIndexFindQuery["dt"]["$lt"] = new Date(nowDate - indexLimits[indexTypeNameWithGroupNumber].toTimeMs);
            }
            if (fact) {
                if (config.facts.emptyRequests) {
                    // Добавляем для тестов, чтобы выполнять пустые запросы
                    factIndexFindQuery["f"] = "empty";
                } else {
                    /*
                    factIndexFindQuery["f"] = {
                        "$ne": fact._id
                    };
                    */
                }
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
                    projection: { "f": 1 }
                },
                factIndexFindSort: {
                    "h": 1,
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
                    evaluationCountersCount: info.evaluationCountersCount,
                    maxCountersPerRequest: config.facts.maxCountersPerRequest,
                    maxCountersProcessing: config.facts.maxCountersProcessing,
                    counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                    relevantIndexCount: 0,
                    queryCountersCount: 0,
                    relevantFactsQueryTime: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryCount: 0,
                    relevantFactsSize: 0,
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
        let relevantFactsQueryTime = 0;
        let relevantFactsQueryCount = 0;
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
            relevantFactsQueryTime = Math.max(relevantFactsQueryTime, result.factsMetrics.relevantFactsTime ?? 0);
            relevantFactsQueryCount += result.factsMetrics.relevantFactsCount ?? 0;
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
         * evaluationCountersCount - число счетчиков, на которые влияет факт (для метрики потенциально изменяемых счетчиков)
         * maxCountersPerRequest - максимальное количество счетчиков на запрос (из файла конфигурации)
         * maxCountersProcessing - максимальное количество счетчиков для обработки (из файла конфигурации)
         * counterIndexCountWithGroup - количество индексов с счетчиками, применимых к факту с учетом разбивки на группы (после фильтрации по условию computationConditions)
         * relevantIndexCount - количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
         * queryCountersCount - количество запрошенных счетчиков в запросах на вычисление счетчиков
         * prepareCountersQueryTime - время подготовки параллельных запросов на вычисление счетчиков
         * processingQueriesTime - время выполнения параллельных запросов на вычисление счетчиков
         * relevantFactsQuerySize - размер запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsQueryTime - время выполнения запроса по индексам для поиска ИД релевантных фактов
         * relevantFactsQueryCount - количество релевантных фактов (факты, которые попали после поиска по индексам)
         * relevantFactsQueryCount - количество релевантных фактов (факты, которые попали после поиска по индексам)
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
                evaluationCountersCount: info.evaluationCountersCount,
                maxCountersPerRequest: config.facts.maxCountersPerRequest,
                maxCountersProcessing: config.facts.maxCountersProcessing,
                counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                relevantIndexCount: queriesByIndexName ? Object.keys(queriesByIndexName).length : 0,
                queryCountersCount: queryCountersCount,
                prepareQueriesTime: startQueriesTime - startPrepareQueriesTime,
                processingQueriesTime: stopQueriesTime - startQueriesTime,
                relevantFactsQuerySize: relevantFactsQuerySize,
                relevantFactsQueryTime: relevantFactsQueryTime,
                relevantFactsQueryCount: relevantFactsQueryCount,
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
                "h": indexInfo.hashValue
            };
            if (depthFromDate || indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0) {
                const fromDateTime = indexLimits[indexTypeNameWithGroupNumber].fromTimeMs > 0 ? nowDate - indexLimits[indexTypeNameWithGroupNumber].fromTimeMs : (depthFromDate ? depthFromDate.getTime() : nowDate);
                if (!match["dt"]) {
                    match["dt"] = {};
                }
                match["dt"]["$gte"] = new Date(fromDateTime);
            }
            if (indexLimits[indexTypeNameWithGroupNumber].toTimeMs > 0) {
                if (!match["dt"]) {
                    match["dt"] = {};
                }
                match["dt"]["$lt"] = new Date(nowDate - indexLimits[indexTypeNameWithGroupNumber].toTimeMs);
            }
            if (fact) {
                if (config.facts.emptyRequests) {
                    // Добавляем для тестов, чтобы выполнять пустые запросы
                    match["f"] = "empty";
                } else {
                    /*
                    match["f"] = {
                        "$ne": fact._id
                    };
                    */
                }
            }
            const sort = {
                "h": 1,
                "dt": -1
            };
            const limit = (!indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords || indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords > depthLimit) ? depthLimit : indexLimits[indexTypeNameWithGroupNumber].maxEvaluatedRecords;
            const aggregateIndexQuery = [
                { "$match": match },
                { "$sort": sort }
            ];
            if (config.facts.skipFactLimit > 0) {
                aggregateIndexQuery.push({ "$skip": config.facts.skipFactLimit });
            }
            aggregateIndexQuery.push({ "$limit": limit });

            if (lookupFacts) {
                aggregateIndexQuery.push({
                    "$lookup": {
                        from: "facts",
                        localField: "f",
                        foreignField: "_id",
                        let: { "factId": "$f" },
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
                    evaluationCountersCount: info.evaluationCountersCount,
                    maxCountersPerRequest: config.facts.maxCountersPerRequest,
                    maxCountersProcessing: config.facts.maxCountersProcessing,
                    counterIndexCountWithGroup: indexCountersInfo ? Object.keys(indexCountersInfo).length : 0,
                    relevantIndexCount: 0,
                    queryCountersCount: 0,
                    prepareCountersQueryTime: 0,
                    relevantFactsQuerySize: 0,
                    relevantFactsQueryTime: 0,
                    relevantFactsQueryCount: 0,
                    relevantFactsSize: 0,
                    countersQuerySize: 0,
                    countersQueryWaitTime: 0,
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

        const queryDispatcher = this._getQueryDispatcher();
        const aggregateOptions = {
            batchSize: config.database.batchSize,
            readConcern: this._databaseOptions.readConcern,
            readPreference: this._databaseOptions.aggregateReadPreference,
            comment: "getRelevantFactCounters - aggregate",
        };

        // Создаем массив промисов для параллельного выполнения запросов
        const startPrepareQueriesTime = Date.now();
        const startQueriesTime = Date.now();
        let queryResults;
        let resultsTransformationTime = 0;
        let dispatcherSummary = {};

        if (queryDispatcher) {
            // Создаем маппинг уникальных ID запросов к indexTypeNameWithGroupNumber
            // Это необходимо для избежания конфликтов при параллельных запросах с одинаковыми индексами
            const queryIdToIndexMap = new Map();

            // Подготовка массива запросов для выполнения через QueryDispatcher
            let queryCounter = 0;
            const preparedQueries = Object.keys(queriesByIndexName).map(indexTypeNameWithGroupNumber => {
                const indexNameQuery = queriesByIndexName[indexTypeNameWithGroupNumber].query;
                // Генерируем уникальный ID для каждого запроса, чтобы избежать конфликтов при параллельных запросах
                // Используем высокоточное время и счетчик для гарантированной уникальности
                const uniqueId = process.hrtime.bigint ? process.hrtime.bigint().toString() : `${Date.now()}_${++queryCounter}`;
                const uniqueQueryId = `${indexTypeNameWithGroupNumber}_${uniqueId}_${Math.random().toString(36).slice(2, 9)}`;
                // Сохраняем маппинг для последующего восстановления indexTypeNameWithGroupNumber
                queryIdToIndexMap.set(uniqueQueryId, indexTypeNameWithGroupNumber);

                return {
                    id: uniqueQueryId,
                    query: indexNameQuery,
                    collectionName: this.FACT_INDEX_COLLECTION_NAME,
                    options: aggregateOptions,
                };
            });

            // Выполняем запросы через QueryDispatcher
            const queryDispatcherResults = await queryDispatcher.executeQueries(preparedQueries);

            // Измеряем время преобразования результатов
            const resultsTransformationStartTime = Date.now();

            // Преобразуем результаты QueryDispatcher в формат, совместимый с локальным кодом
            queryResults = queryDispatcherResults.results.map((dispatcherResult) => {
                // Восстанавливаем indexTypeNameWithGroupNumber из маппинга
                let indexTypeNameWithGroupNumber = queryIdToIndexMap.get(dispatcherResult.id);
                if (!indexTypeNameWithGroupNumber) {
                    // Если маппинг не найден, пытаемся извлечь из ID (для обратной совместимости)
                    // Формат ID: ${indexTypeNameWithGroupNumber}_${timestamp}_${random}
                    const extractedIndex = dispatcherResult.id.split('_')[0];
                    this.logger.warn(`Не найден маппинг для запроса ${dispatcherResult.id}, используем извлеченное значение: ${extractedIndex}`);
                    indexTypeNameWithGroupNumber = extractedIndex || dispatcherResult.id;
                }

                const indexNameQuery = queriesByIndexName[indexTypeNameWithGroupNumber];
                if (!indexNameQuery || !indexNameQuery.query) {
                    this.logger.error(`Не удалось найти запрос для indexTypeNameWithGroupNumber: ${indexTypeNameWithGroupNumber} (ID запроса: ${dispatcherResult.id})`);
                    return {
                        indexTypeName: indexTypeNameWithGroupNumber,
                        counters: null,
                        error: `Не удалось найти запрос для indexTypeNameWithGroupNumber: ${indexTypeNameWithGroupNumber}`,
                        processingTime: 0,
                        metrics: {
                            countersQuerySize: 0,
                            countersQueryTime: 0,
                            countersQueryCount: 1,
                            countersSize: 0,
                        },
                        debug: {
                            countersQuery: null,
                        }
                    };
                }
                const query = indexNameQuery.query;
                const startQuery = startQueriesTime; // Используем общее время начала запросов
                const countersQuerySize = debugMode ? JSON.stringify(query).length : undefined;

                // Определяем результат: если result - массив, берем первый элемент, иначе null
                // В локальном коде используется countersResult[0], где countersResult - результат .toArray()
                let counters = null;
                if (dispatcherResult.result && Array.isArray(dispatcherResult.result) && dispatcherResult.result.length > 0) {
                    counters = dispatcherResult.result[0];
                } else if (dispatcherResult.result && !Array.isArray(dispatcherResult.result)) {
                    counters = dispatcherResult.result;
                }

                const countersQueryTime = dispatcherResult.metrics?.queryTime || 0;
                const countersWaitTime = dispatcherResult.metrics?.waitTime || 0;

                // Определяем сообщение об ошибке для возврата (совместимо с локальным кодом)
                let errorMessage = undefined;
                if (dispatcherResult.error) {
                    errorMessage = dispatcherResult.error instanceof Error
                        ? dispatcherResult.error.message
                        : (dispatcherResult.error.message || dispatcherResult.error);
                }

                // Вычисляем countersSize так же, как в локальном коде: 
                // - при ошибке: 0 (как в локальном коде в блоке catch)
                // - при успехе: длина JSON если debugMode включен, иначе undefined (как в локальном коде в блоке try)
                let countersSize;
                if (dispatcherResult.error) {
                    countersSize = 0; // При ошибке всегда 0, как в локальном коде
                } else {
                    countersSize = debugMode ? JSON.stringify(counters ?? null).length : undefined;
                }

                // Логируем ошибки, если они есть
                if (dispatcherResult.error) {
                    this.logger.error(`Ошибка при выполнении запроса для индекса ${indexTypeNameWithGroupNumber}: ${errorMessage}`);
                    this._writeToLogFile(`Ошибка при выполнении запроса для индекса ${indexTypeNameWithGroupNumber}: ${errorMessage}`);
                    this._writeToLogFile(JSON.stringify(query, null, 2));
                }

                return {
                    indexTypeName: indexTypeNameWithGroupNumber,
                    counters: counters,
                    error: errorMessage,
                    processingTime: countersQueryTime,
                    metrics: {
                        countersQuerySize: countersQuerySize ?? (dispatcherResult.metrics?.querySize || 0),
                        countersQueryTime: countersQueryTime,
                        countersWaitTime: countersWaitTime,
                        countersQueryCount: 1,
                        countersSize: countersSize,
                    },
                    debug: {
                        countersQuery: query,
                    }
                };
            });

            resultsTransformationTime = Date.now() - resultsTransformationStartTime;
            // Сохраняем метрики из summary QueryDispatcher для последующего использования
            dispatcherSummary = queryDispatcherResults.summary || {};

            // this.logger.debug(`✓ *** Получены счетчики: ${JSON.stringify(queryResults)} `);
        } else {

            const queryPromises = Object.keys(queriesByIndexName).map(async (indexTypeNameWithGroupNumber) => {
                // const indexTypeName = indexTypeNameWithGroupNumber.split('#')[0];
                // const groupNumber = parseInt(indexTypeNameWithGroupNumber.split('#')[1] ?? 0);
                const indexNameQuery = queriesByIndexName[indexTypeNameWithGroupNumber].query;
                const startQuery = Date.now();

                // this.logger.info(`Агрегационный запрос на счетчики по фактам: ${JSON.stringify(indexNameQuery)}`);
                const startCountersQueryTime = Date.now();
                const countersQuerySize = debugMode ? JSON.stringify(indexNameQuery).length : undefined;

                // Fallback на локальное выполнение, если QueryDispatcher недоступен
                const factIndexCollection = this._getFactIndexAggregateCollection();
                try {
                    if (this._debugMode) {
                        this.logger.debug(`Агрегационный запрос для индекса ${indexTypeNameWithGroupNumber}: ${JSON.stringify(indexNameQuery)}`);
                    }
                    const countersResult = await factIndexCollection.aggregate(indexNameQuery, aggregateOptions).toArray();
                    const countersSize = debugMode ? JSON.stringify(countersResult[0] ?? null).length : undefined;
                    return {
                        indexTypeName: indexTypeNameWithGroupNumber,
                        counters: countersResult[0],
                        error: undefined,
                        processingTime: Date.now() - startQuery,
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
            queryResults = await Promise.all(queryPromises);
        }
        const stopQueriesTime = Date.now();

        // Объединяем результаты в один JSON объект
        const mergedCounters = {};
        const countersMetrics = {};
        let countersQuerySize = 0;
        let countersQueryTime = 0;
        let countersQueryWaitTime = 0;
        let countersCount = 0;
        let countersSize = 0;
        const countersQuery = {};
        queryResults.forEach((result) => {
            if (result.counters) {
                Object.assign(mergedCounters, result.counters);
            }
            countersMetrics[result.indexTypeName] = {
                processingTime: result.processingTime,
                metrics: result.metrics,
                error: result.error || null // Добавляем информацию об ошибке для мониторинга таймаутов
            };
            countersQuerySize += result.metrics.countersQuerySize ?? 0;
            countersQueryTime = Math.max(countersQueryTime, result.metrics.countersQueryTime ?? 0);
            countersQueryWaitTime = Math.max(countersQueryWaitTime, result.metrics.countersWaitTime ?? 0);
            countersCount += result.metrics.countersQueryCount ?? 0;
            countersSize += result.metrics.countersSize ?? 0;
            countersQuery[result.indexTypeName] = result.debug.countersQuery;
        });

        if (this._debugMode) {
            this.logger.debug(`✓ Получены счетчики: ${JSON.stringify(mergedCounters)} `);
        }

        const queryCountersCount = Object.keys(countersQuery).map(key => countersQuery[key] ? Object.keys((countersQuery[key].find(i => i["$facet"]) ?? { "$facet": {} })["$facet"]).length : 0).reduce((a, b) => a + b, 0);

        /**
         * Структура отладочной информации debug:
         * totalIndexCount - общее количество индексируемых полей факта (связка по полям факта и индекса)
         * factCountersCount - общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions)
         * evaluationCountersCount - число счетчиков, на которые влияет факт (для метрики потенциально изменяемых счетчиков)
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
         * countersQueryWaitTime - время ожидания запросов в QueryDispatcher
         * countersPoolInitTime - время инициализации пула процессов в QueryDispatcher
         * countersQueryTime - время выполнения параллельных запросов на вычисление счетчиков
         * countersBatchPreparationTime - время подготовки батчей в QueryDispatcher
         * countersBatchExecutionTime - время выполнения батчей (IPC коммуникация и ожидание результатов от worker процессов)
         * countersResultsProcessingTime - время обработки результатов в QueryDispatcher
         * countersResultsTransformationTime - время преобразования результатов QueryDispatcher в формат mongoProvider
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
                evaluationCountersCount: info.evaluationCountersCount,
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
                countersQueryWaitTime: countersQueryWaitTime,
                countersPoolInitTime: dispatcherSummary.poolInitTime || 0,
                countersQueryTime: countersQueryTime,
                countersBatchPreparationTime: dispatcherSummary.batchPreparationTime || 0,
                countersBatchExecutionTime: dispatcherSummary.batchExecutionTime || 0,
                countersResultsProcessingTime: dispatcherSummary.resultsProcessingTime || 0,
                countersResultsTransformationTime: resultsTransformationTime,
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
            "h": {
                "$in": indexHashValues
            }
        };
        if (fact) {
            matchQuery["f"] = {
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
        const relevantFactIds = await factIndexCollection.find(matchQuery, {}, findOptions).sort({ dt: -1 }).batchSize(config.database.batchSize).limit(depthLimit).toArray();
        // Сформировать агрегирующий запрос к коллекции facts,
        const aggregateQuery = [
            {
                "$match": {
                    "_id": {
                        "$in": relevantFactIds.map(item => item.f)
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
            const facts = await factsCollection.find(filter, {}, findOptions).toArray();
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
                readPreference: { "mode": "primary" },
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
                readPreference: { "mode": "primary" },
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
                    required: ["_id", "h", "f", "dt", "c"],
                    properties: {
                        _id: {
                            bsonType: "objectId",
                            description: "Уникальный идентификатор индексного значения"
                        },
                        h: {
                            bsonType: "string",
                            description: "Хеш значение <тип индексного значения>:<значение поля факта>"
                        },
                        f: {
                            bsonType: "string",
                            description: "Уникальный идентификатор факта в коллекции facts._id"
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
                    key: { "h": 1, "f": 1 },
                    options: {
                        name: 'idx_h_f',
                        background: true,
                        unique: true
                    }
                },
                {
                    key: { "h": 1, "dt": 1 },
                    options: {
                        name: 'idx_h_dt',
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
            await this._shardCollection(adminDb, this.FACT_INDEX_COLLECTION_NAME, { "h": 1, "f": 1 }, true);
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
        if (!config.isDevelopment) {
            this.logger.debug("*** Production mode on.");
            return {
                success: true
            };
        }
        this.logger.debug("*** Development mode on.");
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