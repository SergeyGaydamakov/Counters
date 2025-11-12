/**
 * QueryWorker - Worker процесс для выполнения MongoDB запросов
 * 
 * Этот модуль запускается как отдельный процесс через child_process.fork()
 * и обрабатывает агрегационные запросы к MongoDB через IPC канал.
 * 
 * IPC Сообщения:
 * - От основного процесса: {type: 'QUERY', id: string, query: Array, collectionName: string, options: object}
 * - К основному процессу: {type: 'RESULT', id: string, result: Array, error: Error | null, metrics: object}
 */

const { MongoClient } = require('mongodb');
const Logger = require('../common/logger');
const config = require('../common/config');
const ipcSerializer = require('../common/ipcSerializer');

const DEBUG_METRICS_ENABLED = config.logging?.debugMode === true;

/**
 * Проверяет, является ли строка ISO 8601 датой
 * @param {string} str - Строка для проверки
 * @returns {boolean} true, если строка является ISO 8601 датой
 */
function isISODateString(str) {
    if (typeof str !== 'string') {
        return false;
    }
    // ISO 8601 формат: YYYY-MM-DDTHH:mm:ss.sssZ или YYYY-MM-DDTHH:mm:ssZ
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/.test(str);
}

function reviveIsoDates(value) {
    if (value === null || value === undefined) {
        return value;
    }

    if (typeof value === 'string') {
        return isISODateString(value) ? new Date(value) : value;
    }

    if (Array.isArray(value)) {
        return value.map(item => reviveIsoDates(item));
    }

    if (typeof value === 'object') {
        const result = {};
        for (const key in value) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                result[key] = reviveIsoDates(value[key]);
            }
        }
        return result;
    }

    return value;
}

/**
 * Создает MongoDB клиент для агрегационных запросов
 * Аналогично _getMongoClientAggregate в mongoProvider.js
 * Клиент создается один раз при инициализации и переиспользуется для всех запросов
 * @param {string} connectionString - Строка подключения
 * @param {object} databaseOptions - Опции подключения
 * @returns {MongoClient} MongoDB клиент
 */
function _getMongoClient(connectionString, databaseOptions) {
    // Опции для подключения по умолчанию
    const options = {
        readConcern: databaseOptions.readConcern,
        readPreference: databaseOptions.aggregateReadPreference,
        writeConcern: databaseOptions.writeConcern,
        appName: "QueryWorker",
        minPoolSize: databaseOptions.minPoolSize || 10,
        maxPoolSize: databaseOptions.maxPoolSize || 100,
        maxIdleTimeMS: databaseOptions.maxIdleTimeMS || 0,
        noDelay: databaseOptions.noDelay,
        maxConnecting: databaseOptions.maxConnecting || 10,
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
 * Сериализация объекта Error для передачи через IPC
 * @param {Error} error - Объект ошибки
 * @returns {object} Сериализованная ошибка
 */
function serializeError(error) {
    if (!error) return null;
    
    return {
        name: error.name || 'Error',
        message: error.message || '',
        stack: error.stack || ''
    };
}

/**
 * Основная функция QueryWorker
 * Выполняется когда процесс запущен через fork()
 */
async function startQueryWorker() {
    const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
    
    let mongoClient = null;
    let mongoDb = null;
    let isConnected = false;
    let isShuttingDown = false;
    
    // Параметры подключения будут получены через INIT сообщение
    let connectionString, databaseName, databaseOptions;
    
    // Активные батчи для параллельной обработки
    // Map<batchId, Promise> - отслеживание активных батчей
    const activeBatches = new Map();
    
    /**
     * Подключение к MongoDB
     * Создает MongoDB клиент один раз при инициализации и подключается к базе данных
     * Клиент переиспользуется для всех последующих запросов
     */
    async function connect() {
        try {
            logger.debug(`QueryWorker: Подключение к MongoDB: ${connectionString}`);
            logger.debug(`QueryWorker: Настройки: \n${JSON.stringify(databaseOptions, null, 2)}`);
            
            // Создаем MongoDB клиент один раз - он будет переиспользоваться для всех запросов
            mongoClient = _getMongoClient(connectionString, databaseOptions);
            await mongoClient.connect();
            mongoDb = mongoClient.db(databaseName);
            isConnected = true;
            
            logger.debug(`QueryWorker: ✓ Подключено к базе данных: ${databaseName}`);
            
            // Отправляем подтверждение подключения
            ipcSerializer.send(process, { type: 'READY' });
        } catch (error) {
            logger.error(`QueryWorker: ✗ Ошибка подключения к MongoDB: ${error.message}`);
            isConnected = false;
            
            // Отправляем ошибку подключения
            ipcSerializer.send(process, { 
                type: 'ERROR', 
                error: serializeError(error),
                message: 'Failed to connect to MongoDB'
            });
            
            throw error;
        }
    }
    
    /**
     * Выполнение агрегационного запроса
     * Использует переиспользуемый MongoDB клиент, созданный при инициализации
     * @param {string} queryId - ID запроса
     * @param {Array} query - Агрегационный пайплайн
     * @param {string} collectionName - Имя коллекции
     * @param {object} options - Опции aggregate()
     * @returns {Promise<Object>} Результат выполнения запроса {result, error, metrics}
     */
    async function executeQuery(queryId, query, collectionName, options = {}) {
        if (!isConnected || !mongoDb || !mongoClient) {
            const error = new Error('MongoDB connection not established');
            return {
                result: null,
                error: serializeError(error),
                metrics: {
                    queryTime: 0,
                    querySize: 0,
                    resultSize: 0
                }
            };
        }
        
        const startTime = Date.now();
        const querySize = DEBUG_METRICS_ENABLED ? JSON.stringify(query).length : 0;
        const normalizedQuery = reviveIsoDates(query);
        
        try {
            logger.debug(`QueryWorker: Выполнение запроса ${queryId} для коллекции ${collectionName}`);
            
            const aggregateOptions = Object.assign({
                batchSize: config.database.batchSize,
                readConcern: this._databaseOptions?.readConcern,
                readPreference: this._databaseOptions?.readPreference,
                comment: "queryWorker - aggregate"
            }, options || {});
            
            const collection = mongoDb.collection(collectionName);
            const result = await collection.aggregate(normalizedQuery, aggregateOptions).toArray();
            
            const queryTime = Date.now() - startTime;
            const resultSize = DEBUG_METRICS_ENABLED ? JSON.stringify(result).length : 0;
            
            logger.debug(`QueryWorker: Запрос ${queryId} выполнен за ${queryTime}ms, размер результата: ${resultSize} байт`);
            
            return {
                result: result,
                error: null,
                metrics: {
                    queryTime: queryTime,
                    querySize: querySize,
                    resultSize: resultSize
                }
            };
        } catch (err) {
            const queryTime = Date.now() - startTime;
            
            logger.error(`QueryWorker: Ошибка выполнения запроса ${queryId}: ${err.message}`);
            
            return {
                result: null,
                error: serializeError(err),
                metrics: {
                    queryTime: queryTime,
                    querySize: querySize,
                    resultSize: 0
                }
            };
        }
    }
    
    /**
     * Отправка результата запроса родительскому процессу
     * @param {string} queryId - ID запроса
     * @param {Object} queryResult - Результат выполнения запроса {result, error, metrics}
     */
    function sendQueryResult(queryId, queryResult) {
        ipcSerializer.send(process, {
            type: 'RESULT',
            id: queryId,
            result: queryResult.result,
            error: queryResult.error,
            metrics: queryResult.metrics
        });
    }
    
    function sendBatchResult(batchId, batchResults) {
        ipcSerializer.send(process, {
            type: 'RESULT_BATCH',
            batchId,
            results: batchResults
        });
    }
    
    async function executeQueryBatch(batchId, requests) {
        if (!Array.isArray(requests) || requests.length === 0) {
            return [];
        }
    
        const executions = requests.map(async (request, index) => {
            const requestId = request?.id || `${batchId}_${index}`;
    
            if (!request || !Array.isArray(request.query) || typeof request.collectionName !== 'string') {
                return {
                    id: requestId,
                    result: null,
                    error: serializeError(new Error('Invalid batch query structure')),
                    metrics: {
                        queryTime: 0,
                        querySize: 0,
                        resultSize: 0
                    }
                };
            }
    
            try {
                const queryResult = await executeQuery(
                    requestId,
                    request.query,
                    request.collectionName,
                    request.options || {}
                );
    
                return {
                    id: requestId,
                    result: queryResult.result,
                    error: queryResult.error,
                    metrics: queryResult.metrics
                };
            } catch (error) {
                return {
                    id: requestId,
                    result: null,
                    error: serializeError(error),
                    metrics: {
                        queryTime: 0,
                        querySize: 0,
                        resultSize: 0
                    }
                };
            }
        });
    
        return Promise.all(executions);
    }
    
    /**
     * Graceful shutdown
     */
    async function shutdown() {
        if (isShuttingDown) return;
        isShuttingDown = true;
        
        logger.debug('QueryWorker: Начало graceful shutdown');
        
        // Ждем завершения всех активных батчей
        if (activeBatches.size > 0) {
            logger.debug(`QueryWorker: Ожидание завершения ${activeBatches.size} активных батчей`);
            try {
                await Promise.all(Array.from(activeBatches.values()));
            } catch (error) {
                logger.warn(`QueryWorker: Ошибка при ожидании завершения батчей: ${error.message}`);
            }
            activeBatches.clear();
        }
        
        try {
            if (mongoClient && isConnected) {
                await mongoClient.close();
                logger.debug('QueryWorker: MongoDB подключение закрыто');
            }
        } catch (error) {
            logger.error(`QueryWorker: Ошибка при закрытии подключения: ${error.message}`);
        }
        
        logger.debug('QueryWorker: Завершение работы');
        process.exit(0);
    }
    
    /**
     * Обработка ошибок процесса
     */
    process.on('error', (error) => {
        logger.error(`QueryWorker: Критическая ошибка процесса: ${error.message}`);
    });
    
    /**
     * Обработка сигналов завершения
     */
    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
    
    /**
     * Обработка отключения от родительского процесса
     */
    process.on('disconnect', async () => {
        logger.debug('QueryWorker: Отключен от родительского процесса');
        await shutdown();
    });
    
    /**
     * Обработка INIT сообщения от родительского процесса
     * Должен быть установлен ПЕРВЫМ, до обработчика обычных сообщений
     */
    process.once('message', async (rawMsg) => {
        try {
            // Десериализуем INIT сообщение (JSON или MessagePack)
            const msg = ipcSerializer.receive(rawMsg);
            
            if (!msg || msg.type !== 'INIT') {
                throw new Error('Invalid initialization message - expected INIT');
            }
            
            const initConnectionString = msg.connectionString;
            const initDatabaseName = msg.databaseName;
            const initDatabaseOptions = msg.databaseOptions;
            
            if (!initConnectionString || !initDatabaseName) {
                throw new Error('Missing required initialization parameters');
            }
            
            // Устанавливаем параметры подключения
            connectionString = initConnectionString;
            databaseName = initDatabaseName;
            databaseOptions = initDatabaseOptions || {};
            
            // Подключаемся к MongoDB
            await connect();
        } catch (error) {
            logger.error(`QueryWorker: Критическая ошибка инициализации: ${error.message}`);
            ipcSerializer.send(process, {
                type: 'ERROR',
                error: serializeError(error),
                message: 'Initialization failed'
            });
            process.exit(1);
        }
    });
    
    /**
     * Обработка IPC сообщений от родительского процесса (QUERY, SHUTDOWN)
     * Устанавливается после обработчика INIT
     */
    process.on('message', async (rawMessage) => {
        try {
            // Десериализуем сообщение (JSON или MessagePack)
            const message = ipcSerializer.receive(rawMessage);
            
            // Игнорируем INIT сообщения - они обрабатываются отдельным обработчиком
            if (message && message.type === 'INIT') {
                return;
            }
            
            if (!message || typeof message !== 'object') {
                logger.warn('QueryWorker: Получено некорректное сообщение');
                return;
            }
            
            if (message.type === 'QUERY') {
                const { id, query, collectionName, options } = message;
                
                if (!id || !query || !collectionName) {
                    logger.warn('QueryWorker: Некорректная структура QUERY сообщения');
                    sendQueryResult(id || 'unknown', {
                        result: null,
                        error: serializeError(new Error('Invalid query message structure')),
                        metrics: { queryTime: 0, querySize: 0, resultSize: 0 }
                    });
                    return;
                }
                
                const queryResult = await executeQuery(id, query, collectionName, options || {});
                sendQueryResult(id, queryResult);
            } else if (message.type === 'QUERY_BATCH') {
                const { batchId, requests } = message;

                if (!batchId) {
                    logger.warn('QueryWorker: Получен QUERY_BATCH без batchId');
                    return;
                }

                // Запускаем обработку батча асинхронно, не блокируя обработчик сообщений
                // Это позволяет воркеру обрабатывать несколько батчей параллельно
                const batchPromise = (async () => {
                    try {
                        const batchResults = await executeQueryBatch(batchId, requests);
                        sendBatchResult(batchId, batchResults);
                        return batchResults;
                    } catch (error) {
                        logger.error(`QueryWorker: Ошибка выполнения батча ${batchId}: ${error.message}`);
                        const fallbackResults = Array.isArray(requests)
                            ? requests.map((request, index) => ({
                                id: request?.id || `${batchId}_${index}`,
                                result: null,
                                error: serializeError(error),
                                metrics: { queryTime: 0, querySize: 0, resultSize: 0 }
                            }))
                            : [];
                        sendBatchResult(batchId, fallbackResults);
                        return fallbackResults;
                    } finally {
                        // Удаляем батч из активных после завершения
                        activeBatches.delete(batchId);
                    }
                })();
                
                // Сохраняем промис для отслеживания активных батчей
                activeBatches.set(batchId, batchPromise);
            } else if (message.type === 'SHUTDOWN') {
                await shutdown();
            } else if (message.type !== 'INIT') {
                // Игнорируем INIT, так как он обработан выше
                logger.warn(`QueryWorker: Неизвестный тип сообщения: ${message.type}`);
            }
        } catch (error) {
            logger.error(`QueryWorker: Ошибка обработки сообщения: ${error.message}`);
            
            // Отправляем ошибку, если есть ID запроса
            if (message && message.id) {
                sendQueryResult(message.id, {
                    result: null,
                    error: serializeError(error),
                    metrics: { queryTime: 0, querySize: 0, resultSize: 0 }
                });
            }
        }
    });
}

// Если модуль запущен напрямую (через fork), запускаем worker
// Когда модуль запускается через child_process.fork(), require.main === module будет true
if (require.main === module) {
    startQueryWorker().catch((error) => {
        console.error('QueryWorker: Fatal error:', error);
        process.exit(1);
    });
}

module.exports = {
    startQueryWorker,
    _getMongoClientAggregate: _getMongoClient,
    serializeError
};

