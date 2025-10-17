const express = require('express');
const Logger = require('../utils/logger');
const xml2js = require('xml2js');
const { ObjectId } = require('mongodb');

const { ERROR_WRONG_MESSAGE_TYPE } = require('../common/errors');
const config = require('../common/config');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Глобальные переменные для отслеживания статистики запросов по потокам
const threadStats = new Map(); // Map<processId, {requestCounter, maxProcessingTime, maxMetrics, maxDebugInfo}>

/**
 * Получает или создает статистику для текущего потока
 * @returns {Object} объект со статистикой потока
 */
function getThreadStats() {
    const processId = process.pid;
    
    if (!threadStats.has(processId)) {
        threadStats.set(processId, {
            requestCounter: 0,
            maxProcessingTime: null,
            maxMetrics: null,
            maxDebugInfo: null
        });
    }
    
    return threadStats.get(processId);
}

/**
 * Очищает статистику для текущего потока
 */
function clearThreadStats() {
    const processId = process.pid;
    threadStats.delete(processId);
}

/**
 * Очищает статистику для всех потоков
 */
function clearAllThreadStats() {
    threadStats.clear();
}

/**
 * Вычисляет CRC32 хеш для строки
 * @param {string} str - строка для хеширования
 * @returns {number} CRC32 хеш
 */
function crc32(str) {
    const buffer = Buffer.from(str, 'utf8');
    let crc = 0xFFFFFFFF;
    
    for (let i = 0; i < buffer.length; i++) {
        crc ^= buffer[i];
        for (let j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >>> 1) ^ 0xEDB88320;
            } else {
                crc = crc >>> 1;
            }
        }
    }
    
    return (crc ^ 0xFFFFFFFF) >>> 0;
}

// В src/utils/dateFormatter.js
function formatDatesInObject(obj, dateFormat = 'iso') {
    const formatters = {
        'iso': (date) => date.toISOString(),
        'local': (date) => date.toLocaleString('sv-SE'),
        'custom': (date) => date.toISOString().replace('T', ' ').replace('Z', ''),
        'russian': (date) => date.toLocaleDateString('ru-RU')
    };
    
    const formatter = formatters[dateFormat] || formatters.iso;
    
    function processValue(value) {
        if (value instanceof Date) {
            return formatter(value);
        } else if (value instanceof ObjectId) {
            return value.toString();
        } else if (Array.isArray(value)) {
            return value.map(processValue);
        } else if (value && typeof value === 'object') {
            // Проверяем, является ли объект примитивным (имеет метод toString)
            // и не является ли он обычным объектом с ключами
            if (value.constructor && value.constructor.name !== 'Object' && 
                typeof value.toString === 'function' && 
                Object.keys(value).length === 0) {
                return value.toString();
            }
            
            const result = {};
            for (const [key, val] of Object.entries(value)) {
                result[key] = processValue(val);
            }
            return result;
        }
        return value;
    }
    
    return processValue(obj);
}

/**
 * Проверяет, разрешен ли тип сообщения для обработки
 * @param {number} messageType - тип сообщения
 * @returns {boolean} true если тип разрешен, false если нет
 */
function isMessageTypeAllowed(messageType) {
    const allowedTypes = config.messageTypes.allowedTypes;
    
    // Если список не задан, обрабатываем все типы
    if (!allowedTypes || allowedTypes.length === 0) {
        return true;
    }
    
    // Проверяем, есть ли тип в списке разрешенных
    return allowedTypes.includes(messageType);
}

/**
 * Сохраняет отладочную информацию в лог, если достигнут лимит запросов
 * @param {Object} factController - экземпляр FactController
 * @param {Object} processingTime - время обработки
 * @param {Object} metrics - метрики
 * @param {Object} debugInfo - отладочная информация
 */
async function saveDebugInfoIfNeeded(factController, message, processingTime, metrics, debugInfo) {
    try {
        // Получаем частоту сохранения из конфигурации
        const logSaveFrequency = config.logging.saveFrequency;
        
        // Получаем статистику для текущего потока
        const stats = getThreadStats();
        
        // Увеличиваем счетчик запросов
        stats.requestCounter++;
        
        // Обновляем максимальное время обработки и связанную информацию
        if (!stats.maxProcessingTime || (processingTime.total > stats.maxProcessingTime.total)) {
            stats.maxProcessingTime = processingTime;
            stats.maxMetrics = metrics;
            stats.maxDebugInfo = debugInfo;
            stats.maxMessage = message;
        }
        
        // Проверяем, достигли ли лимита запросов
        if (stats.requestCounter >= logSaveFrequency) {
            if (factController && factController.dbProvider) {
                const processId = process.pid;
                // Сохраняем в лог
                await factController.dbProvider.saveLog(processId, stats.maxMessage, stats.maxProcessingTime, stats.maxMetrics, stats.maxDebugInfo);
                
                logger.debug(`Отладочная информация сохранена в лог для потока ${processId}`);
            }
            
            // Сбрасываем счетчики для текущего потока
            stats.requestCounter = 0;
            stats.maxProcessingTime = null;
            stats.maxMetrics = null;
            stats.maxDebugInfo = null;
            stats.maxMessage = null;
        }
    } catch (error) {
        logger.error('Ошибка при сохранении отладочной информации в лог:', {
            error: error.message,
            stack: error.stack
        });
    }
}

/**
 * Создает маршруты API
 * @param {Object} factController - экземпляр FactController
 * @returns {Object} Express router
 */
function createRoutes(factController) {
    const router = express.Router();

    // Health check endpoint
    router.get('/health', (req, res) => {
        const memoryUsage = process.memoryUsage();
        res.json({
            status: 'OK',
            worker: process.pid,
            uptime: process.uptime(),
            memory: {
                rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`,
                heapTotal: `${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`,
                heapUsed: `${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB`,
                external: `${Math.round(memoryUsage.external / 1024 / 1024)}MB`
            },
            timestamp: new Date().toISOString()
        });
    });

    // API v1 routes
    const apiV1 = express.Router();

    // POST /api/v1/message/{messageType}/json
    apiV1.post('/message/:messageType/json', async (req, res) => {
        try {
            const { messageType } = req.params;
            const messageData = req.body;
            const debugMode = req.headers['debug-mode'] === 'true';
            if (debugMode) {
                logger.info(`ВКЛЮЧЕН РЕЖИМ ОТЛАДКИ`);
            }
    
            /*
            logger.info('Message processing - request data:', {
                messageType,
                messageData,
                messageDataType: typeof messageData,
                isArray: Array.isArray(messageData),
                bodyKeys: messageData ? Object.keys(messageData) : 'no keys'
            });
            */

            // Валидация входных данных
            if (!messageType || messageType.trim() === '') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный параметр messageType',
                    message: 'messageType не может быть пустым'
                });
            }

            if (!messageData || typeof messageData !== 'object' || Array.isArray(messageData)) {
                return res.status(400).json({
                    success: false,
                    error: 'Неверные данные события',
                    message: 'Тело запроса должно содержать валидный JSON объект'
                });
            }

            // Преобразуем messageType в число
            const messageTypeNumber = parseInt(messageType.trim());
            
            // Проверяем, разрешен ли этот тип сообщения
            if (!isMessageTypeAllowed(messageTypeNumber)) {
                logger.warn(`Тип сообщения ${messageTypeNumber} не разрешен для обработки - возвращаем пустой ответ`, {
                    allowedTypes: config.messageTypes.allowedTypes,
                    messageType: messageTypeNumber
                });
                return res.status(200).json({status: `Тип сообщения ${messageTypeNumber} не разрешен для обработки. Обрабатываются только типы сообщений: ${config.messageTypes.allowedTypes.join(', ')}`});
            }

            // Добавляем тип события в данные
            const message = {
                t: messageTypeNumber,
                d: messageData  // Данные события должны быть в поле 'd'
            };

            logger.debug(`Обработка JSON события типа: ${messageTypeNumber}`, { messageData });

            // Проверяем, что контроллер инициализирован
            if (!factController) {
                logger.error('FactController не инициализирован');
                return res.status(500).json({
                    success: false,
                    error: 'Сервис не готов',
                    message: 'FactController не инициализирован'
                });
            }

            // Обрабатываем сообщение через контроллер
            const result = await factController.processMessageWithCounters(message, debugMode);

            // Асинхронно сохраняем отладочную информацию (не блокируем ответ)
            saveDebugInfoIfNeeded(factController, message, result.processingTime, result.metrics, result.debug)
                .catch(error => {
                    logger.error('Ошибка при сохранении отладочной информации:', error);
                });

            logger.info(`Сообщение ${messageType} успешно обработано`, {
                factId: result.fact._id,
                processingTime: result.processingTime ? result.processingTime.total : 'N/A'
            });

            res.json({
                messageType,
                factId: result.fact._id,
                counters: result.counters || {},
                processingTime: result.processingTime || { total: 0, message: 'No processing time available' },
                metrics: result.metrics,
                debug: result.debug
            });

        } catch (error) {
            logger.error(`Ошибка обработки JSON события ${req.params.messageType}:`, {
                error: error.message,
                stack: error.stack,
                messageData: req.body
            });

            res.status(500).json({
                success: false,
                error: 'Ошибка обработки события',
                message: error.message,
                timestamp: new Date().toISOString()
            });
        }
    });

    // POST /api/v1/message/iris
    apiV1.post('/message/iris', async (req, res) => {
        try {
            // Извлекаем messageType из атрибута MessageTypeId входящего документа
            const messageType = req.body?.MessageTypeId;
            
            // Валидация входных данных
            if (!messageType || messageType.trim() === '') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный атрибут MessageTypeId',
                    message: 'MessageTypeId не может быть пустым'
                });
            }

            // Преобразуем messageType в число
            const messageTypeNumber = parseInt(messageType.trim());
            
            // Проверяем, разрешен ли этот тип сообщения
            if (!isMessageTypeAllowed(messageTypeNumber)) {
                logger.warn(`Тип IRIS сообщения ${messageTypeNumber} не разрешен для обработки - возвращаем пустой IRIS узел`, {
                    allowedTypes: config.messageTypes.allowedTypes,
                    messageType: messageTypeNumber
                });
                
                // Создаем пустой XML ответ с правильными атрибутами
                const emptyResponse = {
                    IRIS: {
                        status: `Тип сообщения ${messageTypeNumber} не разрешен для обработки. Обрабатываются только типы сообщений: ${config.messageTypes.allowedTypes.join(', ')}`
                    },
                    _attributes: {
                        Version: '1',
                        Message: 'ModelResponse',
                        MessageTypeId: messageType,
                        MessageId: req.body?.MessageId || 'unknown'
                    }
                };

                const formattedEmptyResponse = formatDatesInObject(emptyResponse, 'custom');
                const builder = new xml2js.Builder({
                    rootName: 'IRIS',
                    headless: false,
                    renderOpts: { 
                        pretty: true,
                        indent: '  ',
                        newline: '\n'
                    },
                    attrkey: '_attributes',
                    charkey: '_text',
                    explicitArray: false,
                    mergeAttrs: true
                });
                const xmlEmptyResponse = builder.buildObject(formattedEmptyResponse);

                res.set('Content-Type', 'application/xml');
                return res.status(200).send(xmlEmptyResponse);
            }

            // Извлекаем данные из XML запроса
            const xmlData = req.body;
            const messageId = xmlData.MessageId || 'unknown';
            
            // Проверяем, нужно ли обрабатывать этот запрос (уменьшение трафика)
            if (config.messageTypes.irisTrafficReductionFactor > 1) {
                const messageIdCRC32 = crc32(messageId);
                const shouldProcess = (messageIdCRC32 % config.messageTypes.irisTrafficReductionFactor) === 0;
                
                if (!shouldProcess) {
                    logger.info(`IRIS запрос с MessageId ${messageId} пропущен из-за коэффициента уменьшения трафика ${config.messageTypes.irisTrafficReductionFactor}`, {
                        messageId,
                        messageIdCRC32,
                        remainder: messageIdCRC32 % config.messageTypes.irisTrafficReductionFactor
                    });
                    
                    // Возвращаем пустой ответ для пропущенных запросов
                    const emptyResponse = {
                        IRIS: {
                            status: `Запрос пропущен из-за коэффициента уменьшения трафика ${config.messageTypes.irisTrafficReductionFactor}`
                        },
                        _attributes: {
                            Version: '1',
                            Message: 'ModelResponse',
                            MessageTypeId: messageType,
                            MessageId: messageId
                        }
                    };

                    const formattedEmptyResponse = formatDatesInObject(emptyResponse, 'custom');
                    const builder = new xml2js.Builder({
                        rootName: 'IRIS',
                        headless: false,
                        renderOpts: { 
                            pretty: true,
                            indent: '  ',
                            newline: '\n'
                        },
                        attrkey: '_attributes',
                        charkey: '_text',
                        explicitArray: false,
                        mergeAttrs: true
                    });
                    const xmlEmptyResponse = builder.buildObject(formattedEmptyResponse);

                    res.set('Content-Type', 'application/xml');
                    return res.status(200).send(xmlEmptyResponse);
                }
            }

            logger.debug('Получен запрос IRIS', {
                body: req.body,
                headers: req.headers
            });

            const debugMode = req.headers['debug-mode'] === 'true';
            if (debugMode) {
                logger.info(`ВКЛЮЧЕН РЕЖИМ ОТЛАДКИ`);
            }
    
            if (!req.body || typeof req.body !== 'object') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверные данные XML',
                    message: 'Тело запроса должно содержать валидный XML'
                });
            }

            // Преобразуем XML данные в формат для обработки
            // Убираем атрибуты корневого элемента и оставляем только данные
            const messageData = { ...xmlData };
            delete messageData.Version;
            delete messageData.Message;
            delete messageData.MessageTypeId;

            // Создаем сообщение в формате для FactController
            const message = {
                t: messageTypeNumber,
                d: messageData
            };

            logger.info(`Обработка IRIS события типа: ${messageTypeNumber}`, { message });

            // Проверяем, что контроллер инициализирован
            if (!factController) {
                logger.error('FactController не инициализирован');
                return res.status(500).json({
                    success: false,
                    error: 'Сервис не готов',
                    message: 'FactController не инициализирован'
                });
            }

            // Обрабатываем сообщение через контроллер
            const result = await factController.processMessageWithCounters(message, debugMode);

            // Асинхронно сохраняем отладочную информацию (не блокируем ответ)
            saveDebugInfoIfNeeded(factController, message, result.processingTime, result.metrics, result.debug)
                .catch(error => {
                    logger.error('Ошибка при сохранении отладочной информации:', error);
                });

            logger.info(`IRIS сообщение ${messageType} успешно обработано`, {
                factId: result.fact._id,
                processingTime: result.processingTime ? result.processingTime.total : 'N/A'
            });

            // Создаем JSON ответ с правильной структурой для XML
            const jsonResponse = {
                IRIS: {
                    FactId: result.fact._id,
                    Counters: result.counters,
                    Timestamp: new Date().toISOString(),
                    ProcessingTime: result.processingTime || { total: 0 },
                    Metrics: result.metrics,
                    Debug: JSON.stringify(result.debug)
                },
                _attributes: {
                    Version: '1',
                    Message: 'ModelResponse',
                    MessageTypeId: messageType,
                    MessageId: messageId
                }
            };

            // Конвертируем JSON в XML
            const formattedJson = formatDatesInObject(jsonResponse, 'custom');
            const builder = new xml2js.Builder({
                rootName: 'IRIS',
                headless: false, // эквивалент header: true в json2xml
                renderOpts: { 
                    pretty: true,
                    indent: '  ',
                    newline: '\n'
                },
                // Обработка атрибутов (аналог attributes_key: '_attributes')
                attrkey: '_attributes',
                // Обработка текстового содержимого (аналог chars_key: '_text') 
                charkey: '_text',
                // Дополнительные опции для совместимости
                explicitArray: false,
                mergeAttrs: true
            });
            xmlResponse = builder.buildObject(formattedJson);

            // Устанавливаем правильный Content-Type для XML
            res.set('Content-Type', 'application/xml');
            res.send(xmlResponse);

        } catch (error) {
            const messageType = req.body?.MessageTypeId || 'unknown';
            const messageId = req.body?.MessageId || 'unknown';
            
            logger.error(`Ошибка обработки IRIS события ${messageType} MessageId: ${messageId}:`, {
                error: error.message,
                stack: error.stack,
                messageData: req.body
            });

            // Создаем XML ответ с ошибкой
            const errorResponse = {
                IRIS: {
                    Error: {
                        Code: '500',
                        Message: error.message,
                        Timestamp: new Date()
                    }
                },
                _attributes: {
                    Version: '1',
                    Message: 'ModelResponse',
                    MessageTypeId: messageType,
                    MessageId: messageId
                }
            };

            const formattedErrorResponse = formatDatesInObject(errorResponse, 'custom');
            const builder = new xml2js.Builder({
                rootName: 'IRIS',
                headless: false,
                renderOpts: { 
                    pretty: true,
                    indent: '  ',
                    newline: '\n'
                },
                attrkey: '_attributes',
                charkey: '_text',
                explicitArray: false,
                mergeAttrs: true
            });
            const xmlErrorResponse = builder.buildObject(formattedErrorResponse);

            res.set('Content-Type', 'application/xml');
            res.status(500).send(xmlErrorResponse);
        }
    });

    // GET /api/v1/message/{messageType}/iris
    apiV1.get('/message/:messageType/iris', (req, res) => {
        try {
            const { messageType } = req.params;
            
            logger.debug('IRIS Message generation request:', {
                messageType,
                messageTypeType: typeof messageType
            });

            // Валидация входных данных
            if (!messageType || messageType.trim() === '') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный параметр messageType',
                    message: 'messageType не может быть пустым'
                });
            }

            // Преобразуем messageType в число
            const messageTypeNumber = parseInt(messageType.trim());
            if (isNaN(messageTypeNumber)) {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный формат messageType',
                    message: 'messageType должен быть числом'
                });
            }

            // Генерируем сообщение указанного типа
            const generatedMessage = factController.messageGenerator.generateMessage(messageTypeNumber);
            const formattedMessageData = formatDatesInObject(generatedMessage.d, 'custom');

            // Создаем XML вручную для корректного форматирования
            const messageId = new ObjectId().toString();
            let xml = `<IRIS Version="1" Message="ModelRequest" MessageTypeId="${messageTypeNumber}" MessageId="${messageId}">\n`;
            
            // Добавляем все поля из сгенерированного сообщения как дочерние элементы
            for (const [key, value] of Object.entries(formattedMessageData)) {
                xml += `\t<${key}>${value}</${key}>\n`;
            }
            
            xml += '</IRIS>';
            
            logger.info(`IRIS сообщение типа ${messageTypeNumber} успешно сгенерировано`);

            // Устанавливаем правильный Content-Type для XML
            res.set('Content-Type', 'application/xml');
            res.send(xml);

        } catch (error) {
            logger.error(`Ошибка генерации IRIS сообщения типа ${req.params.messageType}:`, {
                error: error.message,
                stack: error.stack
            });

            if (error.code === ERROR_WRONG_MESSAGE_TYPE) {
                return res.status(400).json({
                    success: false,
                    error: 'Ошибка генерации сообщения',
                    message: error.message
                });
            }
            res.status(500).json({
                success: false,
                error: 'Ошибка генерации сообщения',
                message: error.message,
                timestamp: new Date().toISOString()
            });
        }
    });

    // GET /api/v1/message/{messageType}/json
    apiV1.get('/message/:messageType/json', (req, res) => {
        try {
            const { messageType } = req.params;
            
            logger.debug('Message generation request:', {
                messageType,
                messageTypeType: typeof messageType
            });

            // Валидация входных данных
            if (!messageType || messageType.trim() === '') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный параметр messageType',
                    message: 'messageType не может быть пустым'
                });
            }

            // Преобразуем messageType в число
            const messageTypeNumber = parseInt(messageType.trim());
            if (isNaN(messageTypeNumber)) {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный формат messageType',
                    message: 'messageType должен быть числом'
                });
            }

            // Генерируем сообщение указанного типа
            const generatedMessage = factController.messageGenerator.generateMessage(messageTypeNumber);
            
            logger.info(`Сообщение типа ${messageTypeNumber} успешно сгенерировано`);

            res.json(generatedMessage.d);

        } catch (error) {
            logger.error(`Ошибка генерации сообщения типа ${req.params.messageType}:`, {
                error: error.message,
                stack: error.stack
            });

            if (error.code === ERROR_WRONG_MESSAGE_TYPE) {
                return res.status(400).json({
                    success: false,
                    error: 'Ошибка генерации сообщения',
                    message: error.message
                });
            }
            res.status(500).json({
                success: false,
                error: 'Ошибка генерации сообщения',
                message: error.message,
                timestamp: new Date().toISOString()
            });
        }
    });

    // Подключаем API v1 к основному роутеру
    router.use('/api/v1', apiV1);

    return router;
}

module.exports = { 
    createRoutes, 
    getThreadStats, 
    clearThreadStats, 
    clearAllThreadStats 
};
