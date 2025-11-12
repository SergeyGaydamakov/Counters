const Logger = require('../common/logger');
const xml2js = require('xml2js');
const config = require('../common/config');
const { crc32 } = require('../common/crc32');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

/**
 * Middleware для логирования запросов
 */
const requestLogger = (req, res, next) => {
    const start = Date.now();
    const workerId = process.pid;
    
    res.on('finish', () => {
        const duration = Date.now() - start;
        const logData = {
            method: req.method,
            url: req.originalUrl,
            statusCode: res.statusCode,
            duration: `${duration}ms`,
            worker: workerId,
            userAgent: req.get('User-Agent'),
            ip: req.ip || req.connection.remoteAddress
        };
        
        if (res.statusCode >= 400) {
            logger.warn('HTTP Request', logData);
        } else {
            logger.debug('HTTP Request', logData);
        }
    });
    
    next();
};

/**
 * Middleware для валидации JSON
 */
const jsonValidator = (req, res, next) => {
    if (req.method === 'POST' && req.is('application/json')) {
        try {
            logger.debug('JSON Validator - req.body:', { 
                body: req.body, 
                type: typeof req.body,
                isArray: Array.isArray(req.body)
            });
            
            if (req.body && typeof req.body === 'object' && !Array.isArray(req.body)) {
                next();
            } else {
                res.status(400).json({
                    success: false,
                    error: 'Неверный формат JSON',
                    message: 'Тело запроса должно содержать валидный JSON объект'
                });
            }
        } catch (error) {
            res.status(400).json({
                success: false,
                error: 'Ошибка парсинга JSON',
                message: error.message
            });
        }
    } else {
        next();
    }
};

/**
 * Извлекает MessageTypeId из начала XML строки без полного парсинга
 * @param {string} xmlData - XML строка
 * @returns {number|null} - MessageTypeId или null, если не найден
 */
function extractMessageTypeIdFromXmlStart(xmlData) {
    // Ищем паттерн <IRIS ... MessageTypeId="число" ...>
    // Поддерживаем различные варианты расположения атрибутов
    const match = xmlData.match(/<IRIS[^>]*MessageTypeId\s*=\s*["'](\d+)["'][^>]*>/i);
    if (match && match[1]) {
        const messageTypeId = parseInt(match[1], 10);
        if (!isNaN(messageTypeId)) {
            return messageTypeId;
        }
    }
    return null;
}

/**
 * Извлекает MessageId из начала XML строки без полного парсинга
 * @param {string} xmlData - XML строка
 * @returns {string|null} - MessageId или null, если не найден
 */
function extractMessageIdFromXmlStart(xmlData) {
    // Ищем паттерн <IRIS ... MessageId="значение" ...>
    // Поддерживаем различные варианты расположения атрибутов
    const match = xmlData.match(/<IRIS[^>]*MessageId\s*=\s*["']([^"']+)["'][^>]*>/i);
    if (match && match[1]) {
        return match[1];
    }
    return null;
}

/**
 * Проверяет, разрешен ли тип сообщения для обработки
 * @param {number} messageType - тип сообщения
 * @returns {boolean} true если тип разрешен, false если нет
 */
function isMessageTypeAllowed(messageType) {
    const allowedTypes = config.messageTypes?.allowedTypes;
    
    // Если список не задан, обрабатываем все типы
    if (!allowedTypes || allowedTypes.length === 0) {
        return true;
    }
    
    // Проверяем, есть ли тип в списке разрешенных
    return allowedTypes.includes(messageType);
}

/**
 * Создает простой XML ответ для неразрешенных типов сообщений
 * @param {number} messageTypeId - тип сообщения
 * @param {string} messageId - ID сообщения
 * @returns {string} XML строка
 */
function createRejectedXmlResponse(messageTypeId, messageId = 'unknown') {
    const allowedTypes = config.messageTypes?.allowedTypes || [];
    const allowedTypesStr = allowedTypes.length > 0 ? allowedTypes.join(', ') : 'не заданы';
    
    return `<?xml version="1.0" encoding="UTF-8"?>
<IRIS Version="1" Message="ModelResponse" MessageTypeId="${messageTypeId}" MessageId="${messageId}">
  <status>Тип сообщения ${messageTypeId} не разрешен для обработки. Обрабатываются только типы сообщений: ${allowedTypesStr}</status>
</IRIS>`;
}

/**
 * Создает простой XML ответ для пропущенных запросов из-за уменьшения трафика
 * @param {number} messageTypeId - тип сообщения
 * @param {string} messageId - ID сообщения
 * @returns {string} XML строка
 */
function createTrafficReductionXmlResponse(messageTypeId, messageId = 'unknown') {
    const factor = config.messageTypes?.irisTrafficReductionFactor || 1;
    const value = config.messageTypes?.irisTrafficReductionValue || 0;
    
    return `<?xml version="1.0" encoding="UTF-8"?>
<IRIS Version="1" Message="ModelResponse" MessageTypeId="${messageTypeId}" MessageId="${messageId}">
  <status>Запрос пропущен из-за коэффициента уменьшения трафика ${factor} и значения ${value}</status>
</IRIS>`;
}

/**
 * Middleware для парсинга XML запросов для IRIS маршрутов
 * Должен быть размещен ДО express.json()
 * Включает раннюю фильтрацию по MessageTypeId для ускорения обработки
 * Проверяет только начало строки без полного парсинга XML
 */
const irisXmlParser = (req, res, next) => {
    // Проверяем, что это IRIS маршрут и Content-Type XML
    if (req.path.includes('/iris') && req.method === 'POST' && 
        (req.is('application/xml') || req.is('text/xml'))) {
        let data = '';
        let requestRejected = false;
        
        req.on('data', chunk => {
            if (!requestRejected) {
                data += chunk;
                
                // Проверяем начало строки, как только получили открывающий тег IRIS с закрывающей скобкой
                // Это позволяет отбросить запрос максимально быстро, не дожидаясь всего body
                if (data.includes('<IRIS') && data.includes('>')) {
                    const messageTypeId = extractMessageTypeIdFromXmlStart(data);
                    const messageId = extractMessageIdFromXmlStart(data) || 'unknown';
                    
                    if (messageTypeId !== null) {
                        // Проверяем, разрешен ли этот тип сообщения
                        if (!isMessageTypeAllowed(messageTypeId)) {
                            requestRejected = true;
                            
                            logger.warn(`Тип IRIS сообщения ${messageTypeId} не разрешен для обработки - ранняя фильтрация (проверка начала строки)`, {
                                allowedTypes: config.messageTypes?.allowedTypes,
                                messageType: messageTypeId
                            });
                            
                            // Возвращаем простой XML ответ без парсинга XML
                            res.set('Content-Type', 'application/xml');
                            res.status(200).send(createRejectedXmlResponse(messageTypeId, messageId));
                            return;
                        }
                        
                        // Проверяем уменьшение трафика (только если тип разрешен)
                        if (config.messageTypes?.irisTrafficReductionFactor > 1) {
                            const messageIdCRC32 = crc32(messageId);
                            const shouldProcess = (messageIdCRC32 % config.messageTypes.irisTrafficReductionFactor) === config.messageTypes.irisTrafficReductionValue;
                            
                            if (!shouldProcess) {
                                requestRejected = true;
                                
                                logger.debug(`IRIS запрос с MessageId ${messageId} пропущен из-за коэффициента уменьшения трафика ${config.messageTypes.irisTrafficReductionFactor} - ранняя фильтрация (проверка начала строки)`, {
                                    messageId,
                                    messageTypeId,
                                    messageIdCRC32,
                                    remainder: messageIdCRC32 % config.messageTypes.irisTrafficReductionFactor,
                                    irisTrafficReductionValue: config.messageTypes.irisTrafficReductionValue
                                });
                                
                                // Возвращаем простой XML ответ без парсинга XML
                                res.set('Content-Type', 'application/xml');
                                res.status(200).send(createTrafficReductionXmlResponse(messageTypeId, messageId));
                                return;
                            }
                        }
                    }
                }
            }
        });
        
        req.on('end', () => {
            // Если запрос уже был отклонен, ничего не делаем
            if (requestRejected) {
                return;
            }
            
            try {
                // Парсим XML только если тип разрешен или не удалось определить тип
                const parser = new xml2js.Parser({
                    explicitArray: false,
                    mergeAttrs: true,
                    explicitRoot: false
                });

                parser.parseString(data, (err, result) => {
                    if (err) {
                        logger.error('XML parsing error:', { error: err.message, data });
                        return res.status(400).json({
                            success: false,
                            error: 'Ошибка парсинга XML',
                            message: err.message
                        });
                    }
                    
                    req.body = result;
                    logger.debug('XML parsed successfully:', { body: req.body });
                    next();
                });
            } catch (error) {
                logger.error('XML parsing error:', { error: error.message, data });
                res.status(400).json({
                    success: false,
                    error: 'Ошибка парсинга XML',
                    message: error.message
                });
            }
        });
    } else {
        next();
    }
};

/**
 * Middleware для обработки ошибок
 */
const errorHandler = (error, req, res, next) => {
    logger.error(`Необработанная ошибка в воркере ${process.pid}:`, {
        error: error.message,
        stack: error.stack,
        url: req.originalUrl,
        method: req.method,
        body: req.body
    });

    // Не отправляем детали ошибки в production
    const isDevelopment = process.env.NODE_ENV === 'development';
    
    res.status(500).json({
        success: false,
        error: 'Внутренняя ошибка сервера',
        message: isDevelopment ? error.message : 'Произошла внутренняя ошибка',
        timestamp: new Date().toISOString(),
        ...(isDevelopment && { stack: error.stack })
    });
};

/**
 * Middleware для обработки 404
 */
const notFoundHandler = (req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint не найден',
        message: `Маршрут ${req.method} ${req.originalUrl} не существует`,
        availableEndpoints: [
            'GET /health',
            'GET /metrics',
            'GET /cluster-metrics',
            'GET /api/v1/message/{messageType}/json',
            'GET /api/v1/message/{messageType}/iris',
            'POST /api/v1/message/{messageType}/json',
            'POST /api/v1/message/iris'
        ],
        timestamp: new Date().toISOString()
    });
};

/**
 * Middleware для добавления метаданных к ответу
 */
const responseMetadata = (req, res, next) => {
    const originalJson = res.json;
    
    res.json = function(data) {
        if (data && typeof data === 'object' && !data.timestamp) {
            data.timestamp = new Date().toISOString();
        }
        if (data && typeof data === 'object' && !data.worker) {
            data.worker = process.pid;
        }
        return originalJson.call(this, data);
    };
    
    next();
};

module.exports = {
    requestLogger,
    jsonValidator,
    irisXmlParser,
    errorHandler,
    notFoundHandler,
    responseMetadata
};
