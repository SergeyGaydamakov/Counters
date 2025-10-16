const Logger = require('../utils/logger');
const xml2js = require('xml2js');

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
            logger.info('HTTP Request', logData);
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
 * Middleware для парсинга XML запросов для IRIS маршрутов
 * Должен быть размещен ДО express.json()
 */
const irisXmlParser = (req, res, next) => {
    // Проверяем, что это IRIS маршрут и Content-Type XML
    if (req.path.includes('/iris') && req.method === 'POST' && 
        (req.is('application/xml') || req.is('text/xml'))) {
        let data = '';
        
        req.on('data', chunk => {
            data += chunk;
        });
        
        req.on('end', () => {
            try {
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
