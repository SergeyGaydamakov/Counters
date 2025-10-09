const express = require('express');
const Logger = require('../utils/logger');
const json2xml = require('json2xml');

const { ERROR_WRONG_MESSAGE_TYPE } = require('../common/errors');

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

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

            // Добавляем тип события в данные
            const message = {
                t: parseInt(messageType.trim()), // Преобразуем в число
                d: messageData  // Данные события должны быть в поле 'd'
            };

            logger.debug(`Обработка JSON события типа: ${messageType}`, { messageData });

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
            const result = await factController.processMessageWithCounters(message);

            logger.info(`Сообщение ${messageType} успешно обработано`, {
                factId: result.fact._id,
                processingTime: result.processingTime ? result.processingTime.total : 'N/A'
            });

            res.json({
                success: true,
                messageType,
                factId: result.fact._id,
                processingTime: result.processingTime || { total: 0, message: 'No processing time available' },
                counters: result.counters,
                timestamp: new Date().toISOString()
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
            logger.info('Получен запрос IRIS', {
                body: req.body,
                headers: req.headers
            });

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

            if (!req.body || typeof req.body !== 'object') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверные данные XML',
                    message: 'Тело запроса должно содержать валидный XML'
                });
            }

            // Извлекаем данные из XML запроса
            const xmlData = req.body;
            const messageId = xmlData.MessageId || 'unknown';
            
            // Преобразуем XML данные в формат для обработки
            // Убираем атрибуты корневого элемента и оставляем только данные
            const messageData = { ...xmlData };
            delete messageData.Version;
            delete messageData.Message;
            delete messageData.MessageTypeId;

            // Создаем сообщение в формате для FactController
            const message = {
                t: parseInt(messageType.trim()),
                d: messageData
            };

            logger.info(`Обработка IRIS события типа: ${messageType}`, { messageData });

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
            const result = await factController.processMessageWithCounters(message);

            logger.info(`IRIS сообщение ${messageType} успешно обработано`, {
                factId: result.fact._id,
                processingTime: result.processingTime ? result.processingTime.total : 'N/A'
            });

            // Создаем JSON ответ с правильной структурой для XML
            const jsonResponse = {
                IRIS: {
                    FactId: result.fact._id,
                    ProcessingTime: result.processingTime || { total: 0 },
                    Counters: result.counters,
                    Timestamp: new Date().toISOString()
                },
                _attributes: {
                    Version: '1',
                    Message: 'ModelResponse',
                    MessageTypeId: messageType,
                    MessageId: messageId
                }
            };

            // Конвертируем JSON в XML
            const xmlResponse = json2xml(jsonResponse, {
                header: true,
                attributes_key: '_attributes',
                chars_key: '_text'
            });

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
                        Timestamp: new Date().toISOString()
                    }
                },
                _attributes: {
                    Version: '1',
                    Message: 'ModelResponse',
                    MessageTypeId: messageType,
                    MessageId: messageId
                }
            };

            const xmlErrorResponse = json2xml(errorResponse, {
                header: true,
                attributes_key: '_attributes',
                chars_key: '_text'
            });

            res.set('Content-Type', 'application/xml');
            res.status(500).send(xmlErrorResponse);
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

module.exports = { createRoutes };
