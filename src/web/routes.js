const express = require('express');
const Logger = require('../utils/logger');

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

            logger.debug('Message processing - request data:', {
                messageType,
                messageData,
                messageDataType: typeof messageData,
                isArray: Array.isArray(messageData),
                bodyKeys: messageData ? Object.keys(messageData) : 'no keys'
            });

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
                processingTime: result.processingTime.total
            });

            res.json({
                success: true,
                messageType,
                factId: result.fact._id,
                processingTime: result.processingTime,
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

    // POST /api/v1/message/{messageType}/iris (заглушка)
    apiV1.post('/message/:messageType/iris', (req, res) => {
        const { messageType } = req.params;
        
        logger.info(`Получен запрос IRIS для типа события: ${messageType}`, {
            body: req.body,
            headers: req.headers
        });

        res.status(501).json({
            success: false,
            error: 'IRIS обработка не реализована',
            message: 'Данный endpoint находится в разработке',
            messageType,
            timestamp: new Date().toISOString()
        });
    });

    // Подключаем API v1 к основному роутеру
    router.use('/api/v1', apiV1);

    return router;
}

module.exports = { createRoutes };
