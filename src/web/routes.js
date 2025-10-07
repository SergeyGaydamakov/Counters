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

    // POST /api/v1/event/{eventType}/json
    apiV1.post('/event/:eventType/json', async (req, res) => {
        try {
            const { eventType } = req.params;
            const eventData = req.body;

            logger.debug('Event processing - request data:', {
                eventType,
                eventData,
                eventDataType: typeof eventData,
                isArray: Array.isArray(eventData),
                bodyKeys: eventData ? Object.keys(eventData) : 'no keys'
            });

            // Валидация входных данных
            if (!eventType || eventType.trim() === '') {
                return res.status(400).json({
                    success: false,
                    error: 'Неверный параметр eventType',
                    message: 'eventType не может быть пустым'
                });
            }

            if (!eventData || typeof eventData !== 'object' || Array.isArray(eventData)) {
                return res.status(400).json({
                    success: false,
                    error: 'Неверные данные события',
                    message: 'Тело запроса должно содержать валидный JSON объект'
                });
            }

            // Добавляем тип события в данные
            const event = {
                t: parseInt(eventType.trim()), // Преобразуем в число
                d: eventData  // Данные события должны быть в поле 'd'
            };

            logger.debug(`Обработка JSON события типа: ${eventType}`, { eventData });

            // Проверяем, что контроллер инициализирован
            if (!factController) {
                logger.error('FactController не инициализирован');
                return res.status(500).json({
                    success: false,
                    error: 'Сервис не готов',
                    message: 'FactController не инициализирован'
                });
            }

            // Обрабатываем событие через контроллер
            const result = await factController.processEventWithCounters(event);

            logger.info(`Событие ${eventType} успешно обработано`, {
                factId: result.fact._id,
                processingTime: result.processingTime.total
            });

            res.json({
                success: true,
                eventType,
                factId: result.fact._id,
                processingTime: result.processingTime,
                counters: result.counters,
                timestamp: new Date().toISOString()
            });

        } catch (error) {
            logger.error(`Ошибка обработки JSON события ${req.params.eventType}:`, {
                error: error.message,
                stack: error.stack,
                eventData: req.body
            });

            res.status(500).json({
                success: false,
                error: 'Ошибка обработки события',
                message: error.message,
                timestamp: new Date().toISOString()
            });
        }
    });

    // POST /api/v1/event/{eventType}/iris (заглушка)
    apiV1.post('/event/:eventType/iris', (req, res) => {
        const { eventType } = req.params;
        
        logger.info(`Получен запрос IRIS для типа события: ${eventType}`, {
            body: req.body,
            headers: req.headers
        });

        res.status(501).json({
            success: false,
            error: 'IRIS обработка не реализована',
            message: 'Данный endpoint находится в разработке',
            eventType,
            timestamp: new Date().toISOString()
        });
    });

    // Подключаем API v1 к основному роутеру
    router.use('/api/v1', apiV1);

    return router;
}

module.exports = { createRoutes };
