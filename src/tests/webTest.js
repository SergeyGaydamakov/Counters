const http = require('http');
const logger = require('../logger');
const config = require('../config');
const { MongoProvider, CounterProducer } = require('../index');

/**
 * Тестирование Web API
 * 
 * Обновлено в соответствии с последними изменениями:
 * - Новая структура ответов API (messageType, factId, counters, processingTime, debug)
 * - Улучшенная валидация входных данных
 * - XML ответы для IRIS endpoint
 * - Расширенная обработка ошибок
 * - Детальная статистика производительности
 */
class ApiTester {
    constructor(baseUrl = 'http://localhost:3000') {
        this.baseUrl = baseUrl;
        this.logger = logger.fromEnv('LOG_LEVEL', 'INFO');

        // Создаем простую конфигурацию счетчиков для тестов отладочной информации
        this.countersConfig = [
            {
                name: "test_counter",
                comment: "Тестовый счетчик",
                indexTypeName: "test_type_1",
                computationConditions: {},
                evaluationConditions: null,
                attributes: {
                    "count": { "$sum": 1 }
                }
            }
        ];
        this.mongoCounters = new CounterProducer(this.countersConfig);
    }

    /**
     * Выполняет HTTP запрос
     */
    async makeRequest(method, path, data = null, xmlData = false) {
        return new Promise((resolve, reject) => {
            const url = new URL(path, this.baseUrl);
            const options = {
                hostname: url.hostname,
                port: url.port,
                path: url.pathname + url.search,
                method: method,
                headers: {
                    'Content-Type': xmlData ? 'application/xml' : 'application/json'
                }
            };

            if (data) {
                const jsonData = xmlData ? data : JSON.stringify(data);
                options.headers['Content-Length'] = Buffer.byteLength(jsonData);
            }

            const req = http.request(options, (res) => {
                let responseData = '';

                res.on('data', (chunk) => {
                    responseData += chunk;
                });

                res.on('end', () => {
                    try {
                        const parsedData = JSON.parse(responseData);
                        resolve({
                            statusCode: res.statusCode,
                            headers: res.headers,
                            data: parsedData
                        });
                    } catch (error) {
                        resolve({
                            statusCode: res.statusCode,
                            headers: res.headers,
                            data: responseData
                        });
                    }
                });
            });

            req.on('error', (error) => {
                this.logger.error(`HTTP Request Error: ${error.message}`, {
                    method,
                    path,
                    url: url.toString(),
                    error: error.message,
                    code: error.code
                });
                reject(error);
            });

            if (data) {
                const jsonData = xmlData ? data : JSON.stringify(data);
                req.write(jsonData);
            }

            req.end();
        });
    }

    /**
     * Тестирует health check
     */
    async testHealthCheck() {
        this.logger.info('🔍 Тестирование health check...');
        try {
            const response = await this.makeRequest('GET', '/health');
            if (response.statusCode === 200) {
                this.logger.info('✅ Health check успешен', response.data);
                return true;
            } else {
                this.logger.error('❌ Health check failed', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка health check:', {
                message: error.message,
                code: error.code,
                stack: error.stack
            });
            return false;
        }
    }

    /**
     * Тестирует обработку JSON сообщения
     */
    async testJsonMessage(messageType = '1', messageData = null) {
        this.logger.info(`🔍 Тестирование JSON сообщения типа: ${messageType}`);

        let testData = messageData;

        // Если данные не предоставлены, получаем правильную структуру
        if (!testData) {
            try {
                const generateResponse = await this.makeRequest('GET', `/api/v1/message/${messageType}/json`);
                if (generateResponse.statusCode === 200) {
                    testData = generateResponse.data;
                    // Обновляем некоторые поля для тестирования
                    testData.id = `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                    testData.amount = 99.99;
                    testData.dt = new Date().toISOString();
                } else {
                    throw new Error('Не удалось получить структуру сообщения');
                }
            } catch (error) {
                this.logger.error('❌ Ошибка получения структуры сообщения:', error.message);
                return false;
            }
        }

        try {
            const response = await this.makeRequest('POST', `/api/v1/message/${messageType}/json`, testData);

            if (response.statusCode === 200) {
                // Проверяем новую структуру ответа
                const expectedFields = ['messageType', 'factId', 'counters', 'processingTime'];
                const hasAllFields = expectedFields.every(field => response.data.hasOwnProperty(field));

                if (hasAllFields) {
                    this.logger.info('✅ JSON сообщение успешно обработано', {
                        messageType: response.data.messageType,
                        factId: response.data.factId,
                        countersCount: response.data.counters ? Object.keys(response.data.counters).length : 0,
                        processingTime: response.data.processingTime,
                        hasDebug: !!response.data.debug
                    });
                    return true;
                } else {
                    this.logger.error('❌ Неполная структура ответа JSON', {
                        received: Object.keys(response.data),
                        expected: expectedFields
                    });
                    return false;
                }
            } else {
                this.logger.error('❌ Ошибка обработки JSON сообщения', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка JSON сообщения:', error.message);
            return false;
        }
    }

    /**
     * Тестирует IRIS endpoint с XML ответом
     */
    async testIrisMessage(messageType = '1') {
        this.logger.info(`🔍 Тестирование IRIS сообщения типа: ${messageType}`);

        const testData = `
<IRIS Version="1" Message="ModelRequest" MessageTypeId="${messageType}" MessageId="3323123" custom="test">
<id>test_user_123</id>
<productId>R</productId>
<amount>99.99</amount>
<currency>USD</currency>
</IRIS>`;

        try {
            const response = await this.makeRequest('POST', `/api/v1/message/iris`, testData, true);

            if (response.statusCode === 200) {
                // Проверяем, что ответ содержит XML
                if (typeof response.data === 'string' && response.data.includes('<IRIS')) {
                    this.logger.info('✅ IRIS endpoint корректно возвращает XML ответ', {
                        statusCode: response.statusCode,
                        contentType: response.headers['content-type'],
                        responseLength: response.data.length,
                        containsFactId: response.data.includes('FactId'),
                        containsCounters: response.data.includes('Counters')
                    });
                    return true;
                } else {
                    this.logger.error('❌ IRIS endpoint не возвращает XML', {
                        responseType: typeof response.data,
                        responseData: response.data
                    });
                    return false;
                }
            } else {
                this.logger.error('❌ Неожиданный ответ IRIS endpoint', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка IRIS события:', error.message);
            return false;
        }
    }

    /**
     * Тестирует 404 endpoint
     */
    async testNotFound() {
        this.logger.info('🔍 Тестирование 404 endpoint...');

        try {
            const response = await this.makeRequest('GET', '/nonexistent/endpoint');

            if (response.statusCode === 404) {
                this.logger.info('✅ 404 endpoint работает корректно', response.data);
                return true;
            } else {
                this.logger.error('❌ Неожиданный ответ для 404', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка 404 теста:', error.message);
            return false;
        }
    }

    /**
     * Тестирует валидацию JSON
     */
    async testInvalidJson() {
        this.logger.info('🔍 Тестирование валидации JSON...');

        try {
            // Отправляем невалидный JSON с валидным messageType
            const response = await this.makeRequest('POST', '/api/v1/message/1/json', 'invalid json', true);

            if (response.statusCode === 400) {
                // Проверяем структуру ошибки
                const hasErrorFields = response.data.success === false &&
                    response.data.error &&
                    response.data.message;

                if (hasErrorFields) {
                    this.logger.info('✅ Валидация JSON работает корректно (ошибка парсинга обработана)', {
                        error: response.data.error,
                        message: response.data.message,
                        timestamp: response.data.timestamp
                    });
                    return true;
                } else {
                    this.logger.error('❌ Неполная структура ошибки JSON', response.data);
                    return false;
                }
            } else {
                this.logger.error('❌ Неожиданный ответ для невалидного JSON', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка валидации JSON:', error.message);
            return false;
        }
    }

    /**
     * Тестирует валидацию пустого messageType
     */
    async testEmptyMessageType() {
        this.logger.info('🔍 Тестирование валидации пустого messageType...');

        try {
            const response = await this.makeRequest('POST', '/api/v1/message//json', { test: 'data' });

            if (response.statusCode === 404) {
                this.logger.info('✅ Пустой messageType корректно обрабатывается как 404', response.data);
                return true;
            } else {
                this.logger.error('❌ Неожиданный ответ для пустого messageType', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования пустого messageType:', error.message);
            return false;
        }
    }

    /**
     * Тестирует валидацию массива вместо объекта в JSON
     */
    async testArrayInsteadOfObject() {
        this.logger.info('🔍 Тестирование валидации массива вместо объекта...');

        try {
            const response = await this.makeRequest('POST', '/api/v1/message/1/json', ['invalid', 'array']);

            if (response.statusCode === 400) {
                const hasErrorFields = response.data.success === false &&
                    response.data.error &&
                    response.data.message;

                if (hasErrorFields) {
                    this.logger.info('✅ Валидация массива работает корректно', {
                        error: response.data.error,
                        message: response.data.message
                    });
                    return true;
                } else {
                    this.logger.error('❌ Неполная структура ошибки для массива', response.data);
                    return false;
                }
            } else {
                this.logger.error('❌ Неожиданный ответ для массива', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования массива:', error.message);
            return false;
        }
    }

    /**
     * Тестирует генерацию сообщения по типу (GET)
     */
    async testGenerateMessage(messageType = '1') {
        this.logger.info(`🔍 Тестирование генерации сообщения типа: ${messageType}`);

        try {
            const response = await this.makeRequest('GET', `/api/v1/message/${messageType}/json`);

            if (response.statusCode === 200) {
                // Проверяем, что ответ содержит данные сообщения
                if (response.data && typeof response.data === 'object') {
                    this.logger.info('✅ Сообщение успешно сгенерировано', {
                        messageType: messageType,
                        hasMessage: !!response.data,
                        messageStructure: Object.keys(response.data),
                        messageFields: Object.keys(response.data).length
                    });
                    return true;
                } else {
                    this.logger.error('❌ Пустой или невалидный ответ генерации', response.data);
                    return false;
                }
            } else {
                this.logger.error('❌ Ошибка генерации сообщения', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка генерации сообщения:', error.message);
            return false;
        }
    }

    /**
     * Тестирует проверку обязательных полей для сообщения определенного типа
     */
    async testRequiredFieldsValidation(messageType = '1') {
        this.logger.info(`🔍 Тестирование проверки обязательных полей для сообщения типа: ${messageType}`);

        try {
            // Сначала получаем правильную структуру сообщения
            const generateResponse = await this.makeRequest('GET', `/api/v1/message/${messageType}/json`);

            if (generateResponse.statusCode !== 200) {
                this.logger.error('❌ Не удалось получить структуру сообщения', generateResponse);
                return false;
            }

            const correctMessage = generateResponse.data;
            this.logger.info('✅ Получена правильная структура сообщения', {
                messageType,
                fields: Object.keys(correctMessage),
                hasId: !!correctMessage.id
            });

            // Тестируем сообщение без ВСЕХ ключевых полей (id и id2)
            const messageWithoutKeys = { ...correctMessage };
            delete messageWithoutKeys.id;
            delete messageWithoutKeys.id2;

            const responseWithoutKeys = await this.makeRequest('POST', `/api/v1/message/${messageType}/json`, messageWithoutKeys);

            if (responseWithoutKeys.statusCode === 500) {
                const hasCorrectError = responseWithoutKeys.data &&
                    responseWithoutKeys.data.message &&
                    (responseWithoutKeys.data.message.includes('не найдено ни одного ключевого поля') ||
                     responseWithoutKeys.data.message.includes('не найдено ключевое поле'));

                if (hasCorrectError) {
                    this.logger.info('✅ Корректно обработано отсутствие всех ключевых полей', {
                        error: responseWithoutKeys.data.error,
                        message: responseWithoutKeys.data.message
                    });
                    return true;
                } else {
                    this.logger.error('❌ Неожиданная ошибка при отсутствии ключевых полей', responseWithoutKeys.data);
                    return false;
                }
            } else {
                this.logger.error('❌ Неожиданный статус код при отсутствии ключевых полей', responseWithoutKeys);
                return false;
            }

        } catch (error) {
            this.logger.error('❌ Ошибка тестирования обязательных полей:', error.message);
            return false;
        }
    }

    /**
     * Тестирует наличие новых полей в ответах API
     */
    async testNewResponseFields() {
        this.logger.info('🔍 Тестирование новых полей в ответах API...');

        // Получаем правильную структуру сообщения
        let testData;
        try {
            const generateResponse = await this.makeRequest('GET', '/api/v1/message/1/json');
            if (generateResponse.statusCode === 200) {
                testData = generateResponse.data;
                testData.id = `field_test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                testData.amount = 50.00;
                testData.dt = new Date().toISOString();
            } else {
                throw new Error('Не удалось получить структуру сообщения');
            }
        } catch (error) {
            this.logger.error('❌ Ошибка получения структуры сообщения:', error.message);
            return false;
        }

        try {
            const response = await this.makeRequest('POST', '/api/v1/message/1/json', testData);

            if (response.statusCode === 200) {
                const data = response.data;
                const requiredFields = ['messageType', 'factId', 'counters', 'processingTime'];
                const optionalFields = ['debug'];

                // Проверяем обязательные поля
                const missingRequired = requiredFields.filter(field => !data.hasOwnProperty(field));
                if (missingRequired.length > 0) {
                    this.logger.error('❌ Отсутствуют обязательные поля', { missing: missingRequired });
                    return false;
                }

                // Проверяем типы полей
                const typeChecks = [
                    { field: 'messageType', expected: 'string', actual: typeof data.messageType },
                    { field: 'factId', expected: 'string', actual: typeof data.factId },
                    { field: 'counters', expected: 'object', actual: typeof data.counters },
                    { field: 'processingTime', expected: 'object', actual: typeof data.processingTime }
                ];

                const typeErrors = typeChecks.filter(check => check.actual !== check.expected);
                if (typeErrors.length > 0) {
                    this.logger.error('❌ Неверные типы полей', typeErrors);
                    return false;
                }

                // Проверяем структуру processingTime
                if (data.processingTime && typeof data.processingTime === 'object') {
                    const hasTotal = data.processingTime.hasOwnProperty('total');
                    if (!hasTotal) {
                        this.logger.error('❌ processingTime не содержит поле total');
                        return false;
                    }
                }

                this.logger.info('✅ Все новые поля присутствуют и имеют правильные типы', {
                    messageType: data.messageType,
                    factId: data.factId,
                    countersKeys: Object.keys(data.counters || {}),
                    processingTimeKeys: Object.keys(data.processingTime || {}),
                    hasDebug: !!data.debug
                });
                return true;
            } else {
                this.logger.error('❌ Ошибка при тестировании полей ответа', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования полей ответа:', error.message);
            return false;
        }
    }

    /**
     * Тестирует генерацию сообщения несуществующего типа (должна вернуть 400)
     */
    async testGenerateInvalidMessage() {
        this.logger.info('🔍 Тестирование генерации сообщения несуществующего типа...');

        try {
            const response = await this.makeRequest('GET', '/api/v1/message/999/json');

            if (response.statusCode === 400) {
                this.logger.info('✅ Валидация несуществующего типа работает корректно', response.data);
                return true;
            } else {
                this.logger.error('❌ Неожиданный ответ для несуществующего типа', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования несуществующего типа:', error.message);
            return false;
        }
    }

    /**
     * Тестирует генерацию IRIS сообщения
     */
    async testGenerateIrisMessage(messageType = '1') {
        this.logger.info(`🔍 Тестирование генерации IRIS сообщения типа: ${messageType}`);

        try {
            const response = await this.makeRequest('GET', `/api/v1/message/${messageType}/iris`);

            if (response.statusCode === 200) {
                // Проверяем, что ответ содержит XML
                if (typeof response.data === 'string' && response.data.includes('<IRIS')) {
                    this.logger.info('✅ IRIS сообщение успешно сгенерировано', {
                        messageType: messageType,
                        responseLength: response.data.length,
                        containsMessageId: response.data.includes('MessageId'),
                        containsMessageTypeId: response.data.includes('MessageTypeId')
                    });
                    return true;
                } else {
                    this.logger.error('❌ IRIS генерация не возвращает XML', {
                        responseType: typeof response.data,
                        responseData: response.data
                    });
                    return false;
                }
            } else {
                this.logger.error('❌ Ошибка генерации IRIS сообщения', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка генерации IRIS сообщения:', error.message);
            return false;
        }
    }

    /**
     * Тестирует обработку невалидного XML в IRIS endpoint
     */
    async testInvalidIrisXml() {
        this.logger.info('🔍 Тестирование обработки невалидного XML в IRIS endpoint...');

        try {
            const response = await this.makeRequest('POST', '/api/v1/message/iris', 'invalid xml', true);

            if (response.statusCode === 400) {
                this.logger.info('✅ Невалидный XML корректно обработан как ошибка', response.data);
                return true;
            } else {
                this.logger.error('❌ Неожиданный ответ для невалидного XML', response);
                return false;
            }
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования невалидного XML:', error.message);
            return false;
        }
    }

    /**
     * Тестирует сохранение отладочной информации в лог
     */
    async testDebugLogging() {
        this.logger.info('🔍 Тестирование сохранения отладочной информации в лог...');
        let mongoProvider = null;

        try {
            // Создаем экземпляр MongoProvider с правильной конфигурацией
            mongoProvider = new MongoProvider(
                config.database.connectionString,
                'debugLoggingTestDB',
                config.database.options,
                this.mongoCounters,
                config.facts.includeFactDataToIndex,
                config.facts.lookupFacts,
                config.facts.indexBulkUpdate
            );
            await mongoProvider.connect();

            // Очищаем коллекцию логов перед тестом
            await mongoProvider.clearLogCollection();

            // Устанавливаем переменную окружения для частоты сохранения
            process.env.LOG_SAVE_FREQUENCY = '3'; // Сохраняем каждые 3 запроса для тестирования

            // Импортируем функцию saveDebugInfoIfNeeded из routes.js
            // Для тестирования создадим упрощенную версию
            let requestCounter = 0;
            let maxProcessingTime = null;
            let maxMetrics = null;
            let maxDebugInfo = null;
            let maxMessage = null;
            let maxFact = null;

            const saveDebugInfoIfNeeded = async (factController, message, fact, processingTime, metrics, debugInfo) => {
                try {
                    const logSaveFrequency = parseInt(process.env.LOG_SAVE_FREQUENCY || '100');

                    requestCounter++;

                    if (!maxProcessingTime || (processingTime.total > maxProcessingTime.total)) {
                        maxProcessingTime = processingTime;
                        maxMetrics = metrics;
                        maxDebugInfo = debugInfo;
                        maxMessage = message;
                        maxFact = fact;
                    }

                    if (requestCounter >= logSaveFrequency) {
                        if (maxDebugInfo && mongoProvider) {
                            const processId = process.pid;
                            await mongoProvider.saveLog(processId, maxMessage, maxFact, maxProcessingTime, maxMetrics, maxDebugInfo);

                            this.logger.info(`Отладочная информация сохранена в лог`);
                        }

                        requestCounter = 0;
                        maxProcessingTime = null;
                        maxMetrics = null;
                        maxDebugInfo = null;
                        maxMessage = null;
                        maxFact = null;
                    }
                } catch (error) {
                    this.logger.error('Ошибка при сохранении отладочной информации в лог:', {
                        error: error.message,
                        stack: error.stack
                    });
                }
            };

            // Тестовые данные - максимальное время должно быть в первых 3 запросах
            const testMessages = [
                { messageType: 1, message: { t: 1, d: { id: 'test-message-id1', dt: '2025-01-01', f1: 'test-field-1' } }, fact: { _id: 'test-fact-id1', t: 1, c: new Date(), d: { amount: 100, dt: '2025-01-01' } }, processingTime: { total: 100 }, metrics: { test: 'data1' }, debugInfo: { test: 'data1' } },
                { messageType: 2, message: { t: 2, d: { id: 'test-message-id2', dt: '2025-01-01', f1: 'test-field-1' } }, fact: { _id: 'test-fact-id2', t: 2, c: new Date(), d: { amount: 200, dt: '2025-01-01' } }, processingTime: { total: 300 }, metrics: { test: 'data2' }, debugInfo: { test: 'data2' } }, // Максимальное время
                { messageType: 3, message: { t: 3, d: { id: 'test-message-id3', dt: '2025-01-01', f1: 'test-field-1' } }, fact: { _id: 'test-fact-id3', t: 3, c: new Date(), d: { amount: 300, dt: '2025-01-01' } }, processingTime: { total: 150 }, metrics: { test: 'data3' }, debugInfo: { test: 'data3' } },
                { messageType: 4, message: { t: 4, d: { id: 'test-message-id4', dt: '2025-01-01', f1: 'test-field-1' } }, fact: { _id: 'test-fact-id4', t: 4, c: new Date(), d: { amount: 400, dt: '2025-01-01' } }, processingTime: { total: 200 }, metrics: { test: 'data4' }, debugInfo: { test: 'data4' } },
                { messageType: 5, message: { t: 5, d: { id: 'test-message-id5', dt: '2025-01-01', f1: 'test-field-1' } }, fact: { _id: 'test-fact-id5', t: 5, c: new Date(), d: { amount: 500, dt: '2025-01-01' } }, processingTime: { total: 50 }, metrics: { test: 'data5' }, debugInfo: { test: 'data5' } }
            ];

            // Симулируем обработку запросов
            for (let i = 0; i < testMessages.length; i++) {
                const msg = testMessages[i];

                await saveDebugInfoIfNeeded(mongoProvider, msg.message, msg.fact, msg.processingTime, msg.metrics, msg.debugInfo);
            }

            // Проверяем, что в логе есть записи
            const logCount = await mongoProvider.countLogCollection();
            if (logCount === 0) {
                throw new Error('В коллекции логов нет записей');
            }

            // Получаем последнюю запись из лога
            const logCollection = mongoProvider._counterDb.collection(mongoProvider.LOG_COLLECTION_NAME);
            const lastLog = await logCollection.findOne({}, { sort: { c: -1 } });
            this.logger.info('Последняя запись в логе:', lastLog);

            if (!lastLog) {
                throw new Error('Не удалось получить последнюю запись из лога');
            }

            // Проверяем структуру записи
            if (!lastLog._id) {
                throw new Error('Отсутствует поле _id в записи лога');
            }

            if (!lastLog.c || !(lastLog.c instanceof Date)) {
                throw new Error('Отсутствует или некорректно поле c (дата создания) в записи лога');
            }

            if (!lastLog.p || typeof lastLog.p !== 'string') {
                throw new Error('Отсутствует или некорректно поле p (processId) в записи лога');
            }

            if (!lastLog.t || typeof lastLog.t !== 'object') {
                throw new Error('Отсутствует или некорректно поле m (metrics) в записи лога');
            }

            if (!lastLog.m || typeof lastLog.m !== 'object') {
                throw new Error('Отсутствует или некорректно поле m (metrics) в записи лога');
            }

            if (!lastLog.di || typeof lastLog.di !== 'object') {
                throw new Error('Отсутствует или некорректно поле di (debugInfo) в записи лога');
            }

            // Проверяем, что сохранилась информация о максимальном времени обработки
            if (lastLog.t.total !== 300) {
                throw new Error(`Некорректное максимальное время обработки: ожидалось 300, получено ${lastLog.t.total}`);
            }

            if (lastLog.m.test !== 'data2') {
                throw new Error(`Некорректное значение metrics: ожидалось data2, получено ${lastLog.m.test}`);
            }

            if (lastLog.di.test !== 'data2') {
                throw new Error(`Некорректное количество запросов: ожидалось 3, получено ${lastLog.di.test}`);
            }

            this.logger.info('✅ Тест сохранения отладочной информации в лог успешен');
            return true;
        } catch (error) {
            this.logger.error('❌ Ошибка тестирования отладочной информации:', error.message);
            return false;
        } finally {
            // Закрываем соединение с MongoDB
            try {
                if (mongoProvider) {
                    await mongoProvider.disconnect();
                }
            } catch (disconnectError) {
                this.logger.error('Ошибка при закрытии соединения с MongoDB:', disconnectError.message);
            }
        }
    }

    /**
     * Запускает все тесты
     */
    async runAllTests() {
        this.logger.info('🚀 Запуск всех тестов API...');

        const tests = [
            { name: 'Health Check', fn: () => this.testHealthCheck() },
            { name: 'JSON Message', fn: () => this.testJsonMessage() },
            { name: 'IRIS Message', fn: () => this.testIrisMessage() },
            { name: '404 Not Found', fn: () => this.testNotFound() },
            { name: 'Invalid JSON', fn: () => this.testInvalidJson() },
            { name: 'Empty MessageType', fn: () => this.testEmptyMessageType() },
            { name: 'Array Instead of Object', fn: () => this.testArrayInsteadOfObject() },
            { name: 'Required Fields Validation', fn: () => this.testRequiredFieldsValidation() },
            { name: 'New Response Fields', fn: () => this.testNewResponseFields() },
            { name: 'Generate Message', fn: () => this.testGenerateMessage() },
            { name: 'Generate Invalid Message', fn: () => this.testGenerateInvalidMessage() },
            { name: 'Generate IRIS Message', fn: () => this.testGenerateIrisMessage() },
            { name: 'Invalid IRIS XML', fn: () => this.testInvalidIrisXml() },
            { name: 'Debug Logging', fn: () => this.testDebugLogging() }
        ];

        const results = [];

        for (const test of tests) {
            try {
                const result = await test.fn();
                results.push({ name: test.name, passed: result });
            } catch (error) {
                this.logger.error(`❌ Ошибка в тесте ${test.name}:`, error.message);
                results.push({ name: test.name, passed: false, error: error.message });
            }
        }

        // Выводим результаты
        this.logger.info('\n📊 Результаты тестирования:');
        results.forEach(result => {
            const status = result.passed ? '✅' : '❌';
            this.logger.info(`${status} ${result.name}${result.error ? ` - ${result.error}` : ''}`);
        });

        const passedCount = results.filter(r => r.passed).length;
        const totalCount = results.length;

        this.logger.info(`\n📈 Итого: ${passedCount}/${totalCount} тестов прошли успешно`);

        return results;
    }

    /**
     * Тестирует производительность
     */
    async performanceTest(requests = 100, concurrency = 10) {
        this.logger.info(`🚀 Запуск теста производительности: ${requests} запросов, ${concurrency} параллельных`);

        // Сначала получаем правильную структуру сообщения
        let testData;
        try {
            const generateResponse = await this.makeRequest('GET', '/api/v1/message/1/json');
            if (generateResponse.statusCode === 200) {
                testData = generateResponse.data;
                // Обновляем некоторые поля для тестирования
                testData.id = `perf_test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                testData.amount = 100.00;
                testData.dt = new Date().toISOString();
                this.logger.info('✅ Используется правильная структура сообщения для теста производительности');
            } else {
                throw new Error('Не удалось получить структуру сообщения');
            }
        } catch (error) {
            this.logger.error('❌ Ошибка получения структуры сообщения для теста производительности:', error.message);
            return;
        }

        const startTime = Date.now();
        const results = [];

        // Выполняем запросы батчами
        for (let i = 0; i < requests; i += concurrency) {
            const batchSize = Math.min(concurrency, requests - i);
            const batchPromises = [];

            for (let j = 0; j < batchSize; j++) {
                // Создаем уникальные данные для каждого запроса
                const uniqueTestData = { ...testData };
                uniqueTestData.id = `perf_test_${Date.now()}_${i}_${j}_${Math.random().toString(36).substr(2, 9)}`;
                uniqueTestData.amount = Math.floor(Math.random() * 1000) + 1;

                batchPromises.push(this.makeRequest('POST', '/api/v1/message/1/json', uniqueTestData));
            }

            const batchResults = await Promise.allSettled(batchPromises);

            batchResults.forEach(result => {
                if (result.status === 'fulfilled') {
                    const response = result.value;
                    this.logger.debug(`Performance test response: ${response.statusCode}`, {
                        statusCode: response.statusCode,
                        hasData: !!response.data,
                        dataKeys: response.data ? Object.keys(response.data) : 'no data'
                    });
                    const stats = {
                        statusCode: response.statusCode,
                        hasCounters: response.data && !!response.data.counters,
                        hasProcessingTime: response.data && !!response.data.processingTime,
                        hasDebug: response.data && !!response.data.debug,
                        countersCount: response.data && response.data.counters ? Object.keys(response.data.counters).length : 0,
                        processingTimeMs: response.data && response.data.processingTime ? response.data.processingTime.total : 0
                    };
                    results.push(stats);
                } else {
                    this.logger.error(`Performance test request failed: ${result.reason.message}`);
                }
            });
        }

        const endTime = Date.now();
        const totalTime = endTime - startTime;
        const requestsPerSecond = Math.round(requests / (totalTime / 1000));

        // Анализируем результаты
        const successfulRequests = results.filter(r => r.statusCode === 200);
        const avgProcessingTime = successfulRequests.length > 0
            ? Math.round(successfulRequests.reduce((sum, r) => sum + (r.processingTimeMs || 0), 0) / successfulRequests.length)
            : 0;
        const avgCountersCount = successfulRequests.length > 0
            ? Math.round(successfulRequests.reduce((sum, r) => sum + (r.countersCount || 0), 0) / successfulRequests.length)
            : 0;

        this.logger.info(`📊 Результаты производительности:`);
        this.logger.info(`   Время выполнения: ${totalTime}ms`);
        this.logger.info(`   Запросов в секунду: ${requestsPerSecond}`);
        this.logger.info(`   Среднее время на запрос: ${Math.round(totalTime / requests)}ms`);
        this.logger.info(`   Успешных запросов: ${successfulRequests.length}/${requests}`);
        this.logger.info(`   Среднее время обработки сервером: ${avgProcessingTime}ms`);
        this.logger.info(`   Среднее количество счетчиков: ${avgCountersCount}`);
        this.logger.info(`   Запросов с debug: ${successfulRequests.filter(r => r.hasDebug).length}`);
    }

    /**
     * Тест для выявления проблемы с идентификаторами запросов при параллельных запросах
     * Отправляет множество параллельных запросов с одинаковыми индексами, чтобы спровоцировать
     * конфликты ID в ProcessPoolManager.pendingQueries
     * 
     * @param {number} requests - Количество запросов (рекомендуется >= 50 для надежности)
     * @param {number} concurrency - Параллельность (рекомендуется >= 10)
     * @param {string} messageType - Тип сообщения (по умолчанию '1')
     * @returns {Promise<Object>} Результаты теста
     */
    async testQueryIdCollisions(requests = 100, concurrency = 20, messageType = '1') {
        this.logger.info(`🔍 Тест проверки конфликтов идентификаторов запросов:`);
        this.logger.info(`   Запросов: ${requests}, Параллельность: ${concurrency}, Тип сообщения: ${messageType}`);

        // Получаем структуру сообщения
        let baseTestData;
        try {
            const generateResponse = await this.makeRequest('GET', `/api/v1/message/${messageType}/json`);
            if (generateResponse.statusCode !== 200) {
                throw new Error('Не удалось получить структуру сообщения');
            }
            baseTestData = generateResponse.data;
        } catch (error) {
            this.logger.error(`❌ Ошибка получения структуры сообщения: ${error.message}`);
            return {
                success: false,
                error: error.message,
                metrics: {}
            };
        }

        const startTime = Date.now();
        const results = [];
        const errors = [];
        const factIds = new Set(); // Для проверки уникальности результатов
        const duplicateFactIds = []; // Для отслеживания дубликатов

        // Создаем массив всех промисов для максимальной параллельности
        const allPromises = [];
        
        for (let i = 0; i < requests; i++) {
            // Используем одинаковые поля для максимальной вероятности одинаковых индексов
            // Это создаст максимальное давление на систему идентификаторов
            const testData = { ...baseTestData };
            testData.id = `collision_test_${i}_${Date.now()}`;
            testData.amount = 100.00; // Одинаковая сумма для одинаковых индексов
            testData.dt = new Date().toISOString();
            
            // Небольшая вариация для разных запросов, но с одинаковыми индексами
            if (baseTestData.f1) testData.f1 = 'test_collision';
            if (baseTestData.f2) testData.f2 = 'collision_value';
            
            const promise = this.makeRequest('POST', `/api/v1/message/${messageType}/json`, testData)
                .then(response => {
                    // Проверяем метрики на наличие таймаутов в деталях
                    const metrics = response.data?.metrics || {};
                    const countersMetrics = metrics.countersMetrics || {};
                    const details = metrics.details || {};
                    const debug = response.data?.debug || {};
                    
                    // Собираем информацию о таймаутах из метрик
                    const timeouts = [];
                    
                    // Проверяем countersMetrics на ошибки таймаутов
                    // Ошибки могут быть в разных местах структуры, проверяем все возможные варианты
                    Object.keys(countersMetrics).forEach(indexName => {
                        const indexMetrics = countersMetrics[indexName];
                        // Проверяем error в метриках индекса (если он там есть)
                        if (indexMetrics && indexMetrics.error) {
                            const errorMsg = typeof indexMetrics.error === 'string' ? indexMetrics.error : 
                                           (indexMetrics.error?.message || String(indexMetrics.error));
                            if (errorMsg && (errorMsg.includes('timeout') || errorMsg.includes('Timeout'))) {
                                timeouts.push({ index: indexName, error: errorMsg });
                            }
                        }
                        // Проверяем error в metrics внутри indexMetrics
                        if (indexMetrics && indexMetrics.metrics && indexMetrics.metrics.error) {
                            const errorMsg = typeof indexMetrics.metrics.error === 'string' ? indexMetrics.metrics.error : 
                                           (indexMetrics.metrics.error?.message || String(indexMetrics.metrics.error));
                            if (errorMsg && (errorMsg.includes('timeout') || errorMsg.includes('Timeout'))) {
                                timeouts.push({ index: `${indexName}`, error: errorMsg });
                            }
                        }
                    });
                    
                    // Проверяем details на ошибки счетчиков (используется в getRelevantFactCountersFromFact)
                    Object.keys(details).forEach(indexName => {
                        const indexDetails = details[indexName];
                        if (indexDetails && indexDetails.countersErrors) {
                            Object.keys(indexDetails.countersErrors).forEach(groupNumber => {
                                const error = indexDetails.countersErrors[groupNumber];
                                if (error) {
                                    const errorMsg = typeof error === 'string' ? error : (error?.message || String(error));
                                    if (errorMsg && (errorMsg.includes('timeout') || errorMsg.includes('Timeout'))) {
                                        timeouts.push({ index: `${indexName}#${groupNumber}`, error: errorMsg });
                                    }
                                }
                            });
                        }
                    });
                    
                    const result = {
                        requestId: i,
                        statusCode: response.statusCode,
                        factId: response.data?.factId || null,
                        hasError: !!response.data?.error,
                        error: response.data?.error || null,
                        processingTime: response.data?.processingTime?.total || 0,
                        countersCount: response.data?.counters ? Object.keys(response.data.counters).length : 0,
                        timeouts: timeouts,
                        hasTimeouts: timeouts.length > 0
                    };
                    
                    // Проверяем на дубликаты factId
                    if (result.factId) {
                        if (factIds.has(result.factId)) {
                            duplicateFactIds.push({
                                requestId: i,
                                factId: result.factId
                            });
                        } else {
                            factIds.add(result.factId);
                        }
                    }
                    
                    return result;
                })
                .catch(error => {
                    return {
                        requestId: i,
                        statusCode: 0,
                        factId: null,
                        hasError: true,
                        error: error.message,
                        processingTime: 0,
                        countersCount: 0
                    };
                });
            
            allPromises.push(promise);
        }

        // Выполняем все запросы с контролем параллельности
        const batchSize = concurrency;
        for (let i = 0; i < allPromises.length; i += batchSize) {
            const batch = allPromises.slice(i, Math.min(i + batchSize, allPromises.length));
            const batchResults = await Promise.all(batch);
            results.push(...batchResults);
        }

        const endTime = Date.now();
        const totalTime = endTime - startTime;

        // Анализ результатов
        const successfulRequests = results.filter(r => r.statusCode === 200 && !r.hasError);
        const failedRequests = results.filter(r => r.statusCode !== 200 || r.hasError);
        
        // Собираем таймауты из всех запросов (как из ошибок HTTP, так и из метрик)
        const timeoutErrors = [];
        results.forEach(r => {
            // Таймауты из HTTP ошибок
            if (r.error && typeof r.error === 'string' && (r.error.includes('timeout') || r.error.includes('Timeout'))) {
                timeoutErrors.push({ requestId: r.requestId, source: 'http_error', error: r.error });
            }
            // Таймауты из метрик (внутренние запросы к MongoDB)
            if (r.timeouts && r.timeouts.length > 0) {
                r.timeouts.forEach(timeout => {
                    timeoutErrors.push({ requestId: r.requestId, source: 'query_timeout', index: timeout.index, error: timeout.error });
                });
            }
        });
        
        const requestsWithTimeouts = results.filter(r => r.hasTimeouts || (r.error && typeof r.error === 'string' && (r.error.includes('timeout') || r.error.includes('Timeout'))));
        const uniqueFactIds = factIds.size;
        const totalFactIds = results.filter(r => r.factId).length;

        // Проверка на проблему с идентификаторами
        const hasDuplicateFactIds = duplicateFactIds.length > 0;
        const allRequestsProcessed = results.length === requests;
        // Таймауты - это отдельная метрика производительности, они не влияют на проверку конфликтов идентификаторов
        // Таймауты могут быть частыми под нагрузкой - это нормально, главное что нет конфликтов идентификаторов
        const uniqueFactIdsMatch = uniqueFactIds === totalFactIds || totalFactIds === 0;

        const avgProcessingTime = successfulRequests.length > 0
            ? Math.round(successfulRequests.reduce((sum, r) => sum + (r.processingTime || 0), 0) / successfulRequests.length)
            : 0;

        const testResult = {
            // Успешным считается тест, если все запросы обработаны и нет дубликатов идентификаторов
            // Таймауты - это отдельная метрика производительности, не влияющая на успешность проверки конфликтов ID
            success: allRequestsProcessed && uniqueFactIdsMatch && !hasDuplicateFactIds,
            metrics: {
                totalRequests: requests,
                processedRequests: results.length,
                successfulRequests: successfulRequests.length,
                failedRequests: failedRequests.length,
                timeoutErrors: timeoutErrors.length,
                requestsWithTimeouts: requestsWithTimeouts.length,
                timeoutRate: requests > 0 ? `${Math.round((requestsWithTimeouts.length / requests) * 100)}%` : '0%',
                totalTimeoutQueries: timeoutErrors.length, // Общее количество таймаутированных запросов к MongoDB
                totalTime: totalTime,
                requestsPerSecond: Math.round((results.length / totalTime) * 1000),
                avgProcessingTime: avgProcessingTime,
                uniqueFactIds: uniqueFactIds,
                totalFactIds: totalFactIds,
                duplicateFactIds: duplicateFactIds.length,
                hasDuplicateFactIds: hasDuplicateFactIds
            },
            errors: failedRequests.map(r => ({
                requestId: r.requestId,
                error: r.error
            })).slice(0, 10), // Ограничиваем вывод ошибок
            duplicateFactIds: duplicateFactIds.slice(0, 10) // Ограничиваем вывод дубликатов
        };

        // Вывод результатов
        this.logger.info(`\n📊 Результаты теста конфликтов идентификаторов:`);
        this.logger.info(`   Обработано запросов: ${testResult.metrics.processedRequests}/${testResult.metrics.totalRequests}`);
        this.logger.info(`   Успешных: ${testResult.metrics.successfulRequests}`);
        this.logger.info(`   Ошибок: ${testResult.metrics.failedRequests}`);
        if (testResult.metrics.timeoutErrors > 0 || testResult.metrics.requestsWithTimeouts > 0) {
            this.logger.info(`   Запросов с таймаутами: ${testResult.metrics.requestsWithTimeouts}/${requests} (${testResult.metrics.timeoutRate})`);
            this.logger.info(`   Всего таймаутов запросов к MongoDB: ${testResult.metrics.totalTimeoutQueries} - допустимы под нагрузкой`);
        }
        this.logger.info(`   Время выполнения: ${testResult.metrics.totalTime}ms`);
        this.logger.info(`   Запросов в секунду: ${testResult.metrics.requestsPerSecond}`);
        this.logger.info(`   Среднее время обработки: ${testResult.metrics.avgProcessingTime}ms`);
        this.logger.info(`   Уникальных factId: ${testResult.metrics.uniqueFactIds}/${testResult.metrics.totalFactIds}`);
        
        if (testResult.success) {
            this.logger.info(`\n✅ Тест пройден успешно: конфликтов идентификаторов не обнаружено`);
            // Таймауты - это информационная метрика, не влияющая на результат проверки конфликтов ID
            if (testResult.metrics.requestsWithTimeouts > 0 || testResult.metrics.timeoutErrors > 0) {
                this.logger.info(`   ℹ️  Метрика производительности: Запросов с таймаутами: ${testResult.metrics.requestsWithTimeouts}/${requests} (${testResult.metrics.timeoutRate})`);
                this.logger.info(`   ℹ️  Всего таймаутов запросов к MongoDB: ${testResult.metrics.totalTimeoutQueries} - это нормально под нагрузкой`);
                this.logger.info(`   ℹ️  Предупреждения "Получен результат для неизвестного запроса" для таймаутированных запросов теперь игнорируются`);
            }
        } else {
            this.logger.error(`\n❌ Тест провален (конфликты идентификаторов):`);
            if (!allRequestsProcessed) {
                this.logger.error(`   - Не все запросы обработаны`);
            }
            if (hasDuplicateFactIds) {
                this.logger.error(`   - Обнаружены дубликаты factId (возможная проблема с идентификаторами запросов)`);
            }
            if (!uniqueFactIdsMatch) {
                this.logger.error(`   - Несоответствие количества уникальных и общих factId`);
            }
            
            // Выводим метрику таймаутов отдельно, как информацию
            if (testResult.metrics.requestsWithTimeouts > 0 || testResult.metrics.timeoutErrors > 0) {
                this.logger.info(`   ℹ️  Метрика производительности: Запросов с таймаутами: ${testResult.metrics.requestsWithTimeouts}/${requests} (${testResult.metrics.timeoutRate})`);
                this.logger.info(`   ℹ️  Всего таймаутов запросов к MongoDB: ${testResult.metrics.totalTimeoutQueries}`);
            }
        }
        
        if (testResult.metrics.hasDuplicateFactIds) {
            this.logger.error(`   ⚠️  ОБНАРУЖЕНЫ ДУБЛИКАТЫ factId: ${testResult.metrics.duplicateFactIds}`);
            testResult.duplicateFactIds.forEach(dup => {
                this.logger.error(`      Request ${dup.requestId}: ${dup.factId}`);
            });
        }
        
        if (testResult.metrics.failedRequests > 0) {
            this.logger.warn(`   ⚠️  Ошибки в запросах:`);
            testResult.errors.slice(0, 5).forEach(err => {
                this.logger.warn(`      Request ${err.requestId}: ${err.error}`);
            });
        }

        return testResult;
    }

}

// Запуск тестов если файл выполняется напрямую
if (require.main === module) {
    const tester = new ApiTester();

    async function runTests() {
        try {
            // Проверяем аргументы командной строки
            const args = process.argv.slice(2);
            
            // Проверяем, не запущен ли специальный тест конфликтов ID
            if (args.includes('--test-query-id-collisions') || args.includes('--collision-test')) {
                const requests = parseInt(args.find(a => a.startsWith('--requests='))?.split('=')[1]) || 100;
                const concurrency = parseInt(args.find(a => a.startsWith('--concurrency='))?.split('=')[1]) || 20;
                const messageType = args.find(a => a.startsWith('--message-type='))?.split('=')[1] || '1';
                
                tester.logger.info('🔍 Запуск теста конфликтов идентификаторов запросов...');
                await tester.testQueryIdCollisions(requests, concurrency, messageType);
                return;
            }

            // Запускаем все тесты
            await tester.runAllTests();

            // Запускаем тест производительности
            tester.logger.info('\n🚀 Запуск теста производительности...');
            await tester.performanceTest(50, 5);

        } catch (error) {
            tester.logger.error('❌ Ошибка выполнения тестов:', error);
        }
    }

    runTests();
}

module.exports = ApiTester;
