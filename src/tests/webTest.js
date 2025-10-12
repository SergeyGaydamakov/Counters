const http = require('http');
const logger = require('../utils/logger');

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
            
            // Тестируем сообщение без обязательного поля id
            const messageWithoutId = { ...correctMessage };
            delete messageWithoutId.id;
            
            const responseWithoutId = await this.makeRequest('POST', `/api/v1/message/${messageType}/json`, messageWithoutId);
            
            if (responseWithoutId.statusCode === 500) {
                const hasCorrectError = responseWithoutId.data && 
                                      responseWithoutId.data.message && 
                                      responseWithoutId.data.message.includes('не найдено ключевое поле: id');
                
                if (hasCorrectError) {
                    this.logger.info('✅ Корректно обработано отсутствие обязательного поля id', {
                        error: responseWithoutId.data.error,
                        message: responseWithoutId.data.message
                    });
                    return true;
                } else {
                    this.logger.error('❌ Неожиданная ошибка при отсутствии поля id', responseWithoutId.data);
                    return false;
                }
            } else {
                this.logger.error('❌ Неожиданный статус код при отсутствии поля id', responseWithoutId);
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
            { name: 'Generate Invalid Message', fn: () => this.testGenerateInvalidMessage() }
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
}

// Запуск тестов если файл выполняется напрямую
if (require.main === module) {
    const tester = new ApiTester();
    
    async function runTests() {
        try {
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
