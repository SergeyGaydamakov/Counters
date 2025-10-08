const http = require('http');
const logger = require('../utils/logger');

/**
 * Тестирование Web API
 */
class ApiTester {
    constructor(baseUrl = 'http://localhost:3000') {
        this.baseUrl = baseUrl;
        this.logger = logger.fromEnv('LOG_LEVEL', 'INFO');
    }

    /**
     * Выполняет HTTP запрос
     */
    async makeRequest(method, path, data = null, rawData = false) {
        return new Promise((resolve, reject) => {
            const url = new URL(path, this.baseUrl);
            const options = {
                hostname: url.hostname,
                port: url.port,
                path: url.pathname + url.search,
                method: method,
                headers: {
                    'Content-Type': 'application/json'
                }
            };

            if (data) {
                const jsonData = rawData ? data : JSON.stringify(data);
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
                reject(error);
            });

            if (data) {
                const jsonData = rawData ? data : JSON.stringify(data);
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
            this.logger.error('❌ Ошибка health check:', error.message);
            return false;
        }
    }

    /**
     * Тестирует обработку JSON сообщения
     */
    async testJsonMessage(messageType = '1', messageData = null) {
        this.logger.info(`🔍 Тестирование JSON сообщения типа: ${messageType}`);
        
        const testData = messageData || {
            userId: 'test_user_123',
            productId: 'test_product_456',
            amount: 99.99,
            currency: 'USD',
            timestamp: new Date().toISOString(),
            metadata: {
                source: 'api_test',
                testRun: true
            }
        };

        try {
            const response = await this.makeRequest('POST', `/api/v1/message/${messageType}/json`, testData);
            
            if (response.statusCode === 200) {
                this.logger.info('✅ JSON сообщение успешно обработано', {
                    factId: response.data.factId,
                    processingTime: response.data.processingTime,
                    worker: response.data.worker
                });
                return true;
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
     * Тестирует IRIS endpoint (заглушка)
     */
    async testIrisMessage(messageType = 'test_iris') {
        this.logger.info(`🔍 Тестирование IRIS сообщения типа: ${messageType}`);
        
        const testData = {
            irisData: 'test_iris_string_data',
            additionalInfo: 'test_metadata'
        };

        try {
            const response = await this.makeRequest('POST', `/api/v1/message/${messageType}/iris`, testData);
            
            if (response.statusCode === 501) {
                this.logger.info('✅ IRIS endpoint корректно возвращает 501 (не реализовано)', response.data);
                return true;
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
                this.logger.info('✅ Валидация JSON работает корректно (ошибка парсинга обработана)', response.data);
                return true;
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
     * Запускает все тесты
     */
    async runAllTests() {
        this.logger.info('🚀 Запуск всех тестов API...');
        
        const tests = [
            { name: 'Health Check', fn: () => this.testHealthCheck() },
            { name: 'JSON Message', fn: () => this.testJsonMessage() },
            { name: 'IRIS Message', fn: () => this.testIrisMessage() },
            { name: '404 Not Found', fn: () => this.testNotFound() },
            { name: 'Invalid JSON', fn: () => this.testInvalidJson() }
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
        
        const testData = {
            userId: 'perf_test_user',
            productId: 'perf_test_product',
            amount: 100.00,
            currency: 'USD',
            timestamp: new Date().toISOString()
        };

        const startTime = Date.now();
        const promises = [];
        
        for (let i = 0; i < requests; i++) {
            const promise = this.makeRequest('POST', '/api/v1/message/1/json', testData)
                .catch(error => ({ error: error.message }));
            promises.push(promise);
            
            // Ограничиваем параллельность
            if (promises.length >= concurrency) {
                await Promise.all(promises);
                promises.length = 0;
            }
        }
        
        // Обрабатываем оставшиеся запросы
        if (promises.length > 0) {
            await Promise.all(promises);
        }
        
        const endTime = Date.now();
        const totalTime = endTime - startTime;
        const requestsPerSecond = Math.round(requests / (totalTime / 1000));
        
        this.logger.info(`📊 Результаты производительности:`);
        this.logger.info(`   Время выполнения: ${totalTime}ms`);
        this.logger.info(`   Запросов в секунду: ${requestsPerSecond}`);
        this.logger.info(`   Среднее время на запрос: ${Math.round(totalTime / requests)}ms`);
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
