/**
 * Примеры использования Web API
 */

const examples = {
    // Пример запроса для обработки JSON события
    jsonMessage: {
        url: 'POST /api/v1/message/purchase/json',
        headers: {
            'Content-Type': 'application/json'
        },
        body: {
            userId: 'user123',
            productId: 'prod456',
            amount: 99.99,
            currency: 'USD',
            timestamp: '2024-01-15T10:30:00Z',
            metadata: {
                source: 'web',
                campaign: 'summer_sale'
            }
        },
        expectedResponse: {
            success: true,
            messageType: 'purchase',
            factId: 'generated-fact-id',
            processingTime: {
                total: 150,
                counters: 50,
                saveFact: 30,
                saveIndex: 70
            },
            counters: {
                // Результат обработки счетчиков
            },
            timestamp: '2024-01-15T10:30:00.123Z',
            worker: 12345
        }
    },

    // Пример запроса для IRIS события (заглушка)
    irisMessage: {
        url: 'POST /api/v1/message/iris_message/iris',
        headers: {
            'Content-Type': 'application/json'
        },
        body: {
            irisData: 'some_iris_string_data',
            additionalInfo: 'metadata'
        },
        expectedResponse: {
            success: false,
            error: 'IRIS обработка не реализована',
            message: 'Данный endpoint находится в разработке',
            messageType: 'iris_message',
            timestamp: '2024-01-15T10:30:00.123Z',
            worker: 12345
        }
    },

    // Пример health check запроса
    healthCheck: {
        url: 'GET /health',
        expectedResponse: {
            status: 'OK',
            worker: 12345,
            uptime: 3600.5,
            memory: {
                rss: '45MB',
                heapTotal: '20MB',
                heapUsed: '15MB',
                external: '5MB'
            },
            timestamp: '2024-01-15T10:30:00.123Z'
        }
    },

    // Примеры различных типов событий
    messageTypes: [
        {
            type: 'purchase',
            description: 'Покупка товара',
            example: {
                userId: 'user123',
                productId: 'prod456',
                amount: 99.99,
                currency: 'USD'
            }
        },
        {
            type: 'page_view',
            description: 'Просмотр страницы',
            example: {
                userId: 'user123',
                pageUrl: '/products/item123',
                sessionId: 'sess456',
                referrer: 'https://google.com'
            }
        },
        {
            type: 'user_registration',
            description: 'Регистрация пользователя',
            example: {
                userId: 'user123',
                email: 'user@example.com',
                registrationSource: 'web',
                timestamp: '2024-01-15T10:30:00Z'
            }
        },
        {
            type: 'click',
            description: 'Клик по элементу',
            example: {
                userId: 'user123',
                elementId: 'button_buy_now',
                pageUrl: '/products/item123',
                sessionId: 'sess456'
            }
        }
    ],

    // Примеры cURL команд
    curlExamples: {
        jsonMessage: `curl -X POST http://localhost:3000/api/v1/message/purchase/json \\
  -H "Content-Type: application/json" \\
  -d '{
    "userId": "user123",
    "productId": "prod456",
    "amount": 99.99,
    "currency": "USD"
  }'`,

        healthCheck: `curl -X GET http://localhost:3000/health`,

        irisMessage: `curl -X POST http://localhost:3000/api/v1/message/iris_message/iris \\
  -H "Content-Type: application/json" \\
  -d '{
    "irisData": "some_iris_string_data"
  }'`
    },

    // Примеры тестирования производительности
    performanceTest: {
        description: 'Тестирование производительности с помощью Apache Bench (ab)',
        commands: {
            basic: 'ab -n 1000 -c 10 -H "Content-Type: application/json" -p test_data.json http://localhost:3000/api/v1/message/test/json',
            highLoad: 'ab -n 10000 -c 100 -H "Content-Type: application/json" -p test_data.json http://localhost:3000/api/v1/message/test/json',
            healthCheck: 'ab -n 1000 -c 10 http://localhost:3000/health'
        },
        testDataFile: 'test_data.json',
        testDataContent: {
            userId: 'test_user',
            productId: 'test_product',
            amount: 100.00,
            currency: 'USD',
            timestamp: '2024-01-15T10:30:00Z'
        }
    }
};

module.exports = examples;
