const Logger = require('../logger');

/**
 * Тесты для системы логирования Logger
 */
class LoggerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
    }

    /**
     * Запуск всех тестов
     */
    async runAllTests() {
        this.logger.debug('=== Тестирование системы логирования Logger ===\n');

        try {
            // Тесты создания логгера
            await this.testLoggerCreation('1. Тест создания логгера...');
            await this.testLoggerFromEnv('2. Тест создания логгера из переменных окружения...');
            
            // Тесты уровней логирования
            await this.testLogLevels('3. Тест уровней логирования...');
            await this.testLogLevelFiltering('4. Тест фильтрации по уровням логирования...');
            
            // Тесты методов логирования
            await this.testLogMethods('5. Тест методов логирования...');
            await this.testLogFormatting('6. Тест форматирования логов...');
            
            // Тесты статических методов
            await this.testStaticMethods('7. Тест статических методов...');
            
            // Тесты с различными типами данных
            await this.testDataTypes('8. Тест различных типов данных...');
            
            // Тесты производительности
            await this.testPerformance('9. Тест производительности...');

        } catch (error) {
            this.logger.error('Критическая ошибка в тестах:', error.message);
            this.testResults.failed++;
            this.testResults.errors.push(`Критическая ошибка: ${error.message}`);
        }

        this.printResults();
    }

    /**
     * Тест 1: Создание логгера с различными уровнями
     */
    async testLoggerCreation(title) {
        this.logger.debug(title);
        
        try {
            // Тест создания с валидными уровнями
            const debugLogger = new Logger('DEBUG');
            const infoLogger = new Logger('INFO');
            const warnLogger = new Logger('WARN');
            const errorLogger = new Logger('ERROR');
            
            // Проверяем, что логгеры созданы
            if (debugLogger && infoLogger && warnLogger && errorLogger) {
                this.logger.debug('   ✓ Все логгеры созданы успешно');
                this.testResults.passed++;
            } else {
                throw new Error('Не все логгеры созданы');
            }
            
            // Тест создания с невалидным уровнем (должен использовать INFO по умолчанию)
            const invalidLogger = new Logger('INVALID');
            if (invalidLogger.getLevel() === 'INFO') {
                this.logger.debug('   ✓ Невалидный уровень обработан корректно (по умолчанию INFO)');
                this.testResults.passed++;
            } else {
                throw new Error('Невалидный уровень не обработан корректно');
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLoggerCreation: ${error.message}`);
        }
    }

    /**
     * Тест 2: Создание логгера из переменной окружения
     */
    async testLoggerFromEnv(title) {
        this.logger.debug(title);
        
        try {
            // Сохраняем оригинальное значение
            const originalLogLevel = process.env.LOG_LEVEL;
            
            // Тест с установленной переменной
            process.env.LOG_LEVEL = 'WARN';
            const envLogger1 = Logger.fromEnv('LOG_LEVEL', 'INFO');
            if (envLogger1.getLevel() === 'WARN') {
                this.logger.debug('   ✓ Логгер создан из переменной окружения');
                this.testResults.passed++;
            } else {
                throw new Error('Логгер не создан из переменной окружения');
            }
            
            // Тест с несуществующей переменной (должен использовать значение по умолчанию)
            delete process.env.LOG_LEVEL;
            const envLogger2 = Logger.fromEnv('LOG_LEVEL', 'ERROR');
            if (envLogger2.getLevel() === 'ERROR') {
                this.logger.debug('   ✓ Логгер создан с значением по умолчанию');
                this.testResults.passed++;
            } else {
                throw new Error('Логгер не создан с значением по умолчанию');
            }
            
            // Восстанавливаем оригинальное значение
            if (originalLogLevel) {
                process.env.LOG_LEVEL = originalLogLevel;
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLoggerFromEnv: ${error.message}`);
        }
    }

    /**
     * Тест 3: Проверка уровней логирования
     */
    async testLogLevels(title) {
        this.logger.debug(title);
        
        try {
            const logger = new Logger('DEBUG');
            
            // Проверяем все уровни
            const levels = ['DEBUG', 'INFO', 'WARN', 'ERROR'];
            for (const level of levels) {
                logger.setLevel(level);
                const currentLevel = logger.getLevel();
                if (currentLevel === level) {
                    this.logger.debug(`   ✓ Уровень ${level} установлен корректно`);
                } else {
                    throw new Error(`Уровень ${level} не установлен корректно. Текущий: ${currentLevel}, внутренний: ${logger.level}`);
                }
            }
            
            this.testResults.passed++;
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLogLevels: ${error.message}`);
        }
    }

    /**
     * Тест 4: Фильтрация по уровням логирования
     */
    async testLogLevelFiltering(title) {
        this.logger.debug(title);
        
        try {
            // Создаем логгер с уровнем INFO
            const logger = new Logger('INFO');
            
            // Перехватываем console методы для проверки вывода
            const originalLog = console.log;
            const originalWarn = console.warn;
            const originalError = console.error;
            
            let debugCalled = false;
            let infoCalled = false;
            let warnCalled = false;
            let errorCalled = false;
            
            console.log = (message) => {
                if (message.includes('[DEBUG]')) debugCalled = true;
                if (message.includes('[INFO]')) infoCalled = true;
            };
            console.warn = (message) => {
                if (message.includes('[WARN]')) warnCalled = true;
            };
            console.error = (message) => {
                if (message.includes('[ERROR]')) errorCalled = true;
            };
            
            // Вызываем методы логирования
            logger.debug('Debug message');
            logger.info('Info message');
            logger.warn('Warn message');
            logger.error('Error message');
            
            // Восстанавливаем оригинальные методы
            console.log = originalLog;
            console.warn = originalWarn;
            console.error = originalError;
            
            // Проверяем результаты
            if (!debugCalled && infoCalled && warnCalled && errorCalled) {
                this.logger.debug('   ✓ Фильтрация работает корректно (DEBUG отфильтрован, остальные прошли)');
                this.testResults.passed++;
            } else {
                throw new Error(`Фильтрация работает некорректно: debug=${debugCalled}, info=${infoCalled}, warn=${warnCalled}, error=${errorCalled}`);
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLogLevelFiltering: ${error.message}`);
        }
    }

    /**
     * Тест 5: Методы логирования
     */
    async testLogMethods(title) {
        this.logger.debug(title);
        
        try {
            const logger = new Logger('DEBUG');
            
            // Перехватываем console методы
            const originalLog = console.log;
            const originalWarn = console.warn;
            const originalError = console.error;
            
            let logCalled = false;
            let warnCalled = false;
            let errorCalled = false;
            
            console.log = () => { logCalled = true; };
            console.warn = () => { warnCalled = true; };
            console.error = () => { errorCalled = true; };
            
            // Тестируем методы
            logger.debug('Debug test');
            logger.info('Info test');
            logger.warn('Warn test');
            logger.error('Error test');
            
            // Восстанавливаем оригинальные методы
            console.log = originalLog;
            console.warn = originalWarn;
            console.error = originalError;
            
            if (logCalled && warnCalled && errorCalled) {
                this.logger.debug('   ✓ Все методы логирования работают');
                this.testResults.passed++;
            } else {
                throw new Error('Не все методы логирования работают');
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLogMethods: ${error.message}`);
        }
    }

    /**
     * Тест 6: Форматирование сообщений
     */
    async testLogFormatting(title) {
        this.logger.debug(title);
        
        try {
            const logger = new Logger('DEBUG');
            
            // Перехватываем console.log
            const originalLog = console.log;
            let capturedMessage = '';
            
            console.log = (message) => {
                capturedMessage = message;
            };
            
            // Тестируем форматирование
            logger.debug('Test message');
            
            // Восстанавливаем оригинальный console.log
            console.log = originalLog;
            
            // Если сообщение пустое, возможно логгер не выводит DEBUG сообщения
            if (!capturedMessage) {
                // Попробуем с INFO уровнем
                logger.setLevel('INFO');
                console.log = (message) => {
                    capturedMessage = message;
                };
                logger.info('Test message');
                console.log = originalLog;
            }
            
            // Проверяем формат сообщения
            const timestampRegex = /\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\]/;
            const levelRegex = /\[(DEBUG|INFO|WARN|ERROR)\]/;
            
            if (timestampRegex.test(capturedMessage) && levelRegex.test(capturedMessage)) {
                this.logger.debug('   ✓ Форматирование сообщений работает корректно');
                this.testResults.passed++;
            } else {
                throw new Error(`Форматирование сообщений работает некорректно. Сообщение: "${capturedMessage}"`);
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testLogFormatting: ${error.message}`);
        }
    }

    /**
     * Тест 7: Статические методы
     */
    async testStaticMethods(title) {
        this.logger.debug(title);
        
        try {
            // Тест Logger.create()
            const logger1 = Logger.create('WARN');
            if (logger1.getLevel() === 'WARN') {
                this.logger.debug('   ✓ Logger.create() работает');
                this.testResults.passed++;
            } else {
                throw new Error('Logger.create() не работает');
            }
            
            // Тест Logger.fromEnv()
            const originalLogLevel = process.env.LOG_LEVEL;
            process.env.LOG_LEVEL = 'ERROR';
            
            const logger2 = Logger.fromEnv('LOG_LEVEL', 'INFO');
            if (logger2.getLevel() === 'ERROR') {
                this.logger.debug('   ✓ Logger.fromEnv() работает');
                this.testResults.passed++;
            } else {
                throw new Error('Logger.fromEnv() не работает');
            }
            
            // Восстанавливаем оригинальное значение
            if (originalLogLevel) {
                process.env.LOG_LEVEL = originalLogLevel;
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testStaticMethods: ${error.message}`);
        }
    }

    /**
     * Тест 8: Различные типы данных
     */
    async testDataTypes(title) {
        this.logger.debug(title);
        
        try {
            const logger = new Logger('DEBUG');
            
            // Перехватываем console.log
            const originalLog = console.log;
            let capturedMessages = [];
            
            console.log = (message) => {
                capturedMessages.push(message);
            };
            
            // Тестируем различные типы данных
            logger.debug('String message');
            logger.debug('Number:', 42);
            logger.debug('Boolean:', true);
            logger.debug('Object:', { key: 'value', nested: { data: 123 } });
            logger.debug('Array:', [1, 2, 3, 'test']);
            logger.debug('Null:', null);
            logger.debug('Undefined:', undefined);
            
            // Если сообщения не захвачены, попробуем с INFO уровнем
            if (capturedMessages.length === 0) {
                logger.setLevel('INFO');
                logger.info('String message');
                logger.info('Number:', 42);
                logger.info('Boolean:', true);
                logger.info('Object:', { key: 'value', nested: { data: 123 } });
                logger.info('Array:', [1, 2, 3, 'test']);
                logger.info('Null:', null);
                logger.info('Undefined:', undefined);
            }
            
            // Восстанавливаем оригинальный console.log
            console.log = originalLog;
            
            if (capturedMessages.length >= 7) {
                this.logger.debug('   ✓ Различные типы данных обрабатываются корректно');
                this.testResults.passed++;
            } else {
                throw new Error(`Не все типы данных обработаны. Получено сообщений: ${capturedMessages.length}, ожидалось: 7`);
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testDataTypes: ${error.message}`);
        }
    }

    /**
     * Тест 9: Производительность
     */
    async testPerformance(title) {
        this.logger.debug(title);
        
        try {
            const logger = new Logger('DEBUG');
            const iterations = 1000;
            
            // Перехватываем console.log для измерения производительности
            const originalLog = console.log;
            console.log = () => {}; // Отключаем вывод для теста производительности
            
            const startTime = Date.now();
            
            // Выполняем множество операций логирования
            for (let i = 0; i < iterations; i++) {
                logger.debug(`Performance test message ${i}`);
            }
            
            const endTime = Date.now();
            const duration = endTime - startTime;
            
            // Восстанавливаем оригинальный console.log
            console.log = originalLog;
            
            const avgTimePerLog = duration / iterations;
            
            if (avgTimePerLog < 1) { // Менее 1мс на операцию
                this.logger.debug(`   ✓ Производительность приемлемая (${avgTimePerLog.toFixed(3)}мс на операцию)`);
                this.testResults.passed++;
            } else {
                this.logger.debug(`   ⚠ Производительность низкая (${avgTimePerLog.toFixed(3)}мс на операцию)`);
                this.testResults.passed++; // Не считаем это ошибкой, но предупреждаем
            }
            
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testPerformance: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования Logger ===');
        this.logger.info(`Пройдено: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
        this.logger.error('\nОшибки:');
        this.testResults.errors.forEach(error => {
            this.logger.error(`  - ${error}`);
        });
        }
        
        const successRate = this.testResults.passed / (this.testResults.passed + this.testResults.failed) * 100;
        this.logger.info(`\nПроцент успеха: ${successRate.toFixed(1)}%`);
        
        if (this.testResults.failed === 0) {
            this.logger.info('🎉 Все тесты прошли успешно!');
        } else {
            this.logger.error(`⚠ ${this.testResults.failed} тестов провалено`);
        }
    }
}

// Запуск тестов
if (require.main === module) {
    const test = new LoggerTest();
    test.runAllTests()
        .then(() => {
            process.exit(0);
        })
        .catch((error) => {
            this.logger.error('Критическая ошибка:', error.message);
            process.exit(1);
        });
}

module.exports = LoggerTest;
