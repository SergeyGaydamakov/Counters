const EnvConfig = require('../utils/envConfig');
const Logger = require('../utils/logger');

/**
 * Тест конфигурации переменных окружения
 */
class EnvConfigTest {
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
        this.logger.debug('=== Тестирование конфигурации переменных окружения ===\n');

        try {
            await this.testMongoConnectionString();
            await this.testTestDatabaseName();
            await this.testLogLevel();
            await this.testMongoConfig();
            await this.testEnvLoaded();
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        }

        this.printResults();
    }

    /**
     * Тест получения строки подключения к MongoDB
     */
    async testMongoConnectionString() {
        this.logger.debug('1. Тест получения строки подключения к MongoDB...');
        
        try {
            const connectionString = EnvConfig.getMongoConnectionString();
            
            if (!connectionString || typeof connectionString !== 'string') {
                throw new Error('Строка подключения должна быть непустой строкой');
            }

            if (!connectionString.startsWith('mongodb://') && !connectionString.startsWith('mongodb+srv://')) {
                throw new Error('Строка подключения должна начинаться с mongodb:// или mongodb+srv://');
            }

            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно: ${connectionString}`);
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testMongoConnectionString: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения имени тестовой базы данных
     */
    async testTestDatabaseName() {
        this.logger.debug('2. Тест получения имени тестовой базы данных...');
        
        try {
            const databaseName = EnvConfig.getTestDatabaseName();
            
            if (!databaseName || typeof databaseName !== 'string') {
                throw new Error('Имя базы данных должно быть непустой строкой');
            }

            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно: ${databaseName}`);
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testTestDatabaseName: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения уровня логирования
     */
    async testLogLevel() {
        this.logger.debug('3. Тест получения уровня логирования...');
        
        try {
            const logLevel = EnvConfig.getLogLevel();
            const validLevels = ['DEBUG', 'INFO', 'WARN', 'ERROR'];
            
            if (!logLevel || typeof logLevel !== 'string') {
                throw new Error('Уровень логирования должен быть непустой строкой');
            }

            // Убираем лишние пробелы
            const trimmedLogLevel = logLevel.trim();
            if (!validLevels.includes(trimmedLogLevel)) {
                throw new Error(`Уровень логирования должен быть одним из: ${validLevels.join(', ')} (получен: "${logLevel}")`);
            }

            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно: ${logLevel}`);
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testLogLevel: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест получения полной конфигурации MongoDB
     */
    async testMongoConfig() {
        this.logger.debug('4. Тест получения полной конфигурации MongoDB...');
        
        try {
            const config = EnvConfig.getMongoConfig();
            
            if (!config || typeof config !== 'object') {
                throw new Error('Конфигурация должна быть объектом');
            }

            const requiredFields = ['connectionString', 'databaseName', 'logLevel'];
            for (const field of requiredFields) {
                if (!(field in config)) {
                    throw new Error(`Конфигурация должна содержать поле: ${field}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно:`, config);
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testMongoConfig: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест проверки загрузки переменных окружения
     */
    async testEnvLoaded() {
        this.logger.debug('5. Тест проверки загрузки переменных окружения...');
        
        try {
            const isLoaded = EnvConfig.isEnvLoaded();
            
            if (typeof isLoaded !== 'boolean') {
                throw new Error('isEnvLoaded должен возвращать boolean');
            }

            this.testResults.passed++;
            this.logger.debug(`   ✓ Успешно: переменные ${isLoaded ? 'загружены' : 'не загружены'}`);
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push(`testEnvLoaded: ${error.message}`);
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.debug('\n=== Результаты тестирования конфигурации переменных окружения ===');
        this.logger.debug(`Пройдено: ${this.testResults.passed}`);
        this.logger.debug(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
            this.logger.debug('\nОшибки:');
            this.testResults.errors.forEach(error => {
                this.logger.debug(`  - ${error}`);
            });
        }
        
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? (this.testResults.passed / total * 100).toFixed(1) : 0;
        this.logger.debug(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new EnvConfigTest();
    test.runAllTests().catch(console.error);
}

module.exports = EnvConfigTest;
