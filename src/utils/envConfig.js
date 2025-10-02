require('dotenv').config();

/**
 * Утилита для работы с переменными окружения
 */
class EnvConfig {
    /**
     * Получить строку подключения к MongoDB
     * @param {string} defaultValue - значение по умолчанию
     * @returns {string} строка подключения
     */
    static getMongoConnectionString(defaultValue = 'mongodb://localhost:27017') {
        return process.env.MONGODB_CONNECTION_STRING || defaultValue;
    }

    /**
     * Получить имя базы данных для тестов
     * @param {string} defaultValue - значение по умолчанию
     * @returns {string} имя базы данных
     */
    static getTestDatabaseName(defaultValue = 'test') {
        return process.env.TEST_DATABASE_NAME || defaultValue;
    }

    /**
     * Получить уровень логирования
     * @param {string} defaultValue - значение по умолчанию
     * @returns {string} уровень логирования
     */
    static getLogLevel(defaultValue = 'INFO') {
        return process.env.LOG_LEVEL || defaultValue;
    }

    /**
     * Проверить, загружены ли переменные окружения
     * @returns {boolean} true если переменные загружены
     */
    static isEnvLoaded() {
        return process.env.MONGODB_CONNECTION_STRING !== undefined;
    }

    /**
     * Получить все переменные окружения для MongoDB
     * @returns {object} объект с настройками MongoDB
     */
    static getMongoConfig() {
        return {
            connectionString: this.getMongoConnectionString(),
            databaseName: this.getTestDatabaseName(),
            logLevel: this.getLogLevel()
        };
    }
}

module.exports = EnvConfig;
