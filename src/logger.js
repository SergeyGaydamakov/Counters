/**
 * Система логирования с настраиваемыми уровнями
 * Уровни: DEBUG, INFO, WARN, ERROR
 */

class Logger {
    constructor(level = 'INFO') {
        this.levels = {
            "DEBUG": 0,
            "INFO": 1,
            "WARN": 2,
            "ERROR": 3
        };
        const upperLevel = (level.trim() || 'INFO').toString().toUpperCase();
        this.level = this.levels[upperLevel] ?? this.levels.INFO;
        this.colors = {
            "DEBUG": '\x1b[36m',    // Cyan
            "INFO": '\x1b[32m',     // Green
            "WARN": '\x1b[33m',     // Yellow
            "ERROR": '\x1b[31m',    // Red
            "RESET": '\x1b[0m'      // Reset
        };
    }

    /**
     * Установить уровень логирования
     * @param {string} level - Уровень логирования (DEBUG, INFO, WARN, ERROR)
     */
    setLevel(level) {
        const upperLevel = String(level).toUpperCase();
        this.level = this.levels.hasOwnProperty(upperLevel) ? this.levels[upperLevel] : this.levels.INFO;
    }

    /**
     * Получить текущий уровень логирования
     * @returns {string} Текущий уровень логирования
     */
    getLevel() {
        return Object.keys(this.levels).find(key => this.levels[key] === this.level) || 'INFO';
    }

    /**
     * Проверить, должен ли сообщение быть выведено
     * @param {number} messageLevel - Уровень сообщения
     * @returns {boolean} true если сообщение должно быть выведено
     */
    shouldLog(messageLevel) {
        return messageLevel >= this.level;
    }

    /**
     * Форматировать сообщение для вывода
     * @param {string} level - Уровень сообщения
     * @param {string} message - Сообщение
     * @param {Array} args - Дополнительные аргументы
     * @returns {string} Отформатированное сообщение
     */
    formatMessage(level, message, args = []) {
        const timestamp = new Date().toISOString();
        const color = this.colors[level] || this.colors.RESET;
        const reset = this.colors.RESET;
        
        let formattedMessage = `${color}[${timestamp}] [${level}] ${message}${reset}`;
        
        if (args.length > 0) {
            formattedMessage += ' ' + args.map(arg => 
                typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
            ).join(' ');
        }
        
        return formattedMessage;
    }

    /**
     * Логирование уровня DEBUG
     * @param {string} message - Сообщение
     * @param {...any} args - Дополнительные аргументы
     */
    debug(message, ...args) {
        if (this.shouldLog(this.levels.DEBUG)) {
            console.log(this.formatMessage('DEBUG', message, args));
        }
    }

    /**
     * Логирование уровня INFO
     * @param {string} message - Сообщение
     * @param {...any} args - Дополнительные аргументы
     */
    info(message, ...args) {
        if (this.shouldLog(this.levels.INFO)) {
            console.log(this.formatMessage('INFO', message, args));
        }
    }

    /**
     * Логирование уровня WARN
     * @param {string} message - Сообщение
     * @param {...any} args - Дополнительные аргументы
     */
    warn(message, ...args) {
        if (this.shouldLog(this.levels.WARN)) {
            console.warn(this.formatMessage('WARN', message, args));
        }
    }

    /**
     * Логирование уровня ERROR
     * @param {string} message - Сообщение
     * @param {...any} args - Дополнительные аргументы
     */
    error(message, ...args) {
        if (this.shouldLog(this.levels.ERROR)) {
            console.error(this.formatMessage('ERROR', message, args));
        }
    }

    /**
     * Создать экземпляр логгера с указанным уровнем
     * @param {string} level - Уровень логирования
     * @returns {Logger} Новый экземпляр логгера
     */
    static create(level) {
        return new Logger(level);
    }

    /**
     * Создать экземпляр логгера из переменной окружения
     * @param {string} envVar - Имя переменной окружения (по умолчанию 'LOG_LEVEL')
     * @param {string} defaultLevel - Уровень по умолчанию (по умолчанию 'INFO')
     * @returns {Logger} Новый экземпляр логгера
     */
    static fromEnv(envVar = 'LOG_LEVEL', defaultLevel = 'INFO') {
        const level = process.env[envVar] || defaultLevel;
        return new Logger(level);
    }
}

module.exports = Logger;

