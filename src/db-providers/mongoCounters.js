const fs = require('fs');
const Logger = require('../utils/logger');

/**
 * Класс для создания счетчиков на основе конфигурации и фактов
 * 
 * MongoCounters анализирует факты и определяет, какие счетчики должны быть применены
 * на основе условий в конфигурации. Результатом является структура для использования
 * в MongoDB aggregate запросе с оператором $facet.
 * 
 * Структура конфигурации счетчика:
 * @property {string} name - Имя счетчика
 * @property {string} comment - Комментарий к счетчику
 * @property {Object} condition - Условия применения счетчика к факту
 * @property {Array} aggregate - Массив операций MongoDB aggregate для счетчика
 * @property {Array} [variables] - Опциональный массив переменных для инициализации при агрегации
 */
class MongoCounters {
    constructor(configPathOrConfigArray = null) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this._counterConfig = [];
        
        if (!configPathOrConfigArray) {
            this.logger.warn('Конфигурация счетчиков не задана. Счетчики не будут создаваться.');
            return;
        }
        
        // Определяем способ инициализации
        if (Array.isArray(configPathOrConfigArray)) {
            // Инициализация через массив конфигурации
            this._counterConfig = this._validateConfig(configPathOrConfigArray);
            this.logger.info(`Конфигурация счетчиков инициализирована объектом. Количество счетчиков: ${this._counterConfig.length}`);
        } else if (typeof configPathOrConfigArray === 'string') {
            // Инициализация через путь к файлу
            this._counterConfig = this._loadConfig(configPathOrConfigArray);
        } else {
            this.logger.warn('Конфигурация счетчиков не задана. Счетчики будут создаваться по умолчанию.');
            return;
        }
    }

    /**
     * Загружает конфигурацию счетчиков из файла
     * @param {string} configPath - Путь к файлу конфигурации
     * @returns {Array} Массив конфигураций счетчиков
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     */
    _loadConfig(configPath) {
        try {
            if (!fs.existsSync(configPath)) {
                throw new Error(`Файл конфигурации счетчиков не найден: ${configPath}`);
            }

            const configData = fs.readFileSync(configPath, 'utf8');
            const counterConfig = JSON.parse(configData);

            // Валидация структуры конфигурации
            this._validateConfig(counterConfig);

            this.logger.info(`Загружена конфигурация счетчиков из ${configPath}`);
            this.logger.info(`Количество счетчиков: ${counterConfig.length}`);
            return counterConfig;
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации счетчиков: ${error.message}`);
            throw error;
        }
    }

    /**
     * Валидирует структуру загруженной конфигурации счетчиков
     * @param {Array} counterConfig - Конфигурация счетчиков для валидации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateConfig(counterConfig) {
        if (!Array.isArray(counterConfig)) {
            throw new Error('Конфигурация счетчиков должна быть массивом объектов');
        }

        for (let i = 0; i < counterConfig.length; i++) {
            const counter = counterConfig[i];
            
            if (!counter || typeof counter !== 'object') {
                throw new Error(`Счетчик ${i} должен быть объектом`);
            }

            if (!counter.name || typeof counter.name !== 'string') {
                throw new Error(`Счетчик ${i} должен содержать поле 'name' типа string`);
            }

            if (!counter.condition || typeof counter.condition !== 'object') {
                throw new Error(`Счетчик ${i} должен содержать поле 'condition' типа object`);
            }

            if (!Array.isArray(counter.aggregate)) {
                throw new Error(`Счетчик ${i} должен содержать поле 'aggregate' типа array`);
            }

            // Проверяем, что aggregate содержит валидные MongoDB операции
            for (let j = 0; j < counter.aggregate.length; j++) {
                const stage = counter.aggregate[j];
                if (!stage || typeof stage !== 'object') {
                    throw new Error(`Счетчик ${i}, этап aggregate ${j} должен быть объектом`);
                }
            }

            // Проверяем атрибут variables (опциональный)
            if (counter.variables !== undefined) {
                if (!Array.isArray(counter.variables)) {
                    throw new Error(`Счетчик ${i} должен содержать поле 'variables' типа array, если оно указано`);
                }
                
                // Проверяем, что все элементы variables являются строками
                for (let k = 0; k < counter.variables.length; k++) {
                    if (typeof counter.variables[k] !== 'string') {
                        throw new Error(`Счетчик ${i}, переменная ${k} должна быть строкой`);
                    }
                }
            }
        }
        
        this.logger.info(`Конфигурация счетчиков валидна. Количество счетчиков: ${counterConfig.length}`);
        return counterConfig;
    }

    /**
     * Проверяет, подходит ли факт под условие счетчика
     * @param {Object} fact - Факт для проверки
     * @param {Object} condition - Условие счетчика
     * @returns {boolean} true, если факт подходит под условие
     */
    _matchesCondition(fact, condition) {
        if (!fact || !fact.d) {
            return false;
        }

        const factData = fact.d;
        
        // Проверяем каждое условие
        for (const [field, expectedValue] of Object.entries(condition)) {
            const actualValue = factData[field];
            
            if (Array.isArray(expectedValue)) {
                // Если ожидаемое значение - массив, проверяем вхождение
                if (!expectedValue.includes(actualValue)) {
                    return false;
                }
            } else if (typeof expectedValue === 'object' && expectedValue !== null) {
                // Если ожидаемое значение - объект (MongoDB оператор), проверяем соответствие
                if (!this._matchesMongoOperator(actualValue, expectedValue)) {
                    return false;
                }
            } else {
                // Простое сравнение значений
                if (actualValue !== expectedValue) {
                    return false;
                }
            }
        }
        
        return true;
    }

    /**
     * Проверяет соответствие значения MongoDB оператору
     * @param {*} actualValue - Фактическое значение
     * @param {Object} operator - MongoDB оператор
     * @returns {boolean} true, если значение соответствует оператору
     */
    _matchesMongoOperator(actualValue, operator) {
        for (const [op, opValue] of Object.entries(operator)) {
            switch (op) {
                case '$in':
                    if (!Array.isArray(opValue) || !opValue.includes(actualValue)) {
                        return false;
                    }
                    break;
                case '$nin':
                    if (!Array.isArray(opValue) || opValue.includes(actualValue)) {
                        return false;
                    }
                    break;
                case '$ne':
                    if (actualValue === opValue) {
                        return false;
                    }
                    break;
                case '$not':
                    if (this._matchesMongoOperator(actualValue, opValue)) {
                        return false;
                    }
                    break;
                case '$regex':
                    if (typeof actualValue !== 'string' || !new RegExp(opValue).test(actualValue)) {
                        return false;
                    }
                    break;
                case '$exists':
                    const exists = actualValue !== undefined && actualValue !== null;
                    if (opValue && !exists) {
                        return false;
                    }
                    if (!opValue && exists) {
                        return false;
                    }
                    break;
                case '$or':
                    if (!Array.isArray(opValue)) {
                        return false;
                    }
                    const orResult = opValue.some(condition => {
                        if (typeof condition === 'object' && condition !== null) {
                            return this._matchesMongoOperator(actualValue, condition);
                        }
                        return actualValue === condition;
                    });
                    if (!orResult) {
                        return false;
                    }
                    break;
                default:
                    this.logger.warn(`Неподдерживаемый MongoDB оператор: ${op}`);
                    return false;
            }
        }
        return true;
    }

    /**
     * Создает структуру счетчиков для факта
     * @param {Object} fact - Факт для обработки
     * @returns {Object|null} Объект с полями facetStages и variables, или null если нет подходящих счетчиков
     * @returns {Object} facetStages - Структура для использования в MongoDB $facet aggregate запросе
     * @returns {Array<string>} variables - Массив уникальных переменных из всех подходящих счетчиков
     */
    make(fact) {
        if (!fact || !fact.d) {
            this.logger.warn('Передан некорректный факт для создания счетчиков');
            return null;
        }

        const facetStages = {};
        const variablesSet = new Set();
        let matchedCountersCount = 0;

        // Проходим по всем счетчикам и проверяем условия
        for (const counter of this._counterConfig) {
            if (this._matchesCondition(fact, counter.condition)) {
                if (counter.variables && counter.variables.length) {
                    counter.variables.forEach(variable => variablesSet.add(variable));
                }
                facetStages[counter.name] = counter.aggregate;
                matchedCountersCount++;
                this.logger.debug(`Счетчик '${counter.name}' подходит для факта ${fact._id}`);
            }
        }

        // this.logger.info(`Для факта ${fact._id} найдено подходящих счетчиков: ${matchedCountersCount} из ${this._counterConfig.length}`);
        // this.logger.info(`Факт: ${JSON.stringify(fact)}`);

        if (matchedCountersCount > 0) {
            return {
                facetStages: facetStages,
                variables: Array.from(variablesSet)
            };
        }
        
        return null;
    }

    /**
     * Получает список всех счетчиков в конфигурации
     * @returns {Array<string>} Массив имен счетчиков
     */
    getCounterNames() {
        return this._counterConfig.map(counter => counter.name);
    }

    /**
     * Получает конфигурацию счетчика по имени
     * @param {string} counterName - Имя счетчика
     * @returns {Object|null} Конфигурация счетчика или null, если не найден
     */
    getCounterConfig(counterName) {
        return this._counterConfig.find(counter => counter.name === counterName) || null;
    }

    /**
     * Получает общее количество счетчиков в конфигурации
     * @returns {number} Количество счетчиков
     */
    getCounterCount() {
        return this._counterConfig.length;
    }
}

module.exports = MongoCounters;
