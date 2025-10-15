const fs = require('fs');
const Logger = require('../utils/logger');

/**
 * Класс для создания счетчиков на основе конфигурации и фактов
 * Типы счетчиков:
 * average amount - средняя сумма (аккумулятор $avg)
 * distinct values number - количество уникальных значений (аккумулятор $addToSet)
 * frequency - частота (аккумулятор $sum: 1)
 * maximum amount - максимальная сумма (аккумулятор $max)
 * total amount - общая сумма (аккумулятор $sum)
 * 
 * Пока не рассматриваем, следующие счетчики:
 * most occurrence ratio - коэффициент наиболее частого появления
 * most occurrence value - наиболее часто встречающееся значение
 * 
 * Параметры счетчиков:
 * Параметр счетчика - это строка, начинающаяся с "$$" и содержащая имя поля факта, например: "$$f2"
 * 
 * CounterProducer анализирует факты и определяет, какие счетчики должны быть применены
 * на основе условий в конфигурации. Результатом является структура для использования
 * в MongoDB aggregate запросе с оператором $facet.
 * 
 * Поддерживаемые MongoDB операторы в computationConditions:
 * - Операторы сравнения: $eq, $ne, $gt, $gte, $lt, $lte
 * - Операторы для массивов: $in, $nin, $all, $elemMatch, $size
 * - Операторы для строк: $regex, $options
 * - Логические операторы: $not, $and, $or
 * - Операторы существования: $exists
 * - Операторы типов: $type
 * - Операторы модуло: $mod
 * - Операторы выражений: $expr (с поддержкой полей факта)
 * - Неподдерживаемые операторы: $where, $text, геолокационные операторы
 * 
 * Структура конфигурации счетчика:
 * @property {string} name - Имя счетчика
 * @property {string} comment - Комментарий к счетчику
 * @property {string} indexTypeName - Название типа индекса
 * @property {Object} computationConditions - Условия применения счетчика к факту
 * @property {Object} evaluationConditions - Операции MongoDB aggregate для счетчика
 * @property {Object} attributes - Атрибуты счетчика (агрегированные значения)
 */
class CounterProducer {
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
                throw new Error(`Счетчик ${counter.name} должен быть объектом`);
            }

            if (!counter.name || typeof counter.name !== 'string') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'name' типа string`);
            }

            if (!counter.computationConditions || typeof counter.computationConditions !== 'object') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'computationConditions' типа object`);
            }
            // Проверяем, что computationConditions содержит валидные MongoDB операции
            for (let j = 0; j < counter.computationConditions.length; j++) {
                const stage = counter.computationConditions[j];
                if (!stage || typeof stage !== 'object') {
                    throw new Error(`Счетчик ${counter.name}, этап computationConditions ${j} должен быть объектом`);
                }
            }

            if (counter.evaluationConditions === undefined || typeof counter.evaluationConditions !== 'object') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'evaluationConditions' типа object`);
            }

        }
        
        this.logger.info(`Конфигурация счетчиков валидна. Количество счетчиков: ${counterConfig.length}`);
        return counterConfig;
    }

    /**
     * Функция возвращает значение поля по пути с точками
     * 
     * @param {Object} obj - объект для обработки
     * @param {string} path - путь к полю c точками
     * @returns {any} значение поля
     */
    _getValueByPath(obj, path) {
        const fields = path.split('.');
        return fields.reduce((acc, field) => acc[field], obj);
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

        if (!condition) {
            return true;
        }

        // Проверяем каждое условие
        for (const [field, expectedValue] of Object.entries(condition)) {
            // Специальная обработка для $expr
            if (field === '$expr') {
                if (!this._processExprExpression(fact, expectedValue)) {
                    return false;
                }
                continue;
            }

            const actualValue = this._getValueByPath(fact, field);
            
            if (Array.isArray(expectedValue)) {
                // Если ожидаемое значение - массив, проверяем вхождение
                if (!expectedValue.includes(actualValue)) {
                    return false;
                }
            } else if (typeof expectedValue === 'object' && expectedValue !== null) {
                // Если ожидаемое значение - объект (MongoDB оператор), проверяем соответствие
                if (!this._matchesMongoOperator(actualValue, expectedValue, fact)) {
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
     * @param {Object} fact - Факт для обработки (необходим для $expr)
     * @returns {boolean} true, если значение соответствует оператору
     */
    _matchesMongoOperator(actualValue, operator, fact = null) {
        for (const [op, opValue] of Object.entries(operator)) {
            switch (op) {
                // Операторы сравнения
                case '$eq':
                    // Обрабатываем $$NOW в opValue
                    const eqValue = opValue === '$$NOW' ? new Date() : opValue;
                    if (!this._compareValues(actualValue, eqValue, 'eq')) {
                        return false;
                    }
                    break;
                case '$ne':
                    // Если opValue - это объект с операторами, обрабатываем его рекурсивно
                    if (typeof opValue === 'object' && opValue !== null && !Array.isArray(opValue)) {
                        // Проверяем, содержит ли opValue MongoDB операторы
                        const hasOperators = Object.keys(opValue).some(key => key.startsWith('$'));
                        if (hasOperators) {
                            // Рекурсивно обрабатываем операторы и инвертируем результат
                            return !this._matchesMongoOperator(actualValue, opValue, fact);
                        }
                    }
                    // Обрабатываем $$NOW в opValue
                    const neValue = opValue === '$$NOW' ? new Date() : opValue;
                    // Простое сравнение
                    if (actualValue === neValue) {
                        return false;
                    }
                    break;
                case '$gt':
                    // Обрабатываем $$NOW и операторы дат в opValue
                    let gtValue = opValue;
                    if (opValue === '$$NOW') {
                        gtValue = new Date();
                    } else if (typeof opValue === 'object' && opValue !== null) {
                        // Проверяем, является ли это оператором даты
                        const dateResult = this._processDateOperator(opValue);
                        if (dateResult !== null) {
                            gtValue = dateResult;
                        }
                    }
                    if (!this._compareValues(actualValue, gtValue, 'gt')) {
                        return false;
                    }
                    break;
                case '$gte':
                    // Обрабатываем $$NOW и операторы дат в opValue
                    let gteValue = opValue;
                    if (opValue === '$$NOW') {
                        gteValue = new Date();
                    } else if (typeof opValue === 'object' && opValue !== null) {
                        // Проверяем, является ли это оператором даты
                        const dateResult = this._processDateOperator(opValue);
                        if (dateResult !== null) {
                            gteValue = dateResult;
                        }
                    }
                    if (!this._compareValues(actualValue, gteValue, 'gte')) {
                        return false;
                    }
                    break;
                case '$lt':
                    // Обрабатываем $$NOW и операторы дат в opValue
                    let ltValue = opValue;
                    if (opValue === '$$NOW') {
                        ltValue = new Date();
                    } else if (typeof opValue === 'object' && opValue !== null) {
                        // Проверяем, является ли это оператором даты
                        const dateResult = this._processDateOperator(opValue);
                        if (dateResult !== null) {
                            ltValue = dateResult;
                        }
                    }
                    if (!this._compareValues(actualValue, ltValue, 'lt')) {
                        return false;
                    }
                    break;
                case '$lte':
                    // Обрабатываем $$NOW и операторы дат в opValue
                    let lteValue = opValue;
                    if (opValue === '$$NOW') {
                        lteValue = new Date();
                    } else if (typeof opValue === 'object' && opValue !== null) {
                        // Проверяем, является ли это оператором даты
                        const dateResult = this._processDateOperator(opValue);
                        if (dateResult !== null) {
                            lteValue = dateResult;
                        }
                    }
                    if (!this._compareValues(actualValue, lteValue, 'lte')) {
                        return false;
                    }
                    break;

                // Операторы для массивов
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
                case '$all':
                    if (!Array.isArray(actualValue) || !Array.isArray(opValue)) {
                        return false;
                    }
                    if (!opValue.every(val => actualValue.includes(val))) {
                        return false;
                    }
                    break;
                case '$elemMatch':
                    if (!Array.isArray(actualValue)) {
                        return false;
                    }
                    if (!actualValue.some(elem => this._matchesMongoOperator(elem, opValue, fact))) {
                        return false;
                    }
                    break;
                case '$size':
                    if (!Array.isArray(actualValue) || actualValue.length !== opValue) {
                        return false;
                    }
                    break;

                // Операторы для строк
                case '$regex':
                    if (typeof actualValue !== 'string') {
                        return false;
                    }
                    const options = operator.$options || '';
                    const regex = new RegExp(opValue, options);
                    if (!regex.test(actualValue)) {
                        return false;
                    }
                    break;
                case '$options':
                    // $options обрабатывается вместе с $regex
                    break;

                // Логические операторы
                case '$not':
                    if (this._matchesMongoOperator(actualValue, opValue, fact)) {
                        return false;
                    }
                    break;
                case '$and':
                    if (!Array.isArray(opValue)) {
                        return false;
                    }
                    if (!opValue.every(condition => {
                        if (typeof condition === 'object' && condition !== null) {
                            return this._matchesMongoOperator(actualValue, condition, fact);
                        }
                        return actualValue === condition;
                    })) {
                        return false;
                    }
                    break;
                case '$or':
                    if (!Array.isArray(opValue)) {
                        return false;
                    }
                    if (!opValue.some(condition => {
                        if (typeof condition === 'object' && condition !== null) {
                            return this._matchesMongoOperator(actualValue, condition, fact);
                        }
                        return actualValue === condition;
                    })) {
                        return false;
                    }
                    break;

                // Операторы существования и типов
                case '$exists':
                    const exists = actualValue !== undefined && actualValue !== null;
                    if (opValue && !exists) {
                        return false;
                    }
                    if (!opValue && exists) {
                        return false;
                    }
                    break;
                case '$type':
                    const actualType = this._getMongoDBType(actualValue);
                    if (actualType !== opValue) {
                        return false;
                    }
                    break;

                // Операторы выражений
                case '$expr':
                    if (!fact) {
                        this.logger.warn(`Оператор $expr требует факт для обработки`);
                        return false;
                    }
                    return this._processExprExpression(fact, opValue);

                // Операторы модуло
                case '$mod':
                    if (!Array.isArray(opValue) || opValue.length !== 2) {
                        return false;
                    }
                    const [divisor, remainder] = opValue;
                    if (typeof actualValue !== 'number' || actualValue % divisor !== remainder) {
                        return false;
                    }
                    break;

                // Операторы JavaScript
                case '$where':
                    // $where не поддерживается в computationConditions
                    this.logger.warn(`Оператор $where не поддерживается в computationConditions`);
                    return false;

                // Операторы текстового поиска
                case '$text':
                    // $text не поддерживается в computationConditions
                    this.logger.warn(`Оператор $text не поддерживается в computationConditions`);
                    return false;

                // Геолокационные операторы
                case '$geoWithin':
                case '$geoIntersects':
                case '$near':
                case '$nearSphere':
                    // Геолокационные операторы не поддерживаются в computationConditions
                    this.logger.warn(`Геолокационный оператор ${op} не поддерживается в computationConditions`);
                    return false;

                default:
                    this.logger.warn(`Неподдерживаемый MongoDB оператор: ${op}`);
                    return false;
            }
        }
        return true;
    }

    /**
     * Обрабатывает MongoDB операторы для работы с датами
     * @param {Object} operator - Объект с оператором даты
     * @returns {Date|null} Результат выполнения оператора или null при ошибке
     */
    _processDateOperator(operator) {
        if (!operator || typeof operator !== 'object') {
            return null;
        }

        for (const [op, opValue] of Object.entries(operator)) {
            switch (op) {
                case '$dateAdd':
                    return this._processDateAdd(opValue);
                case '$dateSubtract':
                    return this._processDateSubtract(opValue);
                case '$dateDiff':
                    return this._processDateDiff(opValue);
                default:
                    this.logger.warn(`Неподдерживаемый оператор даты: ${op}`);
                    return null;
            }
        }

        return null;
    }

    /**
     * Обрабатывает оператор $dateAdd
     * @param {Object} opValue - Значение оператора
     * @returns {Date|null} Результат выполнения оператора
     */
    _processDateAdd(opValue) {
        if (!opValue || typeof opValue !== 'object') {
            return null;
        }

        const { startDate, unit, amount } = opValue;
        
        if (!startDate || !unit || typeof amount !== 'number') {
            return null;
        }

        // Обрабатываем $$NOW
        const baseDate = startDate === '$$NOW' ? new Date() : new Date(startDate);
        
        if (isNaN(baseDate.getTime())) {
            return null;
        }

        const result = new Date(baseDate);
        
        switch (unit) {
            case 'year':
                result.setFullYear(result.getFullYear() + amount);
                break;
            case 'month':
                result.setMonth(result.getMonth() + amount);
                break;
            case 'day':
                result.setDate(result.getDate() + amount);
                break;
            case 'hour':
                result.setHours(result.getHours() + amount);
                break;
            case 'minute':
                result.setMinutes(result.getMinutes() + amount);
                break;
            case 'second':
                result.setSeconds(result.getSeconds() + amount);
                break;
            case 'millisecond':
                result.setMilliseconds(result.getMilliseconds() + amount);
                break;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }

        return result;
    }

    /**
     * Обрабатывает оператор $dateSubtract
     * @param {Object} opValue - Значение оператора
     * @returns {Date|null} Результат выполнения оператора
     */
    _processDateSubtract(opValue) {
        if (!opValue || typeof opValue !== 'object') {
            return null;
        }

        const { startDate, unit, amount } = opValue;
        
        if (!startDate || !unit || typeof amount !== 'number') {
            return null;
        }

        // Обрабатываем $$NOW
        const baseDate = startDate === '$$NOW' ? new Date() : new Date(startDate);
        
        if (isNaN(baseDate.getTime())) {
            return null;
        }

        const result = new Date(baseDate);
        
        switch (unit) {
            case 'year':
                result.setFullYear(result.getFullYear() - amount);
                break;
            case 'month':
                result.setMonth(result.getMonth() - amount);
                break;
            case 'day':
                result.setDate(result.getDate() - amount);
                break;
            case 'hour':
                result.setHours(result.getHours() - amount);
                break;
            case 'minute':
                result.setMinutes(result.getMinutes() - amount);
                break;
            case 'second':
                result.setSeconds(result.getSeconds() - amount);
                break;
            case 'millisecond':
                result.setMilliseconds(result.getMilliseconds() - amount);
                break;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }

        return result;
    }

    /**
     * Обрабатывает оператор $dateDiff
     * @param {Object} opValue - Значение оператора
     * @returns {number|null} Результат выполнения оператора
     */
    _processDateDiff(opValue) {
        if (!opValue || typeof opValue !== 'object') {
            return null;
        }

        const { startDate, endDate, unit } = opValue;
        
        if (!startDate || !endDate || !unit) {
            return null;
        }

        // Обрабатываем $$NOW
        const start = startDate === '$$NOW' ? new Date() : new Date(startDate);
        const end = endDate === '$$NOW' ? new Date() : new Date(endDate);
        
        if (isNaN(start.getTime()) || isNaN(end.getTime())) {
            return null;
        }

        const diffMs = end.getTime() - start.getTime();
        
        switch (unit) {
            case 'year':
                return Math.floor(diffMs / (365.25 * 24 * 60 * 60 * 1000));
            case 'month':
                return Math.floor(diffMs / (30.44 * 24 * 60 * 60 * 1000));
            case 'day':
                return Math.floor(diffMs / (24 * 60 * 60 * 1000));
            case 'hour':
                return Math.floor(diffMs / (60 * 60 * 1000));
            case 'minute':
                return Math.floor(diffMs / (60 * 1000));
            case 'second':
                return Math.floor(diffMs / 1000);
            case 'millisecond':
                return diffMs;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }
    }

    /**
     * Сравнивает два значения с учетом их типов
     * @param {*} actualValue - Фактическое значение
     * @param {*} expectedValue - Ожидаемое значение
     * @param {string} operator - Оператор сравнения ('gt', 'gte', 'lt', 'lte')
     * @returns {boolean} true, если сравнение выполнено успешно
     */
    _compareValues(actualValue, expectedValue, operator) {
        // Преобразуем строки с числами в числа
        const actual = this._parseValue(actualValue);
        const expected = this._parseValue(expectedValue);
        
        // Если оба значения - числа, сравниваем как числа
        if (typeof actual === 'number' && typeof expected === 'number') {
            switch (operator) {
                case 'eq': return actual === expected;
                case 'gt': return actual > expected;
                case 'gte': return actual >= expected;
                case 'lt': return actual < expected;
                case 'lte': return actual <= expected;
                default: return false;
            }
        }
        
        // Если оба значения - строки, сравниваем как строки
        if (typeof actual === 'string' && typeof expected === 'string') {
            switch (operator) {
                case 'eq': return actual === expected;
                case 'gt': return actual > expected;
                case 'gte': return actual >= expected;
                case 'lt': return actual < expected;
                case 'lte': return actual <= expected;
                default: return false;
            }
        }
        
        // Если одно из значений - дата, сравниваем как даты
        if (actual instanceof Date || expected instanceof Date) {
            const actualDate = actual instanceof Date ? actual : new Date(actual);
            const expectedDate = expected instanceof Date ? expected : new Date(expected);
            
            // Проверяем, что даты валидны
            if (isNaN(actualDate.getTime()) || isNaN(expectedDate.getTime())) {
                return false;
            }
            
            switch (operator) {
                case 'eq': return actualDate.getTime() === expectedDate.getTime();
                case 'gt': return actualDate > expectedDate;
                case 'gte': return actualDate >= expectedDate;
                case 'lt': return actualDate < expectedDate;
                case 'lte': return actualDate <= expectedDate;
                default: return false;
            }
        }
        
        // Если типы не совпадают, возвращаем false
        return false;
    }

    /**
     * Парсит значение, пытаясь преобразовать строки с числами в числа
     * @param {*} value - Значение для парсинга
     * @returns {*} Парсированное значение
     */
    _parseValue(value) {
        if (typeof value === 'string') {
            // Пытаемся преобразовать строку в число
            const num = parseFloat(value.replace(',', '.'));
            if (!isNaN(num)) {
                return num;
            }
        }
        return value;
    }

    /**
     * Определяет тип значения в соответствии с MongoDB типами
     * @param {*} value - Значение для определения типа
     * @returns {string} Тип MongoDB
     */
    _getMongoDBType(value) {
        if (value === null) return 'null';
        if (value === undefined) return 'undefined';
        if (typeof value === 'boolean') return 'bool';
        if (typeof value === 'number') {
            if (Number.isInteger(value)) return 'int';
            return 'double';
        }
        if (typeof value === 'string') return 'string';
        if (Array.isArray(value)) return 'array';
        if (value instanceof Date) return 'date';
        if (typeof value === 'object') return 'object';
        return 'unknown';
    }

    /**
     * Извлекает значение поля из факта по пути MongoDB
     * @param {Object} fact - Факт для извлечения значения
     * @param {string} fieldPath - Путь к полю (например, "$d.field" или "d.field")
     * @returns {*} Значение поля или undefined, если поле не найдено
     */
    _extractFieldValue(fact, fieldPath) {
        if (!fact || !fieldPath) {
            return undefined;
        }

        // Специальная обработка для $$NOW
        if (fieldPath === '$$NOW') {
            return new Date();
        }

        // Убираем префикс $ если он есть
        const cleanPath = fieldPath.startsWith('$') ? fieldPath.substring(1) : fieldPath;
        
        // Разбиваем путь по точкам
        const pathParts = cleanPath.split('.');
        
        // Извлекаем значение по пути
        let current = fact;
        for (const part of pathParts) {
            if (current && typeof current === 'object' && part in current) {
                current = current[part];
            } else {
                return undefined;
            }
        }
        
        return current;
    }

    /**
     * Обрабатывает выражение $expr с полями факта
     * @param {Object} fact - Факт для обработки
     * @param {Object} exprValue - Значение выражения $expr
     * @returns {boolean} Результат выполнения выражения
     */
    _processExprExpression(fact, exprValue) {
        if (!exprValue || typeof exprValue !== 'object') {
            return false;
        }

        // Обрабатываем операторы сравнения в $expr
        for (const [operator, operands] of Object.entries(exprValue)) {
            switch (operator) {
                case '$eq':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return value1 === value2;
                    }
                    break;
                case '$ne':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return value1 !== value2;
                    }
                    break;
                case '$gt':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return this._compareValues(value1, value2, 'gt');
                    }
                    break;
                case '$gte':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return this._compareValues(value1, value2, 'gte');
                    }
                    break;
                case '$lt':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return this._compareValues(value1, value2, 'lt');
                    }
                    break;
                case '$lte':
                    if (Array.isArray(operands) && operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._extractFieldValue(fact, field1);
                        const value2 = this._extractFieldValue(fact, field2);
                        return this._compareValues(value1, value2, 'lte');
                    }
                    break;
                default:
                    this.logger.warn(`Неподдерживаемый оператор в $expr: ${operator}`);
                    return false;
            }
        }

        return false;
    }

    /**
     * Старый метод. Создает структуру счетчиков для факта
     * @param {Object} fact - Факт для обработки
     * @returns {Object|null} Объект с полем facetStages, или null если нет подходящих счетчиков
     * @returns {Object} facetStages - Структура для использования в MongoDB $facet aggregate запросе
     */
    make(fact) {
        if (!fact || !fact.d) {
            this.logger.warn('Передан некорректный факт для создания счетчиков');
            return null;
        }

        const facetStages = {};
        let matchedCountersCount = 0;
        let matchedIndexTypeNames = new Set();

        // Проходим по всем счетчикам и проверяем условия
        for (const counter of this._counterConfig) {
            if (this._matchesCondition(fact, counter.computationConditions)) {
                if (!counter.attributes) {
                    this.logger.warn(`Счетчик '${counter.name}' не имеет атрибутов (attributes). Счетчик не будет добавлен.`);
                    continue;
                }
                const matchStage = counter.evaluationConditions ? { "$match": counter.evaluationConditions } : null;
                const groupStage = { "$group": counter.attributes };
                groupStage["$group"]["_id"] = null;
                facetStages[counter.name] = [];
                if (matchStage) {
                    facetStages[counter.name].push(matchStage);
                }
                if (groupStage) {
                    facetStages[counter.name].push(groupStage);
                }
                matchedIndexTypeNames.add(counter.indexTypeName);
                matchedCountersCount++;
                this.logger.debug(`Счетчик '${counter.name}' подходит для факта ${fact._id}`);
            }
        }

        // this.logger.info(`Для факта ${fact._id} найдено подходящих счетчиков: ${matchedCountersCount} из ${this._counterConfig.length}`);
        // this.logger.info(`facetStages: ${JSON.stringify(facetStages)}`);

        if (matchedCountersCount > 0) {
            return {
                facetStages: facetStages,
                indexTypeNames: Array.from(matchedIndexTypeNames)
            };
        }
        
        return null;
    }

    /**
     * Получает конфигурацию счетчиков для указанного типа факта с кешированием ранее вычисленного результата
     * @param {integer} type - Тип факта
     * @returns {Array<Object>} Массив конфигураций счетчиков для указанного типа факта
     */
    getCounterConfigByType(type) {
        if (!this._counterConfigByType) {
            this._counterConfigByType = {};
        }
        if (!this._counterConfigByType[type]) {
            // Находим счетчики, у которых в computationConditions есть MessageTypeId и его значение равно type
            this._counterConfigByType[type] = [];
            this._counterConfig.forEach(counter => {
                // Проверяем только одно условие на MessageTypeId
                const messageTypeIdValue = counter.computationConditions["d.MessageTypeId"] || counter.computationConditions["t"];
                const condition = { "d.MessageTypeId": messageTypeIdValue };
                // Создаем факт с указанным типом для проверки условия
                const fact = {
                    "d": {
                        "MessageTypeId": type,
                        "t": type
                    }
                };
                if (!messageTypeIdValue || this._matchesCondition(fact, condition)) {
                    this._counterConfigByType[type].push(counter);
                }
            });
        }
        return this._counterConfigByType[type];
    }

    /**
     * Новый метод. Получает счетчики для факта
     * @param {Object} fact - Факт для обработки
     * @returns {Object|null} Объект с полем factCounters, или null если нет подходящих счетчиков
     * @returns {Object} factCounters - Массив счетчиков для факта
     */
    getFactCounters(fact) {
        if (!fact || !fact.d) {
            this.logger.warn('Передан некорректный факт для получения счетчиков');
            return null;
        }

        const factCounters = [];
        // Проходим по всем счетчикам и проверяем условия
        for (const counter of this.getCounterConfigByType(fact.t)) {
            if (this._matchesCondition(fact, counter.computationConditions)) {
                if (!counter.attributes) {
                    this.logger.warn(`Счетчик '${counter.name}' не имеет атрибутов (attributes). Счетчик не будет добавлен.`);
                    continue;
                }
                factCounters.push(counter);
                this.logger.debug(`Счетчик '${counter.name}' подходит для факта ${fact._id}`);
            } else {
                this.logger.debug(`Счетчик '${counter.name}' не подходит для факта ${fact._id} по условиям ${JSON.stringify(counter.computationConditions)}`);
            }
        }

        // this.logger.debug(`Для факта ${fact._id} найдено подходящих счетчиков: ${factCounters.length} из ${this._counterConfig.length}`);
        // this.logger.debug(`facetStages: ${JSON.stringify(facetStages)}`);

        if (!factCounters.length) {
            return null;
        }
        
        return factCounters;
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
    getCounterDescription(counterName) {
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

module.exports = CounterProducer;
