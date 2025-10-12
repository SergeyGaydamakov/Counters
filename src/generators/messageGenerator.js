const { ObjectId } = require('mongodb');
const fs = require('fs');
const path = require('path');
const {ERROR_WRONG_MESSAGE_TYPE} = require('../common/errors')

/**
 * Класс для генерации случайных тестовых данных
 * 
 * Структура сообщения (message):
 * @property {string} _id - Идентификатор сообщения uuidv4
 * @property {number} t - Тип сообщения (число)
 * @property {Date} c - Дата и время создания сообщения в базе данных
 * @property {object} d - данные сообщения
 * 
 */
class MessageGenerator {
    constructor(fieldConfigPathOrArray = null, targetSize = null) {
        // Загружаем конфигурацию полей
        this._fieldConfig = this._loadFieldConfig(fieldConfigPathOrArray);
        
        // Извлекаем информацию из конфигурации
        this._availableFields = this._fieldConfig.map(field => field.src);
        this._availableTypes = this._extractAvailableTypes();
        this._typeFieldsMap = this._buildTypeFieldsMap();
        this._fieldGeneratorsMap = this._buildFieldGeneratorsMap();
        
        // Сохраняем даты для генерации сообщений
        this._targetSize = targetSize;
    }

    /**
     * Загружает конфигурацию полей из файла или использует переданную структуру
     * @param {string|Array|null} fieldConfigPathOrArray - путь к файлу конфигурации, массив конфигурации или null для использования messageConfig.json
     * @returns {Array} массив конфигурации полей
     */
    _loadFieldConfig(fieldConfigPathOrArray) {
        if (!fieldConfigPathOrArray) {
            throw new Error('Не указана конфигурация полей (имя файла или структура)');
        }
        // Определяем способ инициализации
        if (Array.isArray(fieldConfigPathOrArray)) {
            // Инициализация через массив конфигурации
            this._validateFieldConfig(fieldConfigPathOrArray);
            return fieldConfigPathOrArray;
        } else {
            // Инициализация через путь к файлу
            const configPath = fieldConfigPathOrArray || path.join(process.cwd(), 'messageConfig.json');
            return this._loadConfigFromFile(configPath);
        }
    }

    /**
     * Загружает конфигурацию из файла
     * @param {string} configPath - путь к файлу конфигурации
     * @returns {Array} массив конфигурации полей
     */
    _loadConfigFromFile(configPath) {
        try {
            if (!fs.existsSync(configPath)) {
                throw new Error(`Файл конфигурации не найден: ${configPath}`);
            }

            const configData = fs.readFileSync(configPath, 'utf8');
            const fieldConfig = JSON.parse(configData);

            // Валидация структуры конфигурации
            this._validateFieldConfig(fieldConfig);
            return fieldConfig;
        } catch (error) {
            throw new Error(`Ошибка загрузки конфигурации: ${error.message}`);
        }
    }

    /**
     * Валидирует структуру конфигурации полей
     * @param {Array} fieldConfig - конфигурация полей для валидации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateFieldConfig(fieldConfig) {
        if (!Array.isArray(fieldConfig)) {
            throw new Error('Конфигурация полей должна быть массивом объектов');
        }

        for (let i = 0; i < fieldConfig.length; i++) {
            const field = fieldConfig[i];
            
            if (!field || typeof field !== 'object') {
                throw new Error(`Поле конфигурации ${i} должно быть объектом`);
            }

            if (!field.src || typeof field.src !== 'string') {
                throw new Error(`Поле конфигурации ${i} должно содержать поле 'src' типа string`);
            }

            if (!field.dst || typeof field.dst !== 'string') {
                throw new Error(`Поле конфигурации ${i} должно содержать поле 'dst' типа string`);
            }

            if (!Array.isArray(field.message_types) || field.message_types.length === 0) {
                throw new Error(`Поле конфигурации ${i} должно содержать непустой массив 'message_types'`);
            }

            for (let j = 0; j < field.message_types.length; j++) {
                if (typeof field.message_types[j] !== 'number' || !Number.isInteger(field.message_types[j])) {
                    throw new Error(`Для поля конфигурации [${i}] ${field.src} тип сообщения [${j}] имеет значение ${field.message_types[j]}, а должен быть целым числом`);
                }
            }

            // Валидация поля generator (опционального)
            if (field.generator !== undefined) {
                this._validateGeneratorConfig(field.generator, i);
            }
        }
    }

    /**
     * Валидирует конфигурацию генератора для поля
     * @param {Object} generator - конфигурация генератора
     * @param {number} fieldIndex - индекс поля в конфигурации
     * @throws {Error} если конфигурация генератора имеет неверный формат
     */
    _validateGeneratorConfig(generator, fieldIndex) {
        if (!generator || typeof generator !== 'object') {
            throw new Error(`Поле конфигурации ${fieldIndex}: generator должен быть объектом`);
        }

        if (!generator.type || typeof generator.type !== 'string') {
            throw new Error(`Поле конфигурации ${fieldIndex}: generator.type должен быть строкой`);
        }

        const validTypes = ['string', 'integer', 'date', 'enum', 'objectId', 'boolean'];
        if (!validTypes.includes(generator.type)) {
            throw new Error(`Поле конфигурации [${fieldIndex}] ${generator.src} : generator.type = ${generator.type}, а должно быть одним из: ${validTypes.join(', ')}`);
        }

        // Валидация дополнительных параметров
        if (generator.default_value !== undefined) {
            // default_value может быть любого типа в зависимости от типа генератора
            // Валидация будет выполнена в соответствующих методах генерации
        }
        
        if (generator.default_random !== undefined) {
            if (typeof generator.default_random !== 'number' || generator.default_random < 0 || generator.default_random > 1) {
                throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_random должен быть числом от 0 до 1`);
            }
        }

        // Валидация в зависимости от типа генератора
        switch (generator.type) {
            case 'string':
                if (generator.min !== undefined && (typeof generator.min !== 'number' || !Number.isInteger(generator.min) || generator.min < 0)) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.min для string должен быть неотрицательным целым числом`);
                }
                if (generator.max !== undefined && (typeof generator.max !== 'number' || !Number.isInteger(generator.max) || generator.max < 0)) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.max для string должен быть неотрицательным целым числом`);
                }
                if (generator.min !== undefined && generator.max !== undefined && generator.min > generator.max) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.min не может быть больше generator.max`);
                }
                if (generator.default_value !== undefined && typeof generator.default_value !== 'string') {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для string должен быть строкой`);
                }
                break;

            case 'integer':
                if (generator.min !== undefined && (typeof generator.min !== 'number' || !Number.isInteger(generator.min))) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.min для integer должен быть целым числом`);
                }
                if (generator.max !== undefined && (typeof generator.max !== 'number' || !Number.isInteger(generator.max))) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.max для integer должен быть целым числом`);
                }
                if (generator.min !== undefined && generator.max !== undefined && generator.min > generator.max) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.min не может быть больше generator.max`);
                }
                if (generator.default_value !== undefined && (typeof generator.default_value !== 'number' || !Number.isInteger(generator.default_value))) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для integer должен быть целым числом`);
                }
                break;

            case 'date':
                if (generator.min !== undefined && typeof generator.min !== 'string') {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.min для date должен быть строкой`);
                }
                if (generator.max !== undefined && typeof generator.max !== 'string') {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.max для date должен быть строкой`);
                }
                // Проверяем, что даты можно распарсить
                if (generator.min !== undefined) {
                    const minDate = new Date(generator.min);
                    if (isNaN(minDate.getTime())) {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.min содержит невалидную дату`);
                    }
                }
                if (generator.max !== undefined) {
                    const maxDate = new Date(generator.max);
                    if (isNaN(maxDate.getTime())) {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.max содержит невалидную дату`);
                    }
                }
                if (generator.min !== undefined && generator.max !== undefined) {
                    const minDate = new Date(generator.min);
                    const maxDate = new Date(generator.max);
                    if (minDate > maxDate) {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.min не может быть больше generator.max`);
                    }
                }
                if (generator.default_value !== undefined) {
                    if (typeof generator.default_value === 'string') {
                        const defaultDate = new Date(generator.default_value);
                        if (isNaN(defaultDate.getTime())) {
                            throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value содержит невалидную дату`);
                        }
                    } else if (generator.default_value instanceof Date) {
                        // Date объект валиден
                    } else {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для date должен быть строкой или Date объектом`);
                    }
                }
                break;

            case 'enum':
                if (!Array.isArray(generator.values) || generator.values.length === 0) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.values для enum должен быть непустым массивом`);
                }
                if (generator.default_value !== undefined && !generator.values.includes(generator.default_value)) {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для enum должен быть одним из значений в массиве values`);
                }
                break;

            case 'objectId':
                if (generator.default_value !== undefined) {
                    if (typeof generator.default_value !== 'string') {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для objectId должен быть строкой`);
                    }
                    // Проверяем, что default_value является валидным ObjectId (24 hex символа)
                    if (!/^[0-9a-fA-F]{24}$/.test(generator.default_value)) {
                        throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для objectId должен быть валидным ObjectId (24 hex символа)`);
                    }
                }
                break;

            case 'boolean':
                if (generator.default_value !== undefined && typeof generator.default_value !== 'boolean') {
                    throw new Error(`Поле конфигурации ${fieldIndex}: generator.default_value для boolean должен быть булевым значением`);
                }
                break;
        }
    }

    /**
     * Извлекает все доступные типы из конфигурации полей
     * @returns {Array} массив уникальных типов
     */
    _extractAvailableTypes() {
        const types = new Set();
        this._fieldConfig.forEach(field => {
            field.message_types.forEach(type => types.add(type));
        });
        return Array.from(types);
    }

    /**
     * Строит карту полей для каждого типа
     * @returns {Object} объект где ключ - тип, значение - массив полей
     */
    _buildTypeFieldsMap() {
        const typeFieldsMap = {};
        this._availableTypes.forEach(type => {
            typeFieldsMap[type] = this._fieldConfig
                .filter(field => field.message_types.includes(type))
                .map(field => field.src);
        });
        return typeFieldsMap;
    }

    /**
     * Строит карту генераторов для каждого поля
     * @returns {Object} объект где ключ - имя поля, значение - конфигурация генератора
     */
    _buildFieldGeneratorsMap() {
        const fieldGeneratorsMap = {};
        this._fieldConfig.forEach(field => {
            fieldGeneratorsMap[field.src] = field.generator || null;
        });
        return fieldGeneratorsMap;
    }

    /**
     * Генерирует случайную строку из латинских символов
     * @param {number} minLength - минимальная длина строки
     * @param {number} maxLength - максимальная длина строки
     * @param {string} defaultValue - значение по умолчанию
     * @param {number} defaultRandom - вероятность использования значения по умолчанию (0-1)
     * @returns {string} случайная строка
     */
    _generateRandomString(minLength = 2, maxLength = 20, defaultValue = null, defaultRandom = 0) {
        // Проверяем, нужно ли использовать значение по умолчанию
        if (defaultValue !== null && Math.random() < defaultRandom) {
            return defaultValue;
        }
        
        const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        const length = Math.floor(Math.random() * (maxLength - minLength + 1)) + minLength;
        let result = '';
        
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        
        return result;
    }

    /**
     * Генерирует случайное целое число в заданном диапазоне
     * @param {number} min - минимальное значение
     * @param {number} max - максимальное значение
     * @param {number} defaultValue - значение по умолчанию
     * @param {number} defaultRandom - вероятность использования значения по умолчанию (0-1)
     * @returns {number} случайное целое число
     */
    _generateRandomInteger(min = 0, max = 100, defaultValue = null, defaultRandom = 0) {
        // Проверяем, нужно ли использовать значение по умолчанию
        if (defaultValue !== null && Math.random() < defaultRandom) {
            return defaultValue;
        }
        
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    /**
     * Генерирует случайную дату в заданном диапазоне
     * @param {string|Date} minDate - минимальная дата (строка или Date)
     * @param {string|Date} maxDate - максимальная дата (строка или Date)
     * @param {string|Date} defaultValue - значение по умолчанию
     * @param {number} defaultRandom - вероятность использования значения по умолчанию (0-1)
     * @returns {Date} случайная дата
     */
    _generateRandomDateFromRange(minDate, maxDate, defaultValue = null, defaultRandom = 0) {
        // Проверяем, нужно ли использовать значение по умолчанию
        if (defaultValue !== null && Math.random() < defaultRandom) {
            return defaultValue instanceof Date ? defaultValue : new Date(defaultValue);
        }
        
        const fromDate = minDate instanceof Date ? minDate : new Date(minDate);
        const toDate = maxDate instanceof Date ? maxDate : new Date(maxDate);
        return this._generateRandomDate(fromDate, toDate);
    }

    /**
     * Генерирует случайное значение из перечисления
     * @param {Array} values - массив возможных значений
     * @param {*} defaultValue - значение по умолчанию
     * @param {number} defaultRandom - вероятность использования значения по умолчанию (0-1)
     * @returns {*} случайное значение из массива
     */
    _generateRandomEnumValue(values, defaultValue = null, defaultRandom = 0) {
        if (!Array.isArray(values) || values.length === 0) {
            throw new Error('Массив значений для enum не может быть пустым');
        }
        
        // Проверяем, нужно ли использовать значение по умолчанию
        if (defaultValue !== null && Math.random() < defaultRandom) {
            return defaultValue;
        }
        
        return values[Math.floor(Math.random() * values.length)];
    }

    /**
     * Генерирует случайное булево значение
     * @param {boolean} defaultValue - значение по умолчанию
     * @param {number} defaultRandom - вероятность использования значения по умолчанию (0-1)
     * @returns {boolean} случайное булево значение
     */
    _generateRandomBoolean(defaultValue = null, defaultRandom = 0) {
        // Проверяем, нужно ли использовать значение по умолчанию
        if (defaultValue !== null && Math.random() < defaultRandom) {
            return defaultValue;
        }
        
        return Math.random() < 0.5;
    }

    /**
     * Генерирует значение поля на основе конфигурации генератора
     * @param {Object} generatorConfig - конфигурация генератора
     * @returns {*} сгенерированное значение
     */
    _generateFieldValue(generatorConfig) {
        if (!generatorConfig || !generatorConfig.type) {
            // Значение по умолчанию: string с min=6, max=20
            return this._generateRandomString(6, 20);
        }

        // Извлекаем параметры default_value и default_random
        const defaultValue = generatorConfig.default_value !== undefined ? generatorConfig.default_value : null;
        const defaultRandom = generatorConfig.default_random !== undefined ? generatorConfig.default_random : 0;

        switch (generatorConfig.type) {
            case 'string':
                const minLength = generatorConfig.min !== undefined ? generatorConfig.min : 6;
                const maxLength = generatorConfig.max !== undefined ? generatorConfig.max : 20;
                return this._generateRandomString(minLength, maxLength, defaultValue, defaultRandom);

            case 'integer':
                const minInt = generatorConfig.min !== undefined ? generatorConfig.min : 0;
                const maxInt = generatorConfig.max !== undefined ? generatorConfig.max : 100;
                return this._generateRandomInteger(minInt, maxInt, defaultValue, defaultRandom);

            case 'date':
                const minDate = generatorConfig.min !== undefined ? generatorConfig.min : new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
                const maxDate = generatorConfig.max !== undefined ? generatorConfig.max : new Date();
                return this._generateRandomDateFromRange(minDate, maxDate, defaultValue, defaultRandom);

            case 'enum':
                return this._generateRandomEnumValue(generatorConfig.values, defaultValue, defaultRandom);

            case 'objectId':
                // Проверяем, нужно ли использовать значение по умолчанию
                if (defaultValue !== null && Math.random() < defaultRandom) {
                    return new ObjectId(defaultValue);
                }
                return this._generateGuid();

            case 'boolean':
                return this._generateRandomBoolean(defaultValue, defaultRandom);

            default:
                // Fallback к значению по умолчанию
                return this._generateRandomString(6, 20);
        }
    }

    /**
     * Генерирует строку заданной длины для заполнения поля z
     * @param {number} length - требуемая длина строки
     * @returns {string} строка заполнения
     */
    _generatePaddingString(length) {
        if (length <= 0) return '';
        
        return this._generateRandomString(length, length);
    }

    /**
     * Генерирует случайный UUID v4 используя библиотечную функцию
     * @returns {string} UUID строка
     */
    _generateGuid() {
        const guid = new ObjectId();
        // Так как монотонно возрастающий ObjectId не подходит для GUID, нужно преобразовать его случайным образом
        // Нужно поменять местами последние 8 байт строкового представления и первые 8 байт, оставив середину неизменной
        const guidString = guid.toString();
        const first8Bytes = guidString.substring(0, 8);
        const last8Bytes = guidString.substring(16, 24);
        const newGuidString = last8Bytes + guidString.substring(8, 16) + first8Bytes;
        return new ObjectId(newGuidString);
    }

    /**
     * Генерирует случайную дату в заданном диапазоне
     * @param {Date} fromDate - начальная дата
     * @param {Date} toDate - конечная дата
     * @returns {Date} случайная дата
     */
    _generateRandomDate(fromDate, toDate) {
        const fromTime = fromDate.getTime();
        const toTime = toDate.getTime();
        const randomTime = Math.random() * (toTime - fromTime) + fromTime;
        return new Date(randomTime);
    }

    /**
     * Вычисляет размер JSON объекта в байтах
     * @param {Object} obj - объект для измерения
     * @returns {number} размер в байтах
     */
    _calculateJsonSize(obj) {
        return Buffer.byteLength(JSON.stringify(obj), 'utf8');
    }

    /**
     * Генерирует одно сообщение с указанным типом
     * @param {number} type - тип сообщения (t) - число
     * @returns {Object} объект с данными сообщения
     */
    generateMessage(type) {
        if (!this._availableTypes.includes(type)) {
            const error = new Error(`Тип сообщения "${type}" не найден в конфигурации. Доступные типы сообщений: ${this._availableTypes.join(', ')}`);
            error.code = ERROR_WRONG_MESSAGE_TYPE;
            throw error;
        }
        const message = {
            t: type
        };

        // Создаем объект "d" для группировки полей типа
        const dataFields = {};
        
        // Добавляем поля для данного типа на основе конфигурации
        const fieldsForType = this._typeFieldsMap[type];
        
        fieldsForType.forEach(fieldName => {
            // Получаем конфигурацию генератора для поля
            const generatorConfig = this._fieldGeneratorsMap[fieldName];
            
            // Генерируем значение на основе конфигурации
            dataFields[fieldName] = this._generateFieldValue(generatorConfig);
        });
        
        // Добавляем объект "d" с полями типа в сообщение
        message.d = dataFields;
        
        return message;
    }

    /**
     * Генерирует сообщение случайного типа
     * @returns {Object} объект с данными сообщения
     */
    generateRandomTypeMessage() {
        const randomType = this._availableTypes[Math.floor(Math.random() * this._availableTypes.length)];
        return this.generateMessage(randomType);
    }

    /**
     * Возвращает массив доступных типов сообщений
     * @returns {Array<number>} массив типов сообщений
     */
    getAvailableTypes() {
        return [...this._availableTypes];
    }

}

module.exports = MessageGenerator;
