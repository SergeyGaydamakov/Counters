const { ObjectId } = require('mongodb');
const fs = require('fs');
const path = require('path');

/**
 * Класс для генерации случайных тестовых данных
 * 
 * Структура факта (fact):
 * @property {string} i - Идентификатор факта uuidv4
 * @property {number} t - Тип факта (число)
 * @property {number} a - Количество
 * @property {Date} c - Дата и время создания факта в базе данных
 * @property {Date} d - Дата и время факта
 * @property {string} z - Дополнительное поле для достижения нужного размера JSON в байтах
 * @property {string} f1 - Значение случайногенерированного поля f1
 * @property {string} f2 - Значение случайногенерированного поля f2
 * @property {string} fN - Значение случайногенерированного поля fN
 * 
 */
class FactGenerator {
    constructor(fieldConfigPathOrArray = null, fromDate = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), toDate = new Date(), targetSize = null) {
        // Загружаем конфигурацию полей
        this._fieldConfig = this._loadFieldConfig(fieldConfigPathOrArray);
        
        // Извлекаем информацию из конфигурации
        this._availableFields = this._fieldConfig.map(field => field.src);
        this._availableTypes = this._extractAvailableTypes();
        this._typeFieldsMap = this._buildTypeFieldsMap();
        
        // Сохраняем даты для генерации фактов
        this._fromDate = fromDate;
        this._toDate = toDate;
        this._targetSize = targetSize;
    }

    /**
     * Загружает конфигурацию полей из файла или использует переданную структуру
     * @param {string|Array|null} fieldConfigPathOrArray - путь к файлу конфигурации, массив конфигурации или null для использования fieldConfig.json
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
            const configPath = fieldConfigPathOrArray || path.join(process.cwd(), 'fieldConfig.json');
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

            if (!Array.isArray(field.types) || field.types.length === 0) {
                throw new Error(`Поле конфигурации ${i} должно содержать непустой массив 'types'`);
            }

            for (let j = 0; j < field.types.length; j++) {
                if (typeof field.types[j] !== 'number' || !Number.isInteger(field.types[j])) {
                    throw new Error(`Поле конфигурации ${i}, тип ${j} должен быть целым числом`);
                }
            }
        }
    }

    /**
     * Извлекает все доступные типы из конфигурации полей
     * @returns {Array} массив уникальных типов
     */
    _extractAvailableTypes() {
        const types = new Set();
        this._fieldConfig.forEach(field => {
            field.types.forEach(type => types.add(type));
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
                .filter(field => field.types.includes(type))
                .map(field => field.src);
        });
        return typeFieldsMap;
    }

    /**
     * Генерирует случайную строку из латинских символов
     * @param {number} minLength - минимальная длина строки
     * @param {number} maxLength - максимальная длина строки
     * @returns {string} случайная строка
     */
    _generateRandomString(minLength = 2, maxLength = 20) {
        const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        const length = Math.floor(Math.random() * (maxLength - minLength + 1)) + minLength;
        let result = '';
        
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        
        return result;
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
     * Генерирует один факт с заданными параметрами
     * @param {number} type - тип факта (t) - число
     * @returns {Object} объект с данными факта
     */
    generateFact(type) {
        if (!this._availableTypes.includes(type)) {
            throw new Error(`Тип факта "${type}" не найден в конфигурации. Доступные типы: ${this._availableTypes.join(', ')}`);
        }
        const fact = {
            i: this._generateGuid(),
            t: type,
            a: Math.floor(Math.random() * 1000000) + 1, // от 1 до 1000000
            c: new Date(), // дата и время создания объекта
            d: this._generateRandomDate(this._fromDate, this._toDate)
        };

        // Добавляем поля для данного типа на основе конфигурации
        const fieldsForType = this._typeFieldsMap[type];
        
        fieldsForType.forEach(fieldName => {
            fact[fieldName] = this._generateRandomString(2, 20);
            // Иногда генерируем постоянный идентификатор
            if (Math.random() < 0.1) {
                fact[fieldName] = "1234567890";
            }
        });

        // Если задан целевой размер, добавляем поле z для достижения нужного размера
        if (this._targetSize && this._targetSize > 0) {
            fact.z = ''; // Временное значение для расчета
            
            const currentSize = this._calculateJsonSize(fact);
            
            if (currentSize < this._targetSize) {
                // Вычисляем нужную длину строки для поля z
                // Учитываем что поле z уже добавлено как пустая строка: ,"z":""
                // Нужно добавить символы только в значение строки z
                const additionalCharsNeeded = this._targetSize - currentSize;
                
                if (additionalCharsNeeded > 0) {
                    fact.z = this._generatePaddingString(additionalCharsNeeded);
                    
                    // Проверяем финальный размер и корректируем если нужно
                    const finalSize = this._calculateJsonSize(fact);
                    if (finalSize > this._targetSize) {
                        // Если превысили, уменьшаем длину строки z
                        const excess = finalSize - this._targetSize;
                        const newLength = Math.max(0, fact.z.length - excess);
                        fact.z = fact.z.substring(0, newLength);
                    }
                }
            } else {
                // Если уже превышаем целевой размер, оставляем пустое поле z
                fact.z = '';
            }
        }

        return fact;
    }

    /**
     * Генерирует факт случайного типа
     * @returns {Object} объект с данными факта
     */
    generateRandomTypeFact() {
        const randomType = this._availableTypes[Math.floor(Math.random() * this._availableTypes.length)];
        return this.generateFact(randomType);
    }

}

module.exports = FactGenerator;
