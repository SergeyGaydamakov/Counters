const { v4: uuidv4 } = require('uuid');
const { ObjectId } = require('mongodb');

/**
 * Класс для генерации случайных тестовых данных
 * 
 * Структура факта (fact):
 * @property {string} i - Идентификатор факта uuidv4
 * @property {number} t - Тип факта
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
    constructor(_fieldCount = 23, _typeCount = 5, _fieldsPerType = 10, _typeFieldsConfig = null, fromDate = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), toDate = new Date(), targetSize = null) {
        // Массив доступных полей
        this.availableFields = [];
        if (_typeFieldsConfig) {
            // Передана заданная конфигурация полей для типов
            this.typeFieldsConfig = _typeFieldsConfig;
            this.typeCount = Object.keys(this.typeFieldsConfig).length;
            this.fieldsPerType = this.typeFieldsConfig[1].length;
            // Вычисляем количество уникальных полей в конфигурации
            this.fieldCount = new Set(Object.values(this.typeFieldsConfig).flat()).size;
            // Массив доступных полей от 1 до this.fieldCount
            for (let i = 1; i <= this.fieldCount; i++) {
                this.availableFields.push(`f${i}`);
            }
        } else {
            this.typeCount = _typeCount;
            this.fieldsPerType = _fieldsPerType;
            this.fieldCount = _fieldCount;
            for (let i = 1; i <= this.fieldCount; i++) {
                this.availableFields.push(`f${i}`);
            }
            // Генерация заданного количества наборов случайных полей
            this.typeFieldsConfig = this._generateTypeFieldsConfig();
        }
        
        // Сохраняем даты для генерации фактов
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.targetSize = targetSize;
    }

    // Генерация случайной конфигурации полей для типов
    _generateTypeFieldsConfig() {
        const config = {};
        for (let type = 1; type <= this.typeCount; type++) {
            // Перемешиваем доступные поля и берем первые fieldsPerType элементов
            const shuffled = [...this.availableFields].sort(() => 0.5 - Math.random());
            config[type] = shuffled.slice(0, this.fieldsPerType).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
        }
        return config;
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
     * @param {number} type - тип факта (t)
     * @returns {Object} объект с данными факта
     */
    generateFact(type) {
        if (type < 1 || type > this.typeCount) {
            throw new Error(`Тип факта должен быть в диапазоне от 1 до ${this.typeCount}`);
        }
        const fact = {
            i: this._generateGuid(),
            t: type,
            a: Math.floor(Math.random() * 1000000) + 1, // от 1 до 1000000
            c: new Date(), // дата и время создания объекта
            d: this._generateRandomDate(this.fromDate, this.toDate)
        };

        // Добавляем 10 случайных полей для данного типа
        const randomFields = this.typeFieldsConfig[type];
        
        randomFields.forEach(fieldName => {
            fact[fieldName] = this._generateRandomString(2, 20);
            // Иногда генерируем постоянный идентификатор
            if (Math.random() < 0.1) {
                fact[fieldName] = "1234567890";
            }
        });

        // Если задан целевой размер, добавляем поле z для достижения нужного размера
        if (this.targetSize && this.targetSize > 0) {
            fact.z = ''; // Временное значение для расчета
            
            const currentSize = this._calculateJsonSize(fact);
            
            if (currentSize < this.targetSize) {
                // Вычисляем нужную длину строки для поля z
                // Учитываем что поле z уже добавлено как пустая строка: ,"z":""
                // Нужно добавить символы только в значение строки z
                const additionalCharsNeeded = this.targetSize - currentSize;
                
                if (additionalCharsNeeded > 0) {
                    fact.z = this._generatePaddingString(additionalCharsNeeded);
                    
                    // Проверяем финальный размер и корректируем если нужно
                    const finalSize = this._calculateJsonSize(fact);
                    if (finalSize > this.targetSize) {
                        // Если превысили, уменьшаем длину строки z
                        const excess = finalSize - this.targetSize;
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
        const randomType = Math.floor(Math.random() * this.typeCount) + 1;
        return this.generateFact(randomType);
    }

}

module.exports = FactGenerator;
