const crypto = require('crypto');
const Logger = require('../utils/logger');

/**
 * Класс для создания индексных значений из фактов
 * 
 * Структура индексного значения (factIndex):
 * @property {number} it - Номер индексируемого поля (из названия поля fN)
 * @property {string} f - Значение индексируемого поля
 * @property {string} i - Идентификатор факта
 * @property {number} t - Тип факта
 * @property {Date} d - Дата факта
 * @property {Date} c - Дата создания факта в базе данных
 * 
 */
class FactIndexer {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        // Регулярное выражение для поиска полей, начинающихся с 'f' и содержащих число
        this.fieldPattern = /^f(\d+)$/;
        // Выводим хеши по умолчанию для всех полей f1-f23
        for (let i = 1; i <= 23; i++) {
            const hash = this.hash(`f${i}`, "1234567890");
            this.logger.info(`*** Хеш f${i}: <${hash}>`);
        }
        this.logger.info("");
    }

    /**
     * Хеш функция для создания уникального индекса из типа и значения поля
     * @param {number|string} indexType - тип/номер поля (it)
     * @param {string} indexValue - значение поля (f)
     * @returns {string} SHA-256 хеш в hex формате
     */
    hash(indexType, indexValue) {
        const input = `${indexType}:${indexValue}`;
        return crypto.createHash('sha256').update(input).digest('hex');
    }

    /**
     * Создает массив индексных значений из JSON факта
     * @param {Object} fact - JSON объект факта
     * @returns {Array<Object>} массив объектов-индексных значений
     */
    index(fact) {
        if (!fact || typeof fact !== 'object') {
            throw new Error('Факт должен быть объектом');
        }

        // Проверяем наличие обязательных полей
        const requiredFields = ['t', 'i', 'd', 'c'];
        for (const field of requiredFields) {
            if (!(field in fact)) {
                throw new Error(`Отсутствует обязательное поле: ${field}`);
            }
        }

        const indexValues = [];
        
        // Находим все поля, которые начинаются с 'f' и содержат число
        const factFields = Object.keys(fact).filter(key => this.fieldPattern.test(key));
        
        // Если нет полей fN, возвращаем пустой массив
        if (factFields.length === 0) {
            return indexValues;
        } 
        // Создаем индексное значение для каждого поля fN
        factFields.forEach(fieldName => {
            const match = fieldName.match(this.fieldPattern);
            if (match) {
                const fieldNumber = parseInt(match[1], 10); // извлекаем номер из fN
                indexValues.push({
                    it: fieldNumber,    // номер из названия поля (1, 2, 5, 10, etc.)
                    f: fact[fieldName], // сохраняем значение поля (f1, f2, etc.)
                    h: this.hash(fieldName, fact[fieldName]), // SHA-256 хеш для уникального индекса
                    i: fact.i,
                    d: fact.d,
                    c: fact.c
                });
            }
        });

        return indexValues;
    }

    /**
     * Создает индексные значения для массива фактов
     * @param {Array<Object>} facts - массив JSON объектов фактов
     * @returns {Array<Object>} массив всех индексных значений
     */
    indexFacts(facts) {
        if (!Array.isArray(facts)) {
            throw new Error('Факты должны быть массивом');
        }

        const allIndexValues = [];
        
        facts.forEach((fact, index) => {
            try {
                const factIndexValues = this.index(fact);
                allIndexValues.push(...factIndexValues);
            } catch (error) {
                console.warn(`Ошибка при создании индексных значений для факта ${index}: ${error.message}`);
            }
        });

        return allIndexValues;
    }

}

module.exports = FactIndexer;
