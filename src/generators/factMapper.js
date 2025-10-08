const crypto = require('crypto');
const fs = require('fs');
// const path = require('path');
const Logger = require('../utils/logger');

/**
 * Формат файла fieldConfigs.json
 * @property {string} src - Имя атрибута события
 * @property {string} dst - Имя атрибута данных факта
 * @property {Array<integer>} types - Массив типов событий (целое число), для которых применяется маппинг
 * @property {Object} generator - Конфигурация генератора значений для атрибута события
 * @property {string} generator.type - Тип генератора значений
 * @property {Object} generator.params - Параметры генератора значений
 * @property {number} key_type - Если 1 или 2, то атрибут является уникальным ключом и будет использоваться как _id в факте
 */

/**
 * Класс для преобразования входных событий во внутреннюю сохраняемую структуру факта
 * 
 * Использует конфигурацию маппинга из файла fieldConfigs.json для преобразования
 * полей входного события в поля данных факта согласно типам событий.
 * 
 * Структура факта (fact):
 * @property {string} _id - Идентификатор факта из уникального атрибута события
 * @property {number} t - Тип события (число)
 * @property {Date} c - Дата и время создания факта в базе данных
 * @property {object} d - данные факта
 * 
 */
class FactMapper {
    KEY_TYPE_NONE = 0;  // Поле не является ключом (значение по умолчанию, если отсутствует)
    KEY_TYPE_HASH = 1;  // Тип ключа факта является хешом от типа и значения поля
    KEY_TYPE_VALUE = 2; // Тип ключа факта является конкатенацией типа и значения поля
    HASH_ALGORITHM = 'sha1';    // Алгоритм хеширования

    constructor(configPathOrMapArray = null) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this._mappingConfig = [];
        
        if (!configPathOrMapArray) {
            this.logger.info('Конфигурация не задана. Маппинг не будет производиться.');
            return;
        }
        // Определяем способ инициализации
        if (Array.isArray(configPathOrMapArray)) {
            // Инициализация через массив конфигурации
            this._validateConfig(configPathOrMapArray);
            this._mappingConfig = configPathOrMapArray;
            this.logger.info(`Инициализирован с массивом конфигурации. Количество правил: ${this._mappingConfig.length}`);
        } else if (typeof configPathOrMapArray === 'string') {
            // Инициализация через путь к файлу (по умолчанию)
            const configPath = configPathOrMapArray;
            this._mappingConfig = this._loadConfig(configPath);
        } else {
            this.logger.info('Конфигурация не задана. Маппинг не будет производиться.');
            return;
        }
    }

    /**
     * Загружает конфигурацию маппинга из файла
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     */
    _loadConfig(configPath) {
        try {
            if (!fs.existsSync(configPath)) {
                throw new Error(`Файл конфигурации не найден: ${configPath}`);
            }

            const configData = fs.readFileSync(configPath, 'utf8');
            const mappingConfig = JSON.parse(configData);

            // Валидация структуры конфигурации
            this._validateConfig(mappingConfig);

            this.logger.info(`Загружена конфигурация маппинга полей из ${configPath}`);
            this.logger.info(`Количество правил маппинга: ${mappingConfig.length}`);
            return mappingConfig;
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации маппинга полей: ${error.message}`);
            throw error;
        }
    }

    /**
     * Валидирует структуру загруженной конфигурации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateConfig(mappingConfig) {
        if (!Array.isArray(mappingConfig)) {
            throw new Error('Конфигурация маппинга полей должна быть массивом объектов');
        }

        for (let i = 0; i < mappingConfig.length; i++) {
            const rule = mappingConfig[i];
            
            if (!rule || typeof rule !== 'object') {
                throw new Error(`Правило маппинга ${i} должно быть объектом`);
            }

            if (!rule.src || typeof rule.src !== 'string') {
                throw new Error(`Правило маппинга ${i} должно содержать поле 'src' типа string`);
            }

            if (!rule.dst || typeof rule.dst !== 'string') {
                throw new Error(`Правило маппинга ${i} должно содержать поле 'dst' типа string`);
            }

            if (!Array.isArray(rule.message_types) || rule.message_types.length === 0) {
                this.logger.error(`Правило маппинга ${i} должно содержать непустой массив 'message_types': ${JSON.stringify(rule)}`);
                throw new Error(`Правило маппинга ${i} должно содержать непустой массив 'message_types'`);
            }

            for (let j = 0; j < rule.message_types.length; j++) {
                if (typeof rule.message_types[j] !== 'number' || !Number.isInteger(rule.message_types[j])) {
                    throw new Error(`Правило маппинга ${i}, тип ${j} должен быть целым числом`);
                }
            }
        }
        this.logger.info(`Конфигурация маппинга полей валидна. Количество правил маппинга полей: ${mappingConfig.length}`);
    }

    /**
     * Хеш функция для создания уникального индекса из типа и значения поля
     * @param {number|string} factType - тип факта
     * @param {string} factValue - значение факта
     * @returns {string} SHA-256 хеш в hex формате
     */
    _hash(factType, keyValue) {
        const input = `${factType}:${keyValue}`;
        return crypto.createHash(this.HASH_ALGORITHM).update(input).digest('hex');
    }

    /**
     * Получение идентификатора факта
     * 
     */
    getFactId(message) {
        let factId = null;
        this._mappingConfig.filter(rule => rule.message_types.includes(message.t)).forEach(rule => {
            if (rule.key_type === this.KEY_TYPE_HASH) {
                factId = this._hash(message.t, message.d[rule.src]);
            } else if (rule.key_type === this.KEY_TYPE_VALUE) {
                factId = `${message.t}:${String(message.d[rule.src])}`;
            }
        });
        return factId;
    }

    /**
     * Преобразует входное сообщение во внутреннюю сохраняемую структуру факта
     * @param {Object} message - Входное сообщение для преобразования
     * @param {boolean} keepUnmappedFields - Если true, поля, не найденные в правилах маппинга, сохраняются в результате. Если false, такие поля удаляются из результата. По умолчанию true.
     * @returns {Object} Преобразованный факт во внутренней структуре
     */
    mapMessageToFact(message, keepUnmappedFields=true){
        const fact = {
            _id: this.getFactId(message),
            t: message.t,
            c: new Date(), // дата и время создания объекта
            d: this.mapMessageData(message.d, message.t, keepUnmappedFields),
        };
        if (fact._id === null) {
            throw new Error(`В описании полей сообщения с типом ${message.t} не указан ключ (поле с атрибутом key_type: 1 или key_type: 2). \n${JSON.stringify(this._mappingConfig)}`);
        }
        return fact;
    }

    /**
     * Преобразует входное сообщение во внутреннюю сохраняемую структуру факта
     * @param {Object} messageData - Входное сообщение для преобразования
     * @param {string} messageType - Тип сообщения для определения применимых правил маппинга
     * @returns {Object} Преобразованные данные факта во внутренней структуре
     * @throws {Error} если входное сообщение невалидно или тип сообщения не поддерживается
     */
    mapMessageData(messageData, messageType, keepUnmappedFields = true) {
        if (!messageData || typeof messageData !== 'object') {
            throw new Error('Входные данные сообщения должны быть объектом');
        }

        if (!messageType || typeof messageType !== 'number') {
            throw new Error(`Тип сообщения должен быть целым числом, а не ${typeof messageType}. Переданное значение: ${messageType}`);
        }

        // Находим правила маппинга, применимые для данного типа сообщения
        const applicableRules = this.getMappingRulesForType(messageType);

        if (applicableRules.length === 0) {
            this.logger.warn(`Не найдено правил маппинга для типа сообщения: ${messageType}`);
            return messageData; // Возвращаем исходное сообщение без изменений
        }

        // Создаем результирующий объект
        const factData = {};

        // Применяем правила маппинга
        applicableRules.forEach(rule => {
            if (rule.src in messageData) {
                // Если исходное поле существует, копируем его значение в целевое поле
                factData[rule.dst] = messageData[rule.src];
                this.logger.debug(`Применено правило маппинга: ${rule.src} -> ${rule.dst} для типа ${messageType}`);
            } else {
                this.logger.debug(`Исходное поле '${rule.src}' не найдено в факте для типа ${messageType}`);
            }
        });

        // Если keepUnmappedFields=true, добавляем поля, которые не участвуют в маппинге
        if (keepUnmappedFields) {
            const mappedFields = new Set(applicableRules.map(rule => rule.src));
            Object.keys(messageData).forEach(field => {
                if (!mappedFields.has(field)) {
                    factData[field] = messageData[field];
                    this.logger.debug(`Сохранено неотображенное поле: ${field}`);
                }
            });
        }

        return factData;
    }



    /**
     * Получает правила маппинга для конкретного типа сообщения
     * @param {string} messageType - Тип сообщения
     * @returns {Array<Object>} Массив правил маппинга для данного типа
     */
    getMappingRulesForType(messageType) {
        return this._mappingConfig.filter(rule => 
            rule.message_types.includes(messageType)
        );
    }

}

module.exports = FactMapper;
