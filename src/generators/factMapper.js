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
 * @property {boolean} unique_key - Если true, то атрибут является уникальным ключом и будет использоваться как _id в факте
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

            if (!Array.isArray(rule.event_types) || rule.event_types.length === 0) {
                this.logger.error(`Правило маппинга ${i} должно содержать непустой массив 'event_types': ${JSON.stringify(rule)}`);
                throw new Error(`Правило маппинга ${i} должно содержать непустой массив 'event_types'`);
            }

            for (let j = 0; j < rule.event_types.length; j++) {
                if (typeof rule.event_types[j] !== 'number' || !Number.isInteger(rule.event_types[j])) {
                    throw new Error(`Правило маппинга ${i}, тип ${j} должен быть целым числом`);
                }
            }
        }
        this.logger.info(`Конфигурация маппинга полей валидна. Количество правил маппинга полей: ${mappingConfig.length}`);
    }

    /**
     * Преобразует входное событие во внутреннюю сохраняемую структуру факта
     * @param {Object} event - Входное событие для преобразования
     * @param {boolean} keepUnmappedFields - Если true, поля, не найденные в правилах маппинга, сохраняются в результате. Если false, такие поля удаляются из результата. По умолчанию true.
     * @returns {Object} Преобразованный факт во внутренней структуре
     */
    mapEventToFact(event, keepUnmappedFields=true){
        const fact = {
            _id: null,
            t: event.t,
            c: new Date(), // дата и время создания объекта
            d: this.mapEventData(event.d, event.t, keepUnmappedFields),
        };
        // Находим имя поле с уникальным идентификатором события и устанавливаем его значение в поле _id
        this._mappingConfig.forEach(rule => {
            if (rule.unique_key) {
                fact._id = event.d[rule.src];
            }
        });
        if (fact._id === null) {
            throw new Error(`В описании полей события с типом ${event.t} не указан уникальный ключ (поле с атрибутом unique_key: true). \n${JSON.stringify(this._mappingConfig)}`);
        }
        return fact;
    }

    /**
     * Преобразует входное событие во внутреннюю сохраняемую структуру факта
     * @param {Object} eventData - Входное событие для преобразования
     * @param {string} eventType - Тип события для определения применимых правил маппинга
     * @returns {Object} Преобразованные данные факта во внутренней структуре
     * @throws {Error} если входное событие невалидно или тип события не поддерживается
     */
    mapEventData(eventData, eventType, keepUnmappedFields = true) {
        if (!eventData || typeof eventData !== 'object') {
            throw new Error('Входные данные события должны быть объектом');
        }

        if (!eventType || typeof eventType !== 'number') {
            throw new Error(`Тип события должен быть целым числом, а не ${typeof eventType}. Переданное значение: ${eventType}`);
        }

        // Находим правила маппинга, применимые для данного типа события
        const applicableRules = this.getMappingRulesForType(eventType);

        if (applicableRules.length === 0) {
            this.logger.warn(`Не найдено правил маппинга для типа события: ${eventType}`);
            return eventData; // Возвращаем исходное событие без изменений
        }

        // Создаем результирующий объект
        const factData = {};

        // Применяем правила маппинга
        applicableRules.forEach(rule => {
            if (rule.src in eventData) {
                // Если исходное поле существует, копируем его значение в целевое поле
                factData[rule.dst] = eventData[rule.src];
                this.logger.debug(`Применено правило маппинга: ${rule.src} -> ${rule.dst} для типа ${eventType}`);
            } else {
                this.logger.debug(`Исходное поле '${rule.src}' не найдено в факте для типа ${eventType}`);
            }
        });

        // Если keepUnmappedFields=true, добавляем поля, которые не участвуют в маппинге
        if (keepUnmappedFields) {
            const mappedFields = new Set(applicableRules.map(rule => rule.src));
            Object.keys(eventData).forEach(field => {
                if (!mappedFields.has(field)) {
                    factData[field] = eventData[field];
                    this.logger.debug(`Сохранено неотображенное поле: ${field}`);
                }
            });
        }

        return factData;
    }



    /**
     * Получает правила маппинга для конкретного типа события
     * @param {string} eventType - Тип события
     * @returns {Array<Object>} Массив правил маппинга для данного типа
     */
    getMappingRulesForType(eventType) {
        return this._mappingConfig.filter(rule => 
            rule.event_types.includes(eventType)
        );
    }

}

module.exports = FactMapper;
