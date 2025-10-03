const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger');

/**
 * Формат файла factConfigs.json
 * @property {string} src - Имя исходного атрибута
 * @property {string} dst - Имя целевого поля
 * @property {Array<string>} types - Массив типов фактов, для которых применяется маппинг
 */

/**
 * Класс для преобразования входных фактов во внутреннюю сохраняемую структуру
 * 
 * Использует конфигурацию маппинга из файла factConfigs.json для преобразования
 * полей входного факта в поля внутренней структуры согласно типам фактов.
 * 
 * Структура конфигурации маппинга:
 * @property {string} src - Имя исходного атрибута
 * @property {string} dst - Имя целевого поля
 * @property {Array<string>} types - Массив типов фактов, для которых применяется маппинг
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
        } else {
            // Инициализация через путь к файлу (по умолчанию)
            const configPath = configPathOrMapArray || path.join(process.cwd(), 'factConfigs.json');
            this._loadConfig(configPath);
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
            this._mappingConfig = mappingConfig;

            this.logger.info(`Загружена конфигурация маппинга из ${configPath}`);
            this.logger.info(`Количество правил маппинга: ${this._mappingConfig.length}`);
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации: ${error.message}`);
            throw error;
        }
    }

    /**
     * Валидирует структуру загруженной конфигурации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateConfig(mappingConfig) {
        if (!Array.isArray(mappingConfig)) {
            throw new Error('Конфигурация должна быть массивом объектов');
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

            if (!Array.isArray(rule.types) || rule.types.length === 0) {
                throw new Error(`Правило маппинга ${i} должно содержать непустой массив 'types'`);
            }

            for (let j = 0; j < rule.types.length; j++) {
                if (typeof rule.types[j] !== 'string') {
                    throw new Error(`Правило маппинга ${i}, тип ${j} должен быть строкой`);
                }
            }
        }
    }

    /**
     * Преобразует входной факт во внутреннюю сохраняемую структуру
     * @param {Object} inputFact - Входной факт для преобразования
     * @param {string} factType - Тип факта для определения применимых правил маппинга
     * @returns {Object} Преобразованный факт во внутренней структуре
     * @throws {Error} если входной факт невалиден или тип факта не поддерживается
     */
    mapFact(inputFact, factType, keepUnmappedFields = true) {
        if (!inputFact || typeof inputFact !== 'object') {
            throw new Error('Входной факт должен быть объектом');
        }

        if (!factType || typeof factType !== 'string') {
            throw new Error('Тип факта должен быть строкой');
        }

        // Находим правила маппинга, применимые для данного типа факта
        const applicableRules = this.getMappingRulesForType(factType);

        if (applicableRules.length === 0) {
            this.logger.warn(`Не найдено правил маппинга для типа факта: ${factType}`);
            return inputFact; // Возвращаем исходный факт без изменений
        }

        // Создаем результирующий объект
        const mappedFact = {};

        // Применяем правила маппинга
        applicableRules.forEach(rule => {
            if (rule.src in inputFact) {
                // Если исходное поле существует, копируем его значение в целевое поле
                mappedFact[rule.dst] = inputFact[rule.src];
                this.logger.debug(`Применено правило маппинга: ${rule.src} -> ${rule.dst} для типа ${factType}`);
            } else {
                this.logger.debug(`Исходное поле '${rule.src}' не найдено в факте для типа ${factType}`);
            }
        });

        // Если keepUnmappedFields=true, добавляем поля, которые не участвуют в маппинге
        if (keepUnmappedFields) {
            const mappedFields = new Set(applicableRules.map(rule => rule.src));
            Object.keys(inputFact).forEach(field => {
                if (!mappedFields.has(field)) {
                    mappedFact[field] = inputFact[field];
                    this.logger.debug(`Сохранено неотображенное поле: ${field}`);
                }
            });
        }

        return mappedFact;
    }

    /**
     * Преобразует массив входных фактов во внутреннюю структуру
     * @param {Array<Object>} inputFacts - Массив входных фактов
     * @param {string} factType - Тип фактов для определения применимых правил маппинга
     * @returns {Array<Object>} Массив преобразованных фактов
     */
    mapFacts(inputFacts, factType) {
        if (!Array.isArray(inputFacts)) {
            throw new Error('Входные факты должны быть массивом');
        }

        const mappedFacts = [];
        
        inputFacts.forEach((fact, index) => {
            try {
                const mappedFact = this.mapFact(fact, factType);
                mappedFacts.push(mappedFact);
            } catch (error) {
                this.logger.error(`Ошибка маппинга факта ${index}: ${error.message}`);
                // Пропускаем проблемный факт, но продолжаем обработку остальных
            }
        });

        return mappedFacts;
    }


    /**
     * Получает правила маппинга для конкретного типа факта
     * @param {string} factType - Тип факта
     * @returns {Array<Object>} Массив правил маппинга для данного типа
     */
    getMappingRulesForType(factType) {
        return this._mappingConfig.filter(rule => 
            rule.types.includes(factType)
        );
    }

}

module.exports = FactMapper;
