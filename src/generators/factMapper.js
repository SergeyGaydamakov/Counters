const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger');
const { ERROR_WRONG_MESSAGE_STRUCTURE, ERROR_MISSING_KEY_IN_MESSAGE, ERROR_MISSING_KEY_IN_CONFIG, ERROR_WRONG_KEY_TYPE } = require('../common/errors');
const FieldNameMapper = require('./fieldNameMapper');

/**
 * Формат файла fieldConfigs.json
 * @property {string} src - Имя атрибута события
 * @property {string} dst - Имя атрибута данных факта
 * @property {Array<integer>} types - Массив типов событий (целое число), для которых применяется маппинг
 * @property {Object} generator - Конфигурация генератора значений для атрибута события
 * @property {string} generator.type - Тип генератора значений
 * @property {Object} generator.params - Параметры генератора значений
 * @property {number} key_order - Если указан, то атрибут является частью составного ключа для генерации _id факта. Порядок определяется значением (1, 2, 3...)
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
    HASH_ALGORITHM = 'sha1';    // Алгоритм хеширования

    constructor(configPathOrMapArray = null, useShortNames = false) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this._mappingConfig = [];
        this._mappingRulesCache = new Map(); // Кеш для правил маппинга по типам сообщений
        this._useShortNames = useShortNames;
        
        if (!configPathOrMapArray) {
            this.logger.info('Конфигурация не задана. Маппинг не будет производиться.');
            this.fieldNameMapper = new FieldNameMapper([], useShortNames);
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
            this.fieldNameMapper = new FieldNameMapper([], useShortNames);
            return;
        }
        
        // Инициализируем FieldNameMapper после загрузки конфигурации
        this.fieldNameMapper = new FieldNameMapper(this._mappingConfig, useShortNames);
        if (useShortNames) {
            this.logger.info('Используются короткие имена полей (shortDst)');
        }
    }

    /**
     * Ищет файл в нескольких директориях
     * @param {string} filename - имя файла для поиска
     * @param {Array<string>} searchPaths - массив путей для поиска
     * @returns {string|null} путь к найденному файлу или null
     */
    _findFileInPaths(filename, searchPaths) {
        for (const searchPath of searchPaths) {
            const fullPath = path.join(searchPath, filename);
            if (fs.existsSync(fullPath)) {
                return fullPath;
            }
        }
        return null;
    }

    /**
     * Загружает конфигурацию маппинга из файла
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     */
    _loadConfig(configPath) {
        try {
            // Если передан только имя файла, ищем его в стандартных директориях
            if (!path.isAbsolute(configPath) && !configPath.includes(path.sep)) {
                const searchPaths = [
                    process.cwd(),                    // Текущая директория
                    path.join(process.cwd(), '..'),   // Родительская директория
                    path.join(process.cwd(), '..', '..'), // Директория на 2 уровня выше
                    path.join(__dirname, '..', '..'), // Корень проекта относительно src/generators
                ];
                
                const foundPath = this._findFileInPaths(configPath, searchPaths);
                if (foundPath) {
                    configPath = foundPath;
                    this.logger.info(`Файл ${configPath} найден в директории: ${path.dirname(foundPath)}`);
                }
            }

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

        // Проверяем уникальность комбинаций src + dst только при пересечении message_types
        const duplicateCombinations = [];
        
        // Проверяем конфликтующие dst поля только при пересечении message_types
        const conflictingDstFields = [];
        
        // Проверяем конфликтующие shortDst поля только при пересечении message_types
        const conflictingShortDstFields = [];

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

        // Проверяем дублирующиеся комбинации src->dst только при пересечении message_types
        for (let i = 0; i < mappingConfig.length; i++) {
            for (let j = i + 1; j < mappingConfig.length; j++) {
                const rule1 = mappingConfig[i];
                const rule2 = mappingConfig[j];
                
                // Проверяем, есть ли пересечение в message_types
                const hasIntersection = rule1.message_types.some(type => rule2.message_types.includes(type));
                
                if (hasIntersection) {
                    // Проверяем дублирующиеся комбинации src->dst
                    if (rule1.src === rule2.src && rule1.dst === rule2.dst) {
                        duplicateCombinations.push({ 
                            index1: i, 
                            index2: j, 
                            src: rule1.src, 
                            dst: rule1.dst,
                            commonTypes: rule1.message_types.filter(type => rule2.message_types.includes(type))
                        });
                    }
                    
                    // Проверяем конфликтующие dst поля (разные src маппятся на одно dst)
                    if (rule1.src !== rule2.src && rule1.dst === rule2.dst) {
                        conflictingDstFields.push({
                            dst: rule1.dst,
                            src1: rule1.src,
                            src2: rule2.src,
                            index1: i,
                            index2: j,
                            commonTypes: rule1.message_types.filter(type => rule2.message_types.includes(type))
                        });
                    }
                    
                    // Проверяем конфликтующие shortDst поля (разные dst маппятся на одно shortDst)
                    if (rule1.shortDst && rule2.shortDst && 
                        rule1.shortDst === rule2.shortDst && 
                        rule1.dst !== rule2.dst) {
                        conflictingShortDstFields.push({
                            shortDst: rule1.shortDst,
                            dst1: rule1.dst,
                            dst2: rule2.dst,
                            src1: rule1.src,
                            src2: rule2.src,
                            index1: i,
                            index2: j,
                            commonTypes: rule1.message_types.filter(type => rule2.message_types.includes(type))
                        });
                    }
                }
            }
        }

        // Если найдены дублирующиеся комбинации, выбрасываем ошибку
        if (duplicateCombinations.length > 0) {
            const duplicatesInfo = duplicateCombinations.map(dup => 
                `правила ${dup.index1}, ${dup.index2}: ${dup.src}->${dup.dst} (пересекающиеся типы: [${dup.commonTypes.join(', ')}])`
            ).join(', ');
            throw new Error(`Найдены дублирующиеся комбинации src->dst при пересечении message_types: ${duplicatesInfo}. Каждая комбинация src->dst должна быть уникальной для пересекающихся типов сообщений.`);
        }
        
        // Если найдены конфликтующие dst поля, выбрасываем ошибку
        if (conflictingDstFields.length > 0) {
            const conflictsInfo = conflictingDstFields.map(conflict => 
                `поле ${conflict.dst}: ${conflict.src1} (правило ${conflict.index1}) и ${conflict.src2} (правило ${conflict.index2}) (пересекающиеся типы: [${conflict.commonTypes.join(', ')}])`
            ).join(', ');
            throw new Error(`Найдены конфликтующие dst поля при пересечении message_types: ${conflictsInfo}. Разные src поля не могут маппиться на одно dst поле для пересекающихся типов сообщений.`);
        }
        
        // Если найдены конфликтующие shortDst поля, выбрасываем ошибку
        if (conflictingShortDstFields.length > 0) {
            const conflictsInfo = conflictingShortDstFields.map(conflict => 
                `поле ${conflict.shortDst}: ${conflict.dst1}->${conflict.src1} (правило ${conflict.index1}) и ${conflict.dst2}->${conflict.src2} (правило ${conflict.index2}) (пересекающиеся типы: [${conflict.commonTypes.join(', ')}])`
            ).join(', ');
            throw new Error(`Найдены конфликтующие shortDst поля при пересечении message_types: ${conflictsInfo}. Разные dst поля не могут маппиться на одно shortDst поле для пересекающихся типов сообщений.`);
        }

        this.logger.info(`Конфигурация маппинга полей валидна. Количество правил маппинга полей: ${mappingConfig.length}`);
    }

    /**
     * Конвертирует значение в указанный тип
     * @param {any} value - значение для конвертации
     * @param {string} targetType - целевой тип (string, integer, float, date, enum, objectId, boolean)
     * @returns {any} конвертированное значение
     */
    _convertValueToType(value, targetType, srcName, dstName) {
        if (value === undefined) {
            return value;
        }
        if (value === null) {
            return null;
        }

        switch (targetType) {
            case 'string':
            case 'enum': // enum эквивалентен string
                return String(value);
            
            case 'integer':
                const intValue = parseInt(value, 10);
                if (isNaN(intValue)) {
                    this.logger.warn(`Не удалось конвертировать значение '${value}' в integer, используется 0 для поля "${srcName}" -> "${dstName}"`);
                    return 0;
                }
                return intValue;
            
            case 'float':
                const floatValue = parseFloat(value);
                if (isNaN(floatValue) || !isFinite(floatValue)) {
                    this.logger.warn(`Не удалось конвертировать значение '${value}' в float, используется 0.0 для поля "${srcName}" -> "${dstName}"`);
                    return 0.0;
                }
                return floatValue;
            
            case 'date':
                if (value instanceof Date) {
                    return value;
                }
                const dateValue = new Date(value);
                if (isNaN(dateValue.getTime())) {
                    this.logger.warn(`Не удалось конвертировать значение '${value}' в date, используется текущая дата для поля "${srcName}" -> "${dstName}"`);
                    return new Date();
                }
                return dateValue;
            
            case 'objectId':
                // Для objectId просто возвращаем строковое представление
                return String(value);
            
            case 'boolean':
                if (typeof value === 'boolean') {
                    return value;
                }
                if (typeof value === 'string') {
                    const lowerValue = value.toLowerCase();
                    if (lowerValue === 'true' || lowerValue === '1' || lowerValue === 'yes') {
                        return true;
                    }
                    if (lowerValue === 'false' || lowerValue === '0' || lowerValue === 'no') {
                        return false;
                    }
                }
                if (typeof value === 'number') {
                    return value !== 0;
                }
                // Для всех остальных случаев возвращаем false
                return false;
            
            default:
                this.logger.warn(`Неизвестный тип '${targetType}', используется исходное значение для поля "${srcName}" -> "${dstName}"`);
                return value;
        }
    }

    /**
     * Хеш функция для создания уникального индекса из типа и значения поля
     * @param {number|string} factType - тип факта
     * @param {string} factValue - значение факта
     * @returns {string} SHA-256 хеш в hex формате
     */
    _hashHex(factType, keyValues) {
        const input = `${factType}:${keyValues.map(value => String(value).trim()).join(':')}`;
        return crypto.createHash(this.HASH_ALGORITHM).update(input).digest('hex');
    }

    _hashBase64(factType, keyValues) {
        const input = `${factType}:${keyValues.map(value => String(value).trim()).filter(value => !!value).join(':')}`;
        return crypto.createHash(this.HASH_ALGORITHM).update(input).digest('base64');
    }

    /**
     * Валидация структуры сообщения
     */
    _validateMessage(message) {
        if (!message || typeof message !== 'object') {
            const error = new Error('Входное сообщение должно быть объектом. Полный объект: ' + message);
            error.code = ERROR_WRONG_MESSAGE_STRUCTURE;
            throw error;
        }
        if (!message.t || typeof message.t !== 'number') {
            const error = new Error('Тип сообщения должен быть целым числом. Полученное значение: ' + message.t);
            error.code = ERROR_WRONG_MESSAGE_STRUCTURE;
            throw error;
        }
        if (!message.d || typeof message.d !== 'object') {
            const error = new Error('Данные сообщения должны быть объектом. Полученное значение: ' + message.d);
            error.code = ERROR_WRONG_MESSAGE_STRUCTURE;
            throw error;
        }
        return true;
    }

    /**
     * Получение идентификатора факта
     * 
     */
    getFactId(message) {
        // Нужно найти все поля идентификатора факта в конфигурации для данного типа сообщения и отсортировать 
        const keyRules = this._mappingConfig.filter(rule => rule.message_types.includes(message.t) && rule.key_order).sort((a, b) => a.key_order - b.key_order);
        if (!keyRules.length) {
            const error = new Error(`В конфигурации полей сообщения для типа ${message.t} не найдены поля для создания идентификатора факта (описание поля с атрибутом key_order). Маппинг не будет выполняться. Возможно указан неверный файл конфигурации.`);
            this.logger.error(error.message);
            this.logger.error("Возможноые поля сообщения:"+keyRules.map(rule => rule.src).join(', '));
            error.code = ERROR_MISSING_KEY_IN_CONFIG;
            throw error;
        }
        // Нужно найти в сообщении все ключевые поля, должно быть хотя бы одно поле, необязательно наличие всех полей.
        const keyFieldValues = keyRules.map(rule => message.d[rule.src]).filter(field => field !== undefined && field !== null);
        if (!keyFieldValues.length) {
            const error = new Error(`В сообщении типа ${message.t} не найдено ни одного ключевого поля: ${keyRules.map(rule => rule.src).join(', ')}.`);
            error.code = ERROR_MISSING_KEY_IN_MESSAGE;
            throw error;
        }
        // Получаем идентификатор факта
        return this._hashBase64(message.t, keyFieldValues);
    }

    /**
     * Преобразует входное сообщение во внутреннюю сохраняемую структуру факта
     * @param {Object} message - Входное сообщение для преобразования
     * @param {boolean} keepUnmappedFields - Если true, поля, не найденные в правилах маппинга, сохраняются в результате. Если false, такие поля удаляются из результата. По умолчанию true.
     * @returns {Object} Преобразованный факт во внутренней структуре
     */
    mapMessageToFact(message, keepUnmappedFields=true){
        this._validateMessage(message);
        const factId = this.getFactId(message);
        const fact = {
            _id: factId,
            t: message.t,
            c: new Date(), // дата и время создания объекта
            d: this.mapMessageData(message.d, message.t, keepUnmappedFields),
        };
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
                // Получаем исходное значение
                const sourceValue = messageData[rule.src];
                
                // Определяем целевой тип (по умолчанию string, если не указан)
                const targetType = (rule.generator && rule.generator.type) ? rule.generator.type : 'string';
                
                // Конвертируем значение в целевой тип
                const convertedValue = this._convertValueToType(sourceValue, targetType, rule.src, rule.dst);
                
                // Получаем имя целевого поля с учетом настройки useShortNames
                const targetFieldName = this.fieldNameMapper ? this.fieldNameMapper.getFieldName(rule.dst) : rule.dst;
                
                // Сохраняем конвертированное значение в целевое поле
                factData[targetFieldName] = convertedValue;
                
                this.logger.debug(`Применено правило маппинга: ${rule.src} -> ${targetFieldName} (${typeof sourceValue} -> ${targetType}) для типа ${messageType}`);
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
        // Проверяем кеш
        if (this._mappingRulesCache.has(messageType)) {
            return this._mappingRulesCache.get(messageType);
        }
        
        // Если нет в кеше, вычисляем и сохраняем
        const rules = this._mappingConfig.filter(rule => 
            rule.message_types.includes(messageType)
        );
        
        this._mappingRulesCache.set(messageType, rules);
        return rules;
    }

    /**
     * Очищает кеш правил маппинга
     * Полезно при изменении конфигурации во время выполнения
     */
    clearMappingRulesCache() {
        this._mappingRulesCache.clear();
        this.logger.debug('Кеш правил маппинга очищен');
    }

}

module.exports = FactMapper;
