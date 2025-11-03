const crypto = require('crypto');
const fs = require('fs');
const Logger = require('../utils/logger');
const ConditionEvaluator = require('../common/conditionEvaluator');
const FieldNameMapper = require('./fieldNameMapper');

/**
 * Формат файла indexConfig.json
 * @property {string} fieldName - Имя поля факта, включаемого в индекс
 * @property {string} dateName - Имя поля с датой в факте, используемого для индексации
 * @property {string} indexTypeName - Имя типа индекса
 * @property {number} indexType - Тип индекса
 * @property {number} indexValue - Значение индекса (1 - хеш, 2 - само значение поля fieldName)
 * 
 */

/**
 * Класс для создания индексных значений из фактов
 * 
 * Структура индексного значения (factIndex):
 * @property {string} h - Хеш значение типа + поля факта
 * @property {number} it - Номер индексируемого поля (из названия поля fN)
 * @property {string} v - Значение индексируемого поля
 * @property {string} _id - Идентификатор факта
 * @property {number} t - Тип факта
 * @property {Date} dt - Дата факта
 * @property {Object} d - JSON объект с данными факта (необязательное поле, зависит от настроек)
 * @property {Date} c - Дата создания факта в базе данных
 * 
 */
class FactIndexer {
    INDEX_VALUE_HASH = 1;       // Значение индекса является хешом от типа индекса и значения поля
    INDEX_VALUE_VALUE = 2;      // Значение индекса является само значением поля индекса
    HASH_ALGORITHM = 'sha1';    // Алгоритм хеширования

    constructor(configPathOrMapArray = null, includeFactData = false, useShortNames = false, messageConfig = null) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this.includeFactData = includeFactData; // Включать данные факта в индексное значение
        this._conditionEvaluator = new ConditionEvaluator(this.logger);
        this._useShortNames = useShortNames;
        
        // Инициализируем FieldNameMapper
        this.fieldNameMapper = new FieldNameMapper(messageConfig, useShortNames);
        
        try {
            if (Array.isArray(configPathOrMapArray)) {
                this._validateConfig(configPathOrMapArray);
                this._indexConfig = this._transformIndexConfig(configPathOrMapArray);
            } else if (typeof configPathOrMapArray === 'string') {
                const loadedConfig = this._loadConfig(configPathOrMapArray);
                this._indexConfig = this._transformIndexConfig(loadedConfig);
            } else {
                this.logger.info('Конфигурация не задана. Индексирование не будет производиться.');
                this._indexConfig = [];
                return;
            }
        } catch (error) {
            this.logger.error(`Ошибка при создании FactIndexer и загрузке конфигурации индексов ${configPathOrMapArray}: ${error.message}`);
            throw error;
        }
        // Выводим информацию о загруженной конфигурации
        if (this._indexConfig && this._indexConfig.length > 0) {
            this.logger.info(`Загружена конфигурация индексов: ${this._indexConfig.length} элементов`);
            if (useShortNames) {
                this.logger.info('Используются короткие имена полей (shortDst)');
            }
            this._indexConfig.forEach((config, index) => {
                const fieldNameDisplay = Array.isArray(config.fieldName) ? config.fieldName.join(', ') : config.fieldName;
                this.logger.info(`  ${index + 1}. ${fieldNameDisplay} -> ${config.indexTypeName} (тип: ${config.indexType}, значение: ${config.indexValue})`);
            });
        }
    }

    /**
     * Загружает конфигурацию маппинга из файла
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     */
    _loadConfig(configPath) {
        try {
            if (!fs.existsSync(configPath)) {
                throw new Error(`Файл конфигурации индексов не найден: ${configPath}`);
            }

            const configData = fs.readFileSync(configPath, 'utf8');
            const indexConfig = JSON.parse(configData);

            // Валидация структуры конфигурации
            this._validateConfig(indexConfig);

            this.logger.info(`Загружена конфигурация индексов из ${configPath}`);
            this.logger.info(`Количество индексов: ${indexConfig.length}`);
            return indexConfig;
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации индексов: ${error.message}`);
            throw error;
        }
    }

    /**
     * Преобразует конфигурацию индексов с учетом настройки useShortNames
     * @param {Array} indexConfig - Конфигурация индексов
     * @returns {Array} Преобразованная конфигурация
     * @private
     */
    _transformIndexConfig(indexConfig) {
        if (!this._useShortNames || !this.fieldNameMapper) {
            return indexConfig;
        }
        
        return indexConfig.map(configItem => {
            const transformed = { ...configItem };
            
            // Преобразуем fieldName (может быть строка или массив)
            if (Array.isArray(configItem.fieldName)) {
                transformed.fieldName = configItem.fieldName.map(fn => this.fieldNameMapper.getFieldName(fn));
            } else if (typeof configItem.fieldName === 'string') {
                transformed.fieldName = this.fieldNameMapper.getFieldName(configItem.fieldName);
            }
            
            // Преобразуем dateName
            if (typeof configItem.dateName === 'string') {
                transformed.dateName = this.fieldNameMapper.getFieldName(configItem.dateName);
            }
            
            // Преобразуем computationConditions
            if (configItem.computationConditions && typeof configItem.computationConditions === 'object') {
                transformed.computationConditions = this.fieldNameMapper.transformCondition(configItem.computationConditions);
            }
            
            return transformed;
        });
    }
    
    /**
     * Валидирует структуру загруженной конфигурации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateConfig(indexConfig) {
        if (!Array.isArray(indexConfig)) {
            throw new Error('Конфигурация должна быть массивом объектов');
        }

        if (indexConfig.length === 0) {
            throw new Error('Конфигурация не может быть пустым массивом');
        }


        // Регулярное выражение для проверки названий полей f1-f23
        const fieldNamePattern = /^f([1-9]|1[0-9]|2[0-3])$/;

        indexConfig.forEach((configItem, index) => {
            // Проверяем, что элемент конфигурации является объектом
            if (!configItem || typeof configItem !== 'object') {
                throw new Error(`Элемент конфигурации ${index} должен быть объектом`);
            }

            // Проверяем наличие обязательных полей
            const requiredFields = ['fieldName', 'indexTypeName', 'indexType', 'indexValue', 'dateName'];
            for (const field of requiredFields) {
                if (!(field in configItem)) {
                    throw new Error(`Элемент конфигурации ${index}: отсутствует обязательное поле '${field}'`);
                }
            }

            // Проверяем тип поля fieldName (строка или массив строк)
            const isFieldNameString = typeof configItem.fieldName === 'string';
            const isFieldNameArray = Array.isArray(configItem.fieldName) && configItem.fieldName.every(v => typeof v === 'string');
            if (!isFieldNameString && !isFieldNameArray) {
                throw new Error(`Элемент конфигурации ${index}: поле 'fieldName' должно быть строкой или массивом строк`);
            }

            // Проверяем тип поля indexTypeName
            if (typeof configItem.indexTypeName !== 'string') {
                throw new Error(`Элемент конфигурации ${index}: поле 'indexTypeName' должно быть строкой`);
            }


            // Проверяем тип поля indexType
            if (typeof configItem.indexType !== 'number') {
                throw new Error(`Элемент конфигурации ${index}: поле 'indexType' должно быть числом`);
            }

            // Проверяем, что indexType является положительным целым числом
            if (!Number.isInteger(configItem.indexType) || configItem.indexType <= 0) {
                throw new Error(`Элемент конфигурации ${index}: поле 'indexType' должно быть положительным целым числом, получено: ${configItem.indexType}`);
            }

            // Проверяем тип поля indexValue
            if (typeof configItem.indexValue !== 'number') {
                throw new Error(`Элемент конфигурации ${index}: поле 'indexValue' должно быть числом`);
            }

            // Проверяем, что indexValue равен 1 или 2
            if (configItem.indexValue !== 1 && configItem.indexValue !== 2) {
                throw new Error(`Элемент конфигурации ${index}: поле 'indexValue' должно быть 1 или 2, получено: ${configItem.indexValue}`);
            }

            // Проверяем тип поля dateName
            if (typeof configItem.dateName !== 'string') {
                throw new Error(`Элемент конфигурации ${index}: поле 'dateName' должно быть строкой`);
            }

            // Проверяем на наличие лишних полей
            const allowedFields = ['fieldName', 'indexTypeName', 'indexType', 'indexValue', 'dateName', 'limit', 'comment', 'computationConditions'];
            const extraFields = Object.keys(configItem).filter(key => !allowedFields.includes(key));
            if (extraFields.length > 0) {
                throw new Error(`Элемент конфигурации ${index}: содержит недопустимые поля: ${extraFields.join(', ')}`);
            }

            // Валидация computationConditions (если указано)
            if ('computationConditions' in configItem && configItem.computationConditions !== null && typeof configItem.computationConditions !== 'object') {
                throw new Error(`Элемент конфигурации ${index}: поле 'computationConditions' должно быть объектом или null`);
            }
        });

        // Проверяем уникальность комбинаций fieldName + indexTypeName (для массива проверяем каждое имя)
        const usedCombinations = new Set();
        indexConfig.forEach((configItem, index) => {
            const fieldNames = Array.isArray(configItem.fieldName) ? configItem.fieldName : [configItem.fieldName];
            fieldNames.forEach((fn) => {
                const combination = `${fn}:${configItem.indexTypeName}`;
                if (usedCombinations.has(combination)) {
                    throw new Error(`Элемент конфигурации ${index}: дублирующаяся комбинация fieldName и indexTypeName: '${combination}'`);
                }
                usedCombinations.add(combination);
            });
        });

        // Проверяем уникальность indexType
        const usedIndexTypes = new Set();
        indexConfig.forEach((configItem, index) => {
            if (usedIndexTypes.has(configItem.indexType)) {
                throw new Error(`Элемент конфигурации ${index}: дублирующийся indexType: ${configItem.indexType}`);
            }
            usedIndexTypes.add(configItem.indexType);
        });

        this.logger.info(`Конфигурация индексов валидна. Количество индексов: ${indexConfig.length}`);
    }

    /**
     * Хеш функция для создания уникального индекса из типа и значения поля
     * @param {number|string} indexType - тип/номер поля (it)
     * @param {string} indexValue - значение поля (f)
     * @returns {string} SHA-256 хеш в hex формате
     */
    _hashHex(indexType, indexValue) {
        const input = `${indexType}:${indexValue}`;
        return crypto.createHash(this.HASH_ALGORITHM).update(input).digest('hex');
    }

    _hashBase64(indexType, indexValue) {
        const input = `${indexType}:${indexValue}`;
        return crypto.createHash(this.HASH_ALGORITHM).update(input).digest('base64');
    }

    /**
     * Преобразует значение в Date, если это возможно
     * @param {any} value - значение для преобразования
     * @returns {Date|null} объект Date или null, если преобразование невозможно
     */
    _convertToDate(value) {
        if (value === null || value === undefined) {
            return null;
        }

        // Если уже Date объект
        if (value instanceof Date) {
            return isNaN(value.getTime()) ? null : value;
        }

        // Пытаемся создать Date из строки или числа
        const date = new Date(value);
        return isNaN(date.getTime()) ? null : date;
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
        const requiredFields = ['_id', 't', 'c'];
        for (const field of requiredFields) {
            if (!(field in fact)) {
                throw new Error(`Отсутствует обязательное поле: ${field}`);
            }
        }

        const indexValues = [];
        
        // Если нет конфигурации индексов, возвращаем пустой массив
        if (!this._indexConfig || this._indexConfig.length === 0) {
            return indexValues;
        }

        // Если нет поля d, возвращаем пустой массив
        if (!fact.d || typeof fact.d !== 'object') {
            this.logger.warn(`Отсутствует поле d в факте ${fact._id}. Индексирование для ${this._indexConfig.indexTypeName} не будет производиться.`);
            return indexValues;
        }

        // Проходим по конфигурации индексов
        this._indexConfig.forEach(configItem => {
            const fieldNames = Array.isArray(configItem.fieldName) ? configItem.fieldName : [configItem.fieldName];
            const dateName = configItem.dateName;
            
            // Для каждого возможного имени поля формируем индекс
            fieldNames.forEach((fieldName) => {
                if (!(fieldName in fact.d) || fact.d[fieldName] === null || fact.d[fieldName] === undefined) {
                    return;
                }
                // Проверяем логическое выражение индекса (если указано)
                if (configItem.computationConditions && !this._conditionEvaluator.matchesCondition(fact, configItem.computationConditions)) {
                    this.logger.debug(`Индекс '${configItem.indexTypeName}' пропущен для факта ${fact._id} по computationConditions`);
                    return;
                }
                this.logger.debug(`Поле ${fieldName} найдено в факте ${fact._id}`);
                let indexValue;
                
                // Вычисляем значение индекса в зависимости от indexValue
                if (configItem.indexValue === this.INDEX_VALUE_HASH) {
                    // Хеш от типа индекса и значения поля
                    indexValue = this._hashBase64(configItem.indexType, fact.d[fieldName]);
                } else if (configItem.indexValue === this.INDEX_VALUE_VALUE) {
                    // Само значение поля
                    indexValue = `${configItem.indexType}:${String(fact.d[fieldName])}`;
                } else {
                    throw new Error(`Неподдерживаемое значение indexValue: ${configItem.indexValue}`);
                }

                // Получаем дату из поля, указанного в dateName
                let indexDate = null; // значение по умолчанию
                if (dateName in fact.d && fact.d[dateName] !== null && fact.d[dateName] !== undefined) {
                    const convertedDate = this._convertToDate(fact.d[dateName]);
                    if (convertedDate !== null) {
                        indexDate = convertedDate;
                    } else {
                        this.logger.warn(`Ошибка конвертации поля ${dateName} со значением ${fact.d[dateName]}, в котором должна быть дата.`);
                        return;
                    }
                } else {
                    this.logger.warn(`Поле ${dateName}, в котором должна быть дата, не найдено для факта ${fact._id}. Индексирование для ${configItem.indexTypeName} не будет производиться.`);
                    return;
                }
                if (indexDate === null) {
                    this.logger.warn(`Поле ${dateName}, в котором должна быть дата, содержит невалидную дату для факта ${fact._id}. Индексирование для ${configItem.indexTypeName} не будет производиться.`);
                    return;
                }

                const indexData = {
                    "_id": {
                        "h": indexValue,              // вычисленное значение индекса
                        "f": fact._id                 // идентификатор факта
                    },               
                    "dt": indexDate,                  // дата из поля dateName или значение по умолчанию
                    "c": new Date(),                  // дата создания факта
                    // @deprecated нужно удалить после отладки
                    "it": configItem.indexType,       // числовой тип индекса из конфигурации
                    "v": String(fact.d[fieldName]),   // значение поля из факта
                    "t": fact.t,                      // тип факта
                };
                if (this.includeFactData) {
                    indexData["d"] = fact.d;       // JSON объект с данными факта
                }
                indexValues.push(indexData);
            });
        });

        this.logger.debug(`Создано ${indexValues.length} индексных значений`);

        return indexValues;
    }

    /**
     * Получение хешей значений индексов для поиска релевантных фактов
     * @param {object[]} indexValues - массив индексных значений, который получен из метода index
     * @returns {object[]} массив объектов с хешем значения индекса и индексом из конфигурации
     */
    getHashValuesForSearch(indexValues) {
        return indexValues.map(value => ({
            hashValue: value._id.h,
            index: this._indexConfig.find(configItem => configItem.indexType === value.it)
        }));
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
                this.logger.warn(`Ошибка при создании индексных значений для факта ${index}: ${error.message}`);
            }
        });

        return allIndexValues;
    }

}

module.exports = FactIndexer;
