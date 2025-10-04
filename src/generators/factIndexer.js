const crypto = require('crypto');
const fs = require('fs');
const Logger = require('../utils/logger');

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
 * @property {string} i - Идентификатор факта
 * @property {number} t - Тип факта
 * @property {Date} d - Дата факта
 * @property {Date} c - Дата создания факта в базе данных
 * 
 */
class FactIndexer {
    constructor(configPathOrMapArray = null) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        try {
            if (Array.isArray(configPathOrMapArray)) {
                this._validateConfig(configPathOrMapArray);
                this._indexConfig = configPathOrMapArray;
            } else if (typeof configPathOrMapArray === 'string') {
                this._indexConfig = this._loadConfig(configPathOrMapArray);
            } else {
                this.logger.info('Конфигурация не задана. Индексирование не будет производиться.');
                return;
            }
        } catch (error) {
            this.logger.error(`Ошибка при создании FactIndexer и загрузке конфигурации индексов ${configPathOrMapArray}: ${error.message}`);
            throw error;
        }
        // Выводим информацию о загруженной конфигурации
        if (this._indexConfig && this._indexConfig.length > 0) {
            this.logger.info(`Загружена конфигурация индексов: ${this._indexConfig.length} элементов`);
            this._indexConfig.forEach((config, index) => {
                this.logger.info(`  ${index + 1}. ${config.fieldName} -> ${config.indexTypeName} (тип: ${config.indexType}, значение: ${config.indexValue})`);
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

            // Проверяем тип поля fieldName
            if (typeof configItem.fieldName !== 'string') {
                throw new Error(`Элемент конфигурации ${index}: поле 'fieldName' должно быть строкой`);
            }

            // Проверяем формат названия поля (f1-f23)
            if (!fieldNamePattern.test(configItem.fieldName)) {
                throw new Error(`Элемент конфигурации ${index}: поле 'fieldName' должно быть в формате f1-f23, получено: '${configItem.fieldName}'`);
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
            const allowedFields = ['fieldName', 'indexTypeName', 'indexType', 'indexValue', 'dateName'];
            const extraFields = Object.keys(configItem).filter(key => !allowedFields.includes(key));
            if (extraFields.length > 0) {
                throw new Error(`Элемент конфигурации ${index}: содержит недопустимые поля: ${extraFields.join(', ')}`);
            }
        });

        // Проверяем уникальность комбинаций fieldName + indexTypeName
        const usedCombinations = new Set();
        indexConfig.forEach((configItem, index) => {
            const combination = `${configItem.fieldName}:${configItem.indexTypeName}`;
            if (usedCombinations.has(combination)) {
                throw new Error(`Элемент конфигурации ${index}: дублирующаяся комбинация fieldName и indexTypeName: '${combination}'`);
            }
            usedCombinations.add(combination);
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
    hash(indexType, indexValue) {
        const input = `${indexType}:${indexValue}`;
        return crypto.createHash('sha256').update(input).digest('hex');
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
        const requiredFields = ['t', 'i', 'c'];
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
            return indexValues;
        }

        // Проходим по конфигурации индексов
        this._indexConfig.forEach(configItem => {
            const fieldName = configItem.fieldName;
            const dateName = configItem.dateName;
            
            // Проверяем, есть ли поле в факте
            if (fieldName in fact.d && fact.d[fieldName] !== null && fact.d[fieldName] !== undefined) {
                let indexValue;
                
                // Вычисляем значение индекса в зависимости от indexValue
                if (configItem.indexValue === 1) {
                    // Хеш от типа индекса и значения поля
                    indexValue = this.hash(configItem.indexTypeName, fact.d[fieldName]);
                } else if (configItem.indexValue === 2) {
                    // Само значение поля
                    indexValue = fact.d[fieldName];
                } else {
                    throw new Error(`Неподдерживаемое значение indexValue: ${configItem.indexValue}`);
                }

                // Получаем дату из поля, указанного в dateName
                let indexDate = null; // значение по умолчанию
                if (dateName in fact.d && fact.d[dateName] !== null && fact.d[dateName] !== undefined) {
                    const convertedDate = this._convertToDate(fact.d[dateName]);
                    if (convertedDate !== null) {
                        indexDate = convertedDate;
                    }
                } else {
                    this.logger.warn(`Поле ${dateName}, в котором должна быть дата, не найдено для факта ${fact.i}. Индексирование для ${configItem.indexTypeName} не будет производиться.`);
                    return;
                }
                if (indexDate === null) {
                    this.logger.warn(`Поле ${dateName}, в котором должна быть дата, содержит невалидную дату для факта ${fact.i}. Индексирование для ${configItem.indexTypeName} не будет производиться.`);
                    return;
                }

                indexValues.push({
                    it: configItem.indexType,    // числовой тип индекса из конфигурации
                    v: fact.d[fieldName],          // значение поля из факта
                    h: indexValue,               // вычисленное значение индекса
                    i: fact.i,                   // идентификатор факта
                    t: fact.t,                   // тип факта
                    d: indexDate,                // дата из поля dateName или значение по умолчанию
                    c: fact.c                    // дата создания факта
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
