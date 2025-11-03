const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger');

/**
 * Класс для преобразования имен полей между dst и shortDst
 * Используется для оптимизации размера JSON документов путем использования коротких имен полей
 */
class FieldNameMapper {
    /**
     * Создает экземпляр FieldNameMapper
     * @param {Array|string} messageConfigPathOrMapArray - Конфигурация маппинга полей (массив правил или путь к файлу)
     * @param {boolean} useShortNames - Использовать ли короткие имена полей
     */
    constructor(messageConfigPathOrMapArray, useShortNames = false) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this.useShortNames = useShortNames;
        
        // Карты соответствия
        this._dstToShortDst = new Map(); // dst -> shortDst
        this._shortDstToDst = new Map(); // shortDst -> dst
        
        // Загружаем конфигурацию
        let config = null;
        
        if (!messageConfigPathOrMapArray) {
            this.logger.info('Конфигурация не задана. Маппинг имен полей не будет производиться.');
            config = [];
        } else if (Array.isArray(messageConfigPathOrMapArray)) {
            // Инициализация через массив конфигурации
            config = messageConfigPathOrMapArray;
            this.logger.debug(`Инициализирован с массивом конфигурации. Количество правил: ${config.length}`);
        } else if (typeof messageConfigPathOrMapArray === 'string') {
            // Инициализация через путь к файлу
            config = this._loadConfig(messageConfigPathOrMapArray);
        } else {
            this.logger.warn('Некорректный тип конфигурации. Ожидается массив или строка (путь к файлу).');
            config = [];
        }
        
        if (!Array.isArray(config)) {
            this.logger.warn('Конфигурация должна быть массивом. Используется пустой массив.');
            config = [];
        }
        
        // Создаем карты соответствия
        this._buildMaps(config);
        
        // Валидация при включенной настройке
        if (this.useShortNames) {
            this._validateShortDst(config);
        }
    }
    
    /**
     * Ищет файл в нескольких директориях
     * @param {string} filename - имя файла для поиска
     * @param {Array<string>} searchPaths - массив путей для поиска
     * @returns {string|null} путь к найденному файлу или null
     * @private
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
     * @param {string} configPath - Путь к файлу конфигурации
     * @returns {Array} Массив правил конфигурации
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     * @private
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
            const config = JSON.parse(configData);

            this.logger.info(`Загружена конфигурация маппинга полей из ${configPath}`);
            this.logger.debug(`Количество правил маппинга: ${config.length}`);
            return config;
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации маппинга полей: ${error.message}`);
            throw error;
        }
    }
    
    /**
     * Создает карты соответствия dst -> shortDst и shortDst -> dst
     * @param {Array} config - Конфигурация маппинга полей
     * @private
     */
    _buildMaps(config) {
        for (const rule of config) {
            if (!rule || typeof rule !== 'object') continue;
            if (!rule.dst || typeof rule.dst !== 'string') continue;
            
            const dst = rule.dst;
            const shortDst = rule.shortDst;
            
            // Если есть shortDst, добавляем в карты
            if (shortDst && typeof shortDst === 'string') {
                this._dstToShortDst.set(dst, shortDst);
                
                // Проверяем конфликты в обратной карте
                if (this._shortDstToDst.has(shortDst) && this._shortDstToDst.get(shortDst) !== dst) {
                    this.logger.warn(`Конфликт: разные dst ('${this._shortDstToDst.get(shortDst)}' и '${dst}') имеют одинаковый shortDst '${shortDst}'`);
                } else {
                    this._shortDstToDst.set(shortDst, dst);
                }
            }
        }
        
        this.logger.debug(`Созданы карты соответствия: ${this._dstToShortDst.size} пар dst->shortDst`);
    }
    
    /**
     * Валидирует наличие shortDst для всех dst при включенной настройке useShortNames
     * @param {Array} config - Конфигурация маппинга полей
     * @throws {Error} Если при включенной настройке отсутствует shortDst для какого-то dst
     * @private
     */
    _validateShortDst(config) {
        const missingShortDst = [];
        
        for (const rule of config) {
            if (!rule || typeof rule !== 'object') continue;
            if (!rule.dst || typeof rule.dst !== 'string') continue;
            
            if (!rule.shortDst || typeof rule.shortDst !== 'string') {
                missingShortDst.push(rule.dst);
            }
        }
        
        if (missingShortDst.length > 0) {
            const errorMsg = `При включенной настройке USE_SHORT_NAMES=true все поля должны иметь shortDst. Отсутствует shortDst для полей: ${missingShortDst.join(', ')}`;
            this.logger.error(errorMsg);
            throw new Error(errorMsg);
        }
    }
    
    /**
     * Возвращает имя поля с учетом настройки useShortNames
     * @param {string} dstName - Имя поля dst
     * @returns {string} shortDst или dst в зависимости от настройки
     */
    getFieldName(dstName) {
        if (!dstName || typeof dstName !== 'string') {
            return dstName;
        }
        
        if (!this.useShortNames) {
            return dstName;
        }
        
        // Если поле есть в карте, возвращаем shortDst
        if (this._dstToShortDst.has(dstName)) {
            return this._dstToShortDst.get(dstName);
        }
        
        // Если поля нет в карте, логируем предупреждение и возвращаем исходное имя
        this.logger.warn(`Поле '${dstName}' не найдено в конфигурации маппинга. Используется исходное имя.`);
        return dstName;
    }
    
    /**
     * Возвращает оригинальное имя поля (dst) из shortDst
     * @param {string} shortDstName - Имя поля shortDst
     * @returns {string} dst или shortDstName, если соответствие не найдено
     */
    getOriginalFieldName(shortDstName) {
        if (!shortDstName || typeof shortDstName !== 'string') {
            return shortDstName;
        }
        
        if (this._shortDstToDst.has(shortDstName)) {
            return this._shortDstToDst.get(shortDstName);
        }
        
        // Если соответствие не найдено, возвращаем исходное имя
        return shortDstName;
    }
    
    /**
     * Преобразует путь к полю вида "d.fieldName" -> "d.shortFieldName"
     * Если префикса "d." нет, возвращается тот же path
     * @param {string} path - Путь к полю
     * @returns {string} Преобразованный путь
     */
    transformFieldPath(path) {
        if (!path || typeof path !== 'string') {
            return path;
        }
        
        if (!path.startsWith('d.')) {
            return path;
        }
        
        const fieldName = path.substring(2); // убираем "d."
        const transformedField = this.getFieldName(fieldName);
        return `d.${transformedField}`;
    }
    
    /**
     * Преобразует ссылку на поле в MongoDB выражении вида "$d.fieldName" -> "$d.shortFieldName"
     * @param {string|Array} path - Путь к полю или массив путей
     * @returns {string|Array} Преобразованный путь или массив
     */
    transformMongoPath(path) {
        if (!path) {
            return path;
        }
        
        // Обработка массивов с путями
        if (Array.isArray(path)) {
            return path.map(item => this.transformMongoPath(item));
        }
        
        if (typeof path !== 'string') {
            return path;
        }
        
        // Обработка путей вида "$d.fieldName" или "$d.fieldName.suffix"
        // Регулярное выражение: match[1] - первое поле после d., match[2] - остальная часть пути
        const match = path.match(/^\$d\.([^.]+)(.*)$/);
        if (match) {
            const fieldName = match[1];
            const suffix = match[2];  // Может быть пустой строкой или содержать ".subfield"
            const transformedField = this.getFieldName(fieldName);
            return `$d.${transformedField}${suffix}`;
        }
        
        return path;
    }
    
    /**
     * Рекурсивно преобразует объект условий (computationConditions, evaluationConditions)
     * @param {*} condition - Условие для преобразования
     * @returns {*} Преобразованное условие
     */
    transformCondition(condition) {
        if (condition === null || condition === undefined) {
            return condition;
        }
        
        if (Array.isArray(condition)) {
            return condition.map(item => this.transformCondition(item));
        }
        
        if (typeof condition !== 'object') {
            return condition;
        }
        
        const result = {};
        
        for (const [key, value] of Object.entries(condition)) {
            if (key === '$expr') {
                // Специальная обработка $expr
                result[key] = this.transformExprExpression(value);
            } else if (key.startsWith('d.')) {
                // Преобразование пути к полю
                const fieldPath = key.substring(2); // убираем "d."
                const transformedField = this.getFieldName(fieldPath);
                result[`d.${transformedField}`] = this.transformCondition(value);
            } else if (key.startsWith('$')) {
                // MongoDB операторы оставляем без изменений, но рекурсивно обрабатываем значение
                result[key] = this.transformCondition(value);
            } else {
                // Обычные ключи передаем как есть
                result[key] = this.transformCondition(value);
            }
        }
        
        return result;
    }
    
    /**
     * Преобразует $expr выражения
     * @param {*} exprValue - Значение $expr выражения
     * @returns {*} Преобразованное выражение
     */
    transformExprExpression(exprValue) {
        if (!exprValue || typeof exprValue !== 'object' || Array.isArray(exprValue)) {
            return exprValue;
        }
        
        const result = {};
        
        for (const [operator, operands] of Object.entries(exprValue)) {
            if (!Array.isArray(operands)) {
                // Если operands не массив, это может быть вложенное выражение
                result[operator] = this.transformExprExpression(operands);
                continue;
            }
            
            // Обрабатываем массив операндов
            result[operator] = operands.map(operand => {
                if (typeof operand === 'string' && operand.startsWith('$d.')) {
                    // Преобразуем путь к полю в операнде
                    return this.transformMongoPath(operand);
                } else if (typeof operand === 'object' && operand !== null) {
                    // Рекурсивно обрабатываем вложенные объекты (например, $dateAdd)
                    return this.transformCondition(operand);
                }
                return operand;
            });
        }
        
        return result;
    }
    
    /**
     * Преобразует объект attributes счетчика
     * @param {*} attributes - Атрибуты счетчика
     * @returns {*} Преобразованные атрибуты
     */
    transformAttributes(attributes) {
        if (!attributes || typeof attributes !== 'object') {
            return attributes;
        }
        
        const result = {};
        
        for (const [attrName, attrValue] of Object.entries(attributes)) {
            if (typeof attrValue === 'string' && attrValue.startsWith('$d.')) {
                // Прямая ссылка на поле
                result[attrName] = this.transformMongoPath(attrValue);
            } else if (typeof attrValue === 'object' && attrValue !== null && !Array.isArray(attrValue)) {
                // Объект с оператором агрегации (например, {"$sum": "$d.fieldName"})
                const transformedAttr = {};
                for (const [key, value] of Object.entries(attrValue)) {
                    if (typeof value === 'string' && value.startsWith('$d.')) {
                        transformedAttr[key] = this.transformMongoPath(value);
                    } else if (typeof value === 'object' && value !== null) {
                        // Рекурсивно обрабатываем вложенные структуры
                        transformedAttr[key] = this.transformAttributes(value);
                    } else {
                        transformedAttr[key] = value;
                    }
                }
                result[attrName] = transformedAttr;
            } else {
                result[attrName] = attrValue;
            }
        }
        
        return result;
    }
}

module.exports = FieldNameMapper;

