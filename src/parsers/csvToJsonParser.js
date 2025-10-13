const fs = require('fs');
const path = require('path');

/**
 * Парсер для преобразования CSV файла counters.csv в JSON формат countersCfg.json
 * Обрабатывает условия Evaluation и Computation, а также атрибуты агрегации
 */
class CsvToJsonParser {
    constructor() {
        this.errors = [];
        this.warnings = [];
    }

    /**
     * Основной метод для парсинга CSV файла
     * @param {string} csvFilePath - путь к CSV файлу
     * @param {string} outputFilePath - путь к выходному JSON файлу
     */
    parse(csvFilePath, outputFilePath) {
        try {
            console.log(`Начинаем парсинг файла: ${csvFilePath}`);
            
            // Читаем CSV файл
            const csvContent = fs.readFileSync(csvFilePath, 'utf8');
            const lines = this.parseCSV(csvContent);
            
            if (lines.length === 0) {
                throw new Error('CSV файл пуст');
            }

            // Получаем заголовки
            const headers = lines[0];
            console.log('Заголовки CSV:', headers);

            // Парсим данные
            const counters = [];
            for (let i = 1; i < lines.length; i++) {
                try {
                    const counter = this.parseCounterLine(lines[i], headers, i + 1);
                    if (counter) {
                        counters.push(counter);
                    }
                } catch (error) {
                    this.errors.push(`Строка ${i + 1}: ${error.message}`);
                    console.error(`Ошибка в строке ${i + 1}:`, error.message);
                }
            }

            // Создаем результирующий объект
            const result = {
                counters: counters,
                metadata: {
                    totalCounters: counters.length,
                    errors: this.errors.length,
                    warnings: this.warnings.length,
                    parsedAt: new Date().toISOString()
                }
            };

            // Сохраняем результат
            fs.writeFileSync(outputFilePath, JSON.stringify(result, null, 2), 'utf8');
            
            console.log(`Парсинг завершен. Создано ${counters.length} счетчиков`);
            console.log(`Ошибок: ${this.errors.length}, Предупреждений: ${this.warnings.length}`);
            
            if (this.errors.length > 0) {
                console.log('Ошибки:', this.errors);
            }
            if (this.warnings.length > 0) {
                console.log('Предупреждения:', this.warnings);
            }

            return result;

        } catch (error) {
            console.error('Критическая ошибка парсинга:', error.message);
            throw error;
        }
    }

    /**
     * Парсинг CSV с учетом экранирования кавычек
     * @param {string} content - содержимое CSV файла
     * @returns {Array} массив строк с полями
     */
    parseCSV(content) {
        const lines = [];
        const rows = content.split('\n');
        
        for (const row of rows) {
            if (row.trim() === '') continue;
            
            const fields = this.parseCSVRow(row);
            lines.push(fields);
        }
        
        return lines;
    }

    /**
     * Парсинг одной строки CSV с учетом экранирования
     * @param {string} row - строка CSV
     * @returns {Array} массив полей
     */
    parseCSVRow(row) {
        const fields = [];
        let current = '';
        let inQuotes = false;
        let i = 0;
        
        while (i < row.length) {
            const char = row[i];
            
            if (char === '"') {
                if (inQuotes && i + 1 < row.length && row[i + 1] === '"') {
                    // Экранированная кавычка
                    current += '"';
                    i += 2;
                } else {
                    // Начало или конец кавычек
                    inQuotes = !inQuotes;
                    i++;
                }
            } else if (char === ';' && !inQuotes) {
                // Разделитель полей
                fields.push(current.trim());
                current = '';
                i++;
            } else {
                current += char;
                i++;
            }
        }
        
        // Добавляем последнее поле
        fields.push(current.trim());
        
        return fields;
    }

    /**
     * Парсинг одной строки счетчика с валидацией
     * @param {Array} fields - поля строки
     * @param {Array} headers - заголовки
     * @param {number} lineNumber - номер строки
     * @returns {Object} объект счетчика
     */
    parseCounterLine(fields, headers, lineNumber) {
        // Валидация количества полей
        if (fields.length !== headers.length) {
            throw new Error(`Количество полей (${fields.length}) не соответствует количеству заголовков (${headers.length})`);
        }

        // Создаем объект из полей
        const rowData = {};
        headers.forEach((header, index) => {
            rowData[header] = fields[index];
        });

        // Валидация обязательных полей
        this.validateRequiredFields(rowData, lineNumber);

        // Парсим условия с обработкой ошибок
        let evaluationConditions = null;
        let computationConditions = null;
        
        try {
            evaluationConditions = this.parseConditions(
                rowData['Evaluation Conditions'], 
                'evaluation', 
                lineNumber
            );
        } catch (error) {
            this.warnings.push(`Строка ${lineNumber}: Ошибка парсинга Evaluation Conditions: ${error.message}`);
        }
        
        try {
            computationConditions = this.parseConditions(
                rowData['Computation Conditions'], 
                'computation', 
                lineNumber
            );
        } catch (error) {
            this.warnings.push(`Строка ${lineNumber}: Ошибка парсинга Computation Conditions: ${error.message}`);
        }

        // Парсим атрибуты с обработкой ошибок
        let attributes = null;
        try {
            attributes = this.parseAttributes(rowData['Attributes'], lineNumber);
        } catch (error) {
            this.warnings.push(`Строка ${lineNumber}: Ошибка парсинга Attributes: ${error.message}`);
            attributes = { cnt: { "$sum": 1 } }; // Базовый атрибут
        }

        // Валидация индекса
        const indexTypeName = this.validateIndex(rowData['Index'], lineNumber);

        // Создаем объект счетчика
        const counter = {
            name: rowData['Name'],
            comment: rowData['Comment'] || `${rowData['Name']} - счетчик`,
            indexTypeName: indexTypeName,
            computationConditions: computationConditions,
            evaluationConditions: evaluationConditions,
            attributes: attributes
        };

        // Финальная валидация счетчика
        this.validateCounter(counter, lineNumber);

        return counter;
    }

    /**
     * Валидация обязательных полей
     * @param {Object} rowData - данные строки
     * @param {number} lineNumber - номер строки
     */
    validateRequiredFields(rowData, lineNumber) {
        if (!rowData['Name'] || rowData['Name'].trim() === '') {
            throw new Error('Отсутствует обязательное поле Name');
        }

        // Проверяем корректность имени счетчика (разрешаем точки)
        if (!/^[a-zA-Z0-9_.]+$/.test(rowData['Name'])) {
            this.warnings.push(`Строка ${lineNumber}: Имя счетчика содержит недопустимые символы: ${rowData['Name']}`);
        }
    }

    /**
     * Валидация индекса
     * @param {string} index - название индекса
     * @param {number} lineNumber - номер строки
     * @returns {string} валидное название индекса
     */
    validateIndex(index, lineNumber) {
        if (!index || index.trim() === '') {
            this.warnings.push(`Строка ${lineNumber}: Отсутствует название индекса, используется idx_default`);
            return 'idx_default';
        }

        // Проверяем корректность названия индекса (разрешаем пробелы и точки)
        if (!/^[a-zA-Z0-9_.\s]+$/.test(index)) {
            this.warnings.push(`Строка ${lineNumber}: Название индекса содержит недопустимые символы: ${index}`);
        }

        return index;
    }

    /**
     * Финальная валидация счетчика
     * @param {Object} counter - объект счетчика
     * @param {number} lineNumber - номер строки
     */
    validateCounter(counter, lineNumber) {
        // Проверяем, что есть хотя бы одно условие
        if (!counter.computationConditions && !counter.evaluationConditions) {
            this.warnings.push(`Строка ${lineNumber}: Счетчик ${counter.name} не имеет условий`);
        }

        // Проверяем атрибуты
        if (!counter.attributes || Object.keys(counter.attributes).length === 0) {
            this.warnings.push(`Строка ${lineNumber}: Счетчик ${counter.name} не имеет атрибутов`);
        }

        // Проверяем корректность MongoDB операторов в условиях
        this.validateMongoOperators(counter.computationConditions, 'computation', lineNumber);
        this.validateMongoOperators(counter.evaluationConditions, 'evaluation', lineNumber);
    }

    /**
     * Валидация MongoDB операторов
     * @param {Object} conditions - условия
     * @param {string} type - тип условий
     * @param {number} lineNumber - номер строки
     */
    validateMongoOperators(conditions, type, lineNumber) {
        if (!conditions) return;

        const validOperators = ['$in', '$nin', '$eq', '$ne', '$gt', '$lt', '$gte', '$lte', '$regex', '$not', '$expr', '$dateAdd', '$dateSubtract', '$options'];
        
        const validateObject = (obj, path = '') => {
            for (const [key, value] of Object.entries(obj)) {
                if (typeof value === 'object' && value !== null) {
                    if (Array.isArray(value)) {
                        continue; // Массивы не проверяем
                    }
                    
                    for (const [op, val] of Object.entries(value)) {
                        if (op.startsWith('$') && !validOperators.includes(op)) {
                            this.warnings.push(`Строка ${lineNumber}: Неизвестный MongoDB оператор '${op}' в ${type} условиях${path ? ` (${path})` : ''}`);
                        }
                    }
                    
                    validateObject(value, path ? `${path}.${key}` : key);
                }
            }
        };

        validateObject(conditions);
    }

    /**
     * Парсинг условий (Evaluation или Computation)
     * @param {string} conditionsStr - строка условий
     * @param {string} type - тип условий ('evaluation' или 'computation')
     * @param {number} lineNumber - номер строки
     * @returns {Object} объект условий MongoDB
     */
    parseConditions(conditionsStr, type, lineNumber) {
        if (!conditionsStr || conditionsStr.trim() === '') {
            return null;
        }

        try {
            // Убираем кавычки если есть
            let cleanStr = conditionsStr.trim();
            if (cleanStr.startsWith('"') && cleanStr.endsWith('"')) {
                cleanStr = cleanStr.slice(1, -1);
            }

            const conditions = {};
            const expressions = this.extractExpressions(cleanStr);

            for (const expr of expressions) {
                try {
                    const parsedExpr = this.parseExpression(expr, type, lineNumber);
                    if (parsedExpr) {
                        Object.assign(conditions, parsedExpr);
                    }
                } catch (error) {
                    this.warnings.push(`Строка ${lineNumber}: Не удалось распарсить выражение "${expr}": ${error.message}`);
                }
            }

            return Object.keys(conditions).length > 0 ? conditions : null;

        } catch (error) {
            this.warnings.push(`Строка ${lineNumber}: Ошибка парсинга условий "${conditionsStr}": ${error.message}`);
            return null;
        }
    }

    /**
     * Извлечение выражений из строки условий с улучшенной обработкой
     * @param {string} str - строка условий
     * @returns {Array} массив выражений
     */
    extractExpressions(str) {
        const expressions = [];
        let current = '';
        let depth = 0;
        let i = 0;

        while (i < str.length) {
            const char = str[i];
            
            if (char === '(') {
                depth++;
                current += char;
            } else if (char === ')') {
                depth--;
                current += char;
                if (depth === 0) {
                    expressions.push(current.trim());
                    current = '';
                }
            } else if (char === ';' && depth === 0) {
                // Точка с запятой на верхнем уровне - разделитель выражений
                if (current.trim() !== '') {
                    expressions.push(current.trim());
                }
                current = '';
            } else {
                current += char;
            }
            i++;
        }

        // Добавляем последнее выражение, если есть
        if (current.trim() !== '') {
            expressions.push(current.trim());
        }

        return expressions.filter(expr => expr !== '');
    }

    /**
     * Парсинг одного выражения
     * @param {string} expr - выражение в скобках
     * @param {string} type - тип условий
     * @param {number} lineNumber - номер строки
     * @returns {Object} объект условия MongoDB
     */
    parseExpression(expr, type, lineNumber) {
        // Убираем скобки
        let cleanExpr = expr.trim();
        if (cleanExpr.startsWith('(') && cleanExpr.endsWith(')')) {
            cleanExpr = cleanExpr.slice(1, -1);
        }

        // Разбираем выражение на части с учетом операторов
        const parts = this.parseExpressionParts(cleanExpr);
        if (parts.length < 3) {
            throw new Error(`Неверный формат выражения: ${expr}`);
        }

        const fieldName = parts[0].trim();
        const operator = parts[1].trim();
        const values = parts.slice(2).join(' ').trim();

        return this.buildMongoCondition(fieldName, operator, values, type, lineNumber);
    }

    /**
     * Разбор выражения на части с учетом сложных операторов
     * @param {string} expr - выражение
     * @returns {Array} массив частей
     */
    parseExpressionParts(expr) {
        const parts = [];
        let current = '';
        let i = 0;

        // Сначала ищем имя поля
        while (i < expr.length && expr[i] !== ' ') {
            current += expr[i];
            i++;
        }
        if (current.trim() !== '') {
            parts.push(current.trim());
            current = '';
        }

        // Пропускаем пробелы
        while (i < expr.length && expr[i] === ' ') {
            i++;
        }

        // Ищем оператор
        const operators = ['¬=*=', '=*=', '¬*=', '*=', '≥', '≤', '≠', '=', '>', '<', 'is'];
        let foundOperator = null;
        
        for (const op of operators) {
            if (expr.substring(i, i + op.length) === op) {
                foundOperator = op;
                parts.push(op);
                i += op.length;
                break;
            }
        }

        if (!foundOperator) {
            throw new Error(`Неизвестный оператор в выражении: ${expr}`);
        }

        // Пропускаем пробелы после оператора
        while (i < expr.length && expr[i] === ' ') {
            i++;
        }

        // Остальная часть - значения
        if (i < expr.length) {
            parts.push(expr.substring(i).trim());
        }

        return parts;
    }

    /**
     * Построение условия MongoDB
     * @param {string} fieldName - имя поля
     * @param {string} operator - оператор
     * @param {string} values - значения
     * @param {string} type - тип условий
     * @param {number} lineNumber - номер строки
     * @returns {Object} условие MongoDB
     */
    buildMongoCondition(fieldName, operator, values, type, lineNumber) {
        const mongoField = `d.${fieldName}`;
        const parsedValues = this.parseValues(values, type);

        switch (operator) {
            case '=':
                if (parsedValues.length === 1) {
                    return { [mongoField]: parsedValues[0] };
                } else {
                    return { [mongoField]: { "$in": parsedValues } };
                }

            case '≠':
                if (parsedValues.length === 1) {
                    return { [mongoField]: { "$ne": parsedValues[0] } };
                } else {
                    return { [mongoField]: { "$nin": parsedValues } };
                }

            case '>':
                return { [mongoField]: { "$gt": parsedValues[0] } };

            case '<':
                return { [mongoField]: { "$lt": parsedValues[0] } };

            case '≥':
                return { [mongoField]: { "$gte": parsedValues[0] } };

            case '≤':
                return { [mongoField]: { "$lte": parsedValues[0] } };

            case '=*=':
                if (parsedValues.length === 1) {
                    return { [mongoField]: { "$regex": parsedValues[0], "$options": "i" } };
                } else {
                    const regexPattern = parsedValues.map(v => `(${this.escapeRegex(v)})`).join('|');
                    return { [mongoField]: { "$regex": regexPattern, "$options": "i" } };
                }

            case '¬=*=':
                if (parsedValues.length === 1) {
                    return { [mongoField]: { "$not": { "$regex": parsedValues[0], "$options": "i" } } };
                } else {
                    const regexPattern = parsedValues.map(v => `(${this.escapeRegex(v)})`).join('|');
                    return { [mongoField]: { "$not": { "$regex": regexPattern, "$options": "i" } } };
                }

            case '*=':
                if (parsedValues.length === 1) {
                    return { [mongoField]: { "$regex": `^${this.escapeRegex(parsedValues[0])}`, "$options": "i" } };
                } else {
                    const regexPattern = parsedValues.map(v => `^${this.escapeRegex(v)}`).join('|');
                    return { [mongoField]: { "$regex": regexPattern, "$options": "i" } };
                }

            case '¬*=':
                if (parsedValues.length === 1) {
                    return { [mongoField]: { "$not": { "$regex": `^${this.escapeRegex(parsedValues[0])}`, "$options": "i" } } };
                } else {
                    const regexPattern = parsedValues.map(v => `^${this.escapeRegex(v)}`).join('|');
                    return { [mongoField]: { "$not": { "$regex": regexPattern, "$options": "i" } } };
                }

            case 'is':
                if (parsedValues[0] === 'true') {
                    return { [mongoField]: true };
                } else if (parsedValues[0] === 'false') {
                    return { [mongoField]: false };
                } else {
                    throw new Error(`Неверное значение для оператора 'is': ${parsedValues[0]}`);
                }

            default:
                this.warnings.push(`Строка ${lineNumber}: Неизвестный оператор '${operator}' в выражении`);
                return null;
        }
    }

    /**
     * Парсинг значений с учетом специальных конструкций
     * @param {string} values - строка значений
     * @param {string} type - тип условий
     * @returns {Array} массив значений
     */
    parseValues(values, type) {
        const result = [];
        
        // Разделяем значения по точке с запятой
        const valueParts = values.split(';').map(v => v.trim()).filter(v => v !== '');
        
        for (const part of valueParts) {
            const processedValue = this.processValue(part, type);
            if (processedValue !== null) {
                result.push(processedValue);
            }
        }
        
        return result;
    }

    /**
     * Обработка одного значения с учетом специальных конструкций
     * @param {string} value - значение
     * @param {string} type - тип условий
     * @returns {*} обработанное значение
     */
    processValue(value, type) {
        // Обработка пустого значения (∅)
        if (value === '∅') {
            return null;
        }

        // Обработка значений в фигурных скобках {fieldName}
        if (value.startsWith('{') && value.endsWith('}')) {
            const fieldName = value.slice(1, -1);
            if (type === 'evaluation') {
                return `$$d.${fieldName}`;
            } else {
                return `d.${fieldName}`;
            }
        }

        // Обработка значений в квадратных скобках [fieldName]
        if (value.startsWith('[') && value.endsWith(']')) {
            const fieldName = value.slice(1, -1);
            if (type === 'evaluation') {
                return { "$expr": { "$eq": [`$d.${fieldName}`, `$d.${fieldName}`] } };
            } else {
                return `d.${fieldName}`;
            }
        }

        // Обработка дат и времени (например: ([md_INN_recepient_reg_dt] + 730d))
        if (value.includes('+') || value.includes('-')) {
            return this.processDateExpression(value, type);
        }

        // Обработка числовых значений
        if (!isNaN(value) && !isNaN(parseFloat(value))) {
            return parseFloat(value);
        }

        // Обработка строковых значений
        return value;
    }

    /**
     * Обработка выражений с датами и временем
     * @param {string} value - значение с датой
     * @param {string} type - тип условий
     * @returns {Object} MongoDB выражение для даты
     */
    processDateExpression(value, type) {
        // Пример: ([md_INN_recepient_reg_dt] + 730d)
        const match = value.match(/\[([^\]]+)\]\s*([+-])\s*(\d+)([dhms])/);
        if (match) {
            const [, fieldName, operator, amount, unit] = match;
            const mongoOperator = operator === '+' ? '$dateAdd' : '$dateSubtract';
            
            return {
                [mongoOperator]: {
                    startDate: `$$d.${fieldName}`,
                    unit: this.convertTimeUnit(unit),
                    amount: parseInt(amount)
                }
            };
        }

        // Если не удалось распарсить, возвращаем как есть
        return value;
    }

    /**
     * Конвертация единиц времени
     * @param {string} unit - единица времени
     * @returns {string} MongoDB единица времени
     */
    convertTimeUnit(unit) {
        const units = {
            'd': 'day',
            'h': 'hour', 
            'm': 'minute',
            's': 'second'
        };
        return units[unit] || unit;
    }

    /**
     * Экранирование специальных символов для regex
     * @param {string} str - строка
     * @returns {string} экранированная строка
     */
    escapeRegex(str) {
        if (typeof str !== 'string') {
            return String(str);
        }
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    /**
     * Парсинг атрибутов агрегации с улучшенной обработкой
     * @param {string} attributesStr - строка атрибутов
     * @param {number} lineNumber - номер строки
     * @returns {Object} объект атрибутов MongoDB
     */
    parseAttributes(attributesStr, lineNumber) {
        if (!attributesStr || attributesStr.trim() === '') {
            return {
                cnt: { "$sum": 1 }
            };
        }

        const attributes = {};
        
        // Разбираем атрибуты по типу с более точным поиском
        const attributeTypes = {
            'frequency': 'cnt',
            'total amount': 'sum',
            'average amount': 'avg',
            'maximum amount': 'max',
            'minimum amount': 'min',
            'distinct values number': 'dst'
        };

        // Ищем типы атрибутов в строке (регистронезависимо)
        const lowerStr = attributesStr.toLowerCase();
        
        for (const [typeName, attrName] of Object.entries(attributeTypes)) {
            if (lowerStr.includes(typeName.toLowerCase())) {
                switch (attrName) {
                    case 'cnt':
                        attributes.cnt = { "$sum": 1 };
                        break;
                    case 'sum':
                        // Определяем поле для суммирования
                        const sumField = this.extractSumField(attributesStr);
                        attributes.sum = { "$sum": sumField };
                        break;
                    case 'avg':
                        // Определяем поле для усреднения
                        const avgField = this.extractAvgField(attributesStr);
                        attributes.avg = { "$avg": avgField };
                        break;
                    case 'max':
                        // Определяем поле для максимума
                        const maxField = this.extractMaxField(attributesStr);
                        attributes.max = { "$max": maxField };
                        break;
                    case 'min':
                        // Определяем поле для минимума
                        const minField = this.extractMinField(attributesStr);
                        attributes.min = { "$min": minField };
                        break;
                    case 'dst':
                        // Определяем поле для уникальных значений
                        const dstField = this.extractDstField(attributesStr);
                        attributes.dst = { "$addToSet": dstField };
                        break;
                }
            }
        }

        // Если атрибуты не найдены, добавляем базовый
        if (Object.keys(attributes).length === 0) {
            attributes.cnt = { "$sum": 1 };
        }

        return attributes;
    }

    /**
     * Извлечение поля для суммирования из строки атрибутов
     * @param {string} attributesStr - строка атрибутов
     * @returns {string} поле для суммирования
     */
    extractSumField(attributesStr) {
        // По умолчанию используем Amount
        if (attributesStr.toLowerCase().includes('amount')) {
            return "$d.Amount";
        }
        return "$d.a";
    }

    /**
     * Извлечение поля для усреднения из строки атрибутов
     * @param {string} attributesStr - строка атрибутов
     * @returns {string} поле для усреднения
     */
    extractAvgField(attributesStr) {
        // По умолчанию используем Amount
        if (attributesStr.toLowerCase().includes('amount')) {
            return "$d.Amount";
        }
        return "$d.a";
    }

    /**
     * Извлечение поля для максимума из строки атрибутов
     * @param {string} attributesStr - строка атрибутов
     * @returns {string} поле для максимума
     */
    extractMaxField(attributesStr) {
        // По умолчанию используем Amount
        if (attributesStr.toLowerCase().includes('amount')) {
            return "$d.Amount";
        }
        return "$d.a";
    }

    /**
     * Извлечение поля для минимума из строки атрибутов
     * @param {string} attributesStr - строка атрибутов
     * @returns {string} поле для минимума
     */
    extractMinField(attributesStr) {
        // По умолчанию используем Amount
        if (attributesStr.toLowerCase().includes('amount')) {
            return "$d.Amount";
        }
        return "$d.a";
    }

    /**
     * Извлечение поля для уникальных значений из строки атрибутов
     * @param {string} attributesStr - строка атрибутов
     * @returns {string} поле для уникальных значений
     */
    extractDstField(attributesStr) {
        // Пытаемся определить поле по контексту
        if (attributesStr.toLowerCase().includes('pan')) {
            return "$d.PAN";
        }
        if (attributesStr.toLowerCase().includes('client')) {
            return "$d.s_client_id";
        }
        if (attributesStr.toLowerCase().includes('token')) {
            return "$d.dPan";
        }
        // По умолчанию используем PAN
        return "$d.PAN";
    }
}

module.exports = CsvToJsonParser;
