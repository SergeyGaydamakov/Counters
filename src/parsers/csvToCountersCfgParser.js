const fs = require('fs');
const path = require('path');

/**
 * Парсер для преобразования counters.csv в countersCfg.json
 * Обрабатывает условия и преобразует их в MongoDB операторы
 */
class CsvToCountersCfgParser {
    constructor() {
        this.attributeMapping = {
            'average amount': { name: 'avg', accumulator: '$avg' },
            'distinct values number': { name: 'dst', accumulator: '$addToSet' },
            'frequency': { name: 'cnt', accumulator: '$sum' },
            'maximum amount': { name: 'max', accumulator: '$max' },
            'minimum amount': { name: 'min', accumulator: '$min' },
            'total amount': { name: 'sum', accumulator: '$sum' }
        };
        
        this.dateFields = ['Timestamp', 'reg_dt', 'created_at', 'updated_at'];
        this.booleanFields = ['pe_tb_client_private_flag', 'pe_vip_flag_3', 'IP_GSMflag'];
    }

    /**
     * Парсит CSV файл и преобразует в JSON конфигурацию
     * @param {string} csvFilePath - путь к CSV файлу
     * @param {string} outputPath - путь для сохранения JSON
     */
    async parseCsvToJson(csvFilePath, outputPath) {
        try {
            console.log(`Чтение файла: ${csvFilePath}`);
            const csvContent = fs.readFileSync(csvFilePath, 'utf8');
            
            const lines = this.parseCsvLines(csvContent);
            const headers = lines[0];
            
            console.log(`Найдено ${lines.length - 1} записей для обработки`);
            
            const counters = [];
            const errors = [];
            
            for (let i = 1; i < lines.length; i++) {
                try {
                    const row = lines[i];
                    const counter = this.parseCounterRow(headers, row, i + 1);
                    if (counter) {
                        counters.push(counter);
                    }
                } catch (error) {
                    errors.push(`Строка ${i + 1}: ${error.message}`);
                    console.error(`Ошибка в строке ${i + 1}:`, error.message);
                }
            }
            
            const result = {
                counters: counters,
                metadata: {
                    totalCounters: counters.length,
                    errors: errors,
                    generatedAt: new Date().toISOString()
                }
            };
            
            fs.writeFileSync(outputPath, JSON.stringify(result, null, 2), 'utf8');
            console.log(`Результат сохранен в: ${outputPath}`);
            console.log(`Обработано счетчиков: ${counters.length}`);
            console.log(`Ошибок: ${errors.length}`);
            
            if (errors.length > 0) {
                console.log('Ошибки:', errors);
            }
            
            return result;
            
        } catch (error) {
            console.error('Ошибка при парсинге:', error);
            throw error;
        }
    }

    /**
     * Парсит CSV строки с учетом экранирования кавычек
     */
    parseCsvLines(content) {
        const lines = [];
        let currentLine = '';
        let inQuotes = false;
        let i = 0;
        
        while (i < content.length) {
            const char = content[i];
            
            if (char === '"') {
                if (inQuotes && content[i + 1] === '"') {
                    // Экранированная кавычка
                    currentLine += '"';
                    i += 2;
                    continue;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (char === ';' && !inQuotes) {
                // Разделитель поля
                currentLine += '|FIELD_SEPARATOR|';
            } else if (char === '\n' && !inQuotes) {
                // Конец строки
                if (currentLine.trim()) {
                    lines.push(currentLine.split('|FIELD_SEPARATOR|'));
                }
                currentLine = '';
            } else {
                currentLine += char;
            }
            i++;
        }
        
        // Добавляем последнюю строку
        if (currentLine.trim()) {
            lines.push(currentLine.split('|FIELD_SEPARATOR|'));
        }
        
        return lines;
    }

    /**
     * Парсит одну строку счетчика
     */
    parseCounterRow(headers, row, lineNumber) {
        const data = {};
        
        // Создаем объект данных по заголовкам
        headers.forEach((header, index) => {
            data[header.trim()] = row[index] ? row[index].trim() : '';
        });
        
        // Проверяем обязательные поля
        if (!data.Name) {
            throw new Error('Отсутствует обязательное поле Name');
        }
        
        const counter = {
            name: data.Name,
            comment: data.Comment || `${data.Name} - счетчик`,
            indexTypeName: data.Index || 'idx_default',
            computationConditions: this.parseConditions(data['Computation Conditions'], 'computation', lineNumber),
            evaluationConditions: this.parseConditions(data['Evaluation Conditions'], 'evaluation', lineNumber),
            attributes: this.parseAttributes(data.Attributes, lineNumber)
        };
        
        return counter;
    }

    /**
     * Парсит условия из строки
     */
    parseConditions(conditionsStr, type, lineNumber) {
        if (!conditionsStr || conditionsStr.trim() === '') {
            return type === 'computation' ? {} : null;
        }
        
        try {
            // Убираем внешние кавычки если есть
            let cleanStr = conditionsStr.trim();
            if ((cleanStr.startsWith('"') && cleanStr.endsWith('"')) ||
                (cleanStr.startsWith("'") && cleanStr.endsWith("'"))) {
                cleanStr = cleanStr.slice(1, -1);
            }
            
            const conditions = {};
            const exprConditions = [];
            
            // Разбиваем на отдельные выражения в скобках
            const expressions = this.extractExpressions(cleanStr);
            
            for (const expr of expressions) {
                try {
                    const parsed = this.parseExpression(expr, type);
                    if (parsed.isExpr) {
                        exprConditions.push(parsed.condition);
                    } else {
                        Object.assign(conditions, parsed.condition);
                    }
                } catch (error) {
                    console.warn(`Строка ${lineNumber}: Не удалось распарсить выражение "${expr}": ${error.message}`);
                }
            }
            
            // Добавляем $expr условия если есть
            if (exprConditions.length > 0) {
                if (exprConditions.length === 1) {
                    conditions['$expr'] = exprConditions[0];
                } else {
                    conditions['$expr'] = { '$and': exprConditions };
                }
            }
            
            return Object.keys(conditions).length > 0 ? conditions : (type === 'computation' ? {} : null);
            
        } catch (error) {
            console.warn(`Строка ${lineNumber}: Ошибка парсинга условий "${conditionsStr}": ${error.message}`);
            return type === 'computation' ? {} : null;
        }
    }

    /**
     * Извлекает выражения из строки условий
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
                if (depth === 1) {
                    current = '';
                } else {
                    current += char;
                }
            } else if (char === ')') {
                depth--;
                if (depth === 0) {
                    expressions.push(current.trim());
                    current = '';
                } else {
                    current += char;
                }
            } else {
                current += char;
            }
            i++;
        }
        
        return expressions.filter(expr => expr.trim() !== '');
    }

    /**
     * Парсит одно выражение
     */
    parseExpression(expr, type) {
        // Обработка дат и времени
        const dateMatch = expr.match(/\(([^)]+)\s*([<>]=?)\s*\(\[([^)]+)\]\s*\+\s*(\d+)([dhms])\)\)/);
        if (dateMatch) {
            const [, field, operator, otherField, amount, unit] = dateMatch;
            const mongoOperator = this.getMongoOperator(operator);
            const unitMap = { d: 'day', h: 'hour', m: 'minute', s: 'second' };
            
            return {
                isExpr: false,
                condition: {
                    [`d.${field}`]: {
                        [mongoOperator]: {
                            "$dateAdd": {
                                "startDate": `$${otherField}`,
                                "unit": unitMap[unit],
                                "amount": parseInt(amount)
                            }
                        }
                    }
                }
            };
        }
        
        // Обработка выражений с OR оператором (|)
        if (expr.includes(' | ')) {
            return this.parseOrExpression(expr, type);
        }
        
        // Обработка обычных выражений
        const parts = expr.split(/\s+/);
        if (parts.length < 2) {
            throw new Error('Недостаточно частей в выражении');
        }
        
        const fieldName = parts[0];
        const operator = parts[1];
        const valuesStr = parts.slice(2).join(' ');
        
        // Обработка значений в фигурных или квадратных скобках
        if (valuesStr.includes('{') || valuesStr.includes('[')) {
            return this.parseFieldReference(fieldName, operator, valuesStr, type);
        }
        
        // Парсинг значений
        const values = this.parseValues(valuesStr);
        
        // Определение MongoDB оператора
        const mongoCondition = this.buildMongoCondition(fieldName, operator, values, type);
        
        return {
            isExpr: false,
            condition: mongoCondition
        };
    }

    /**
     * Парсит выражения с OR оператором
     */
    parseOrExpression(expr, type) {
        const orParts = expr.split(' | ');
        const conditions = [];
        
        for (const part of orParts) {
            try {
                const parsed = this.parseExpression(part.trim(), type);
                if (parsed.isExpr) {
                    conditions.push(parsed.condition);
                } else {
                    // Преобразуем в $expr для OR логики
                    const field = Object.keys(parsed.condition)[0];
                    const condition = parsed.condition[field];
                    conditions.push({ [field]: condition });
                }
            } catch (error) {
                console.warn(`Не удалось распарсить часть OR выражения "${part}": ${error.message}`);
            }
        }
        
        if (conditions.length === 0) {
            throw new Error('Не удалось распарсить ни одной части OR выражения');
        }
        
        return {
            isExpr: true,
            condition: { '$or': conditions }
        };
    }

    /**
     * Обрабатывает ссылки на поля в фигурных или квадратных скобках
     */
    parseFieldReference(fieldName, operator, valuesStr, type) {
        const curlyMatch = valuesStr.match(/\{([^}]+)\}/);
        const squareMatch = valuesStr.match(/\[([^\]]+)\]/);
        
        if (curlyMatch) {
            const otherField = curlyMatch[1];
            if (type === 'evaluation') {
                return {
                    isExpr: false,
                    condition: {
                        [`d.${fieldName}`]: `$$${otherField}`
                    }
                };
            } else {
                return {
                    isExpr: false,
                    condition: {
                        [`d.${fieldName}`]: `$d.${otherField}`
                    }
                };
            }
        }
        
        if (squareMatch) {
            const otherField = squareMatch[1];
            const mongoOperator = this.getMongoOperator(operator);
            return {
                isExpr: true,
                condition: {
                    [mongoOperator]: [`$d.${fieldName}`, `$d.${otherField}`]
                }
            };
        }
        
        throw new Error('Не удалось распарсить ссылку на поле');
    }

    /**
     * Парсит значения из строки
     */
    parseValues(valuesStr) {
        // Убираем точки с запятой и разбиваем
        const values = valuesStr.split(/[;,\s]+/)
            .map(v => v.trim())
            .filter(v => v !== '');
        
        return values.map(v => {
            // Обработка специальных значений
            if (v === '∅') {
                return null;
            }
            
            // Обработка boolean значений
            if (v === 'true' || v === 'false') {
                return v === 'true';
            }
            
            // Обработка чисел для MessageTypeID
            if (/^\d+$/.test(v)) {
                return parseInt(v);
            }
            
            // Обработка чисел с плавающей точкой
            if (/^\d+[,.]\d+$/.test(v)) {
                return parseFloat(v.replace(',', '.'));
            }
            
            return v;
        });
    }

    /**
     * Строит MongoDB условие
     */
    buildMongoCondition(fieldName, operator, values, type) {
        const mongoField = `d.${fieldName}`;
        
        // Специальная обработка для computation условий
        if (type === 'computation') {
            if (operator === '=' && values.length === 1) {
                return { [mongoField]: values[0] };
            } else if (operator === '=' && values.length > 1) {
                return { [mongoField]: values };
            }
        }
        
        const mongoOperator = this.getMongoOperator(operator);
        
        switch (operator) {
            case '=':
                if (values.length === 1) {
                    return { [mongoField]: values[0] };
                } else {
                    return { [mongoField]: { [mongoOperator]: values } };
                }
                
            case '≠':
                if (values.length === 1) {
                    return { [mongoField]: { [mongoOperator]: values[0] } };
                } else {
                    return { [mongoField]: { [mongoOperator]: values } };
                }
                
            case '=*=':
                // Содержит подстроку
                if (values.length === 1) {
                    return { [mongoField]: { "$regex": values[0], "$options": "i" } };
                } else {
                    return { [mongoField]: { "$regex": values.join('|'), "$options": "i" } };
                }
                
            case '¬=*=':
                // Не содержит подстроку
                if (values.length === 1) {
                    return { [mongoField]: { "$not": { "$regex": values[0], "$options": "i" } } };
                } else {
                    return { [mongoField]: { "$not": { "$regex": values.join('|'), "$options": "i" } } };
                }
                
            case '*=':
                // Начинается с
                if (values.length === 1) {
                    return { [mongoField]: { "$regex": `^${values[0]}`, "$options": "i" } };
                } else {
                    return { [mongoField]: { "$regex": `^(${values.join('|')})`, "$options": "i" } };
                }
                
            case '¬*=':
                // Не начинается с
                if (values.length === 1) {
                    return { [mongoField]: { "$not": { "$regex": `^${values[0]}`, "$options": "i" } } };
                } else {
                    return { [mongoField]: { "$not": { "$regex": `^(${values.join('|')})`, "$options": "i" } } };
                }
                
            case '=*':
                // Содержит подстроку (альтернативный синтаксис)
                if (values.length === 1) {
                    return { [mongoField]: { "$regex": values[0], "$options": "i" } };
                } else {
                    return { [mongoField]: { "$regex": values.join('|'), "$options": "i" } };
                }
                
            case '¬=*':
                // Не содержит подстроку (альтернативный синтаксис)
                if (values.length === 1) {
                    return { [mongoField]: { "$not": { "$regex": values[0], "$options": "i" } } };
                } else {
                    return { [mongoField]: { "$not": { "$regex": values.join('|'), "$options": "i" } } };
                }
                
            case '¬=':
                // Не равно (альтернативный синтаксис)
                if (values.length === 1) {
                    return { [mongoField]: { "$ne": values[0] } };
                } else {
                    return { [mongoField]: { "$nin": values } };
                }
                
            case 'is':
                // Boolean значения
                const boolValue = values[0] === 'true';
                return { [mongoField]: boolValue };
                
            default:
                // Операторы сравнения
                if (['>', '<', '>=', '≤', '≥'].includes(operator)) {
                    return { [mongoField]: { [mongoOperator]: values[0] } };
                }
                
                throw new Error(`Неизвестный оператор: ${operator}`);
        }
    }

    /**
     * Получает MongoDB оператор
     */
    getMongoOperator(operator) {
        const operatorMap = {
            '=': '$eq',
            '≠': '$ne',
            '>': '$gt',
            '<': '$lt',
            '>=': '$gte',
            '≤': '$lte',
            '≥': '$gte',
            '=*=': '$in',
            '¬=*=': '$nin',
            '*=': '$in',
            '¬*=': '$nin'
        };
        
        return operatorMap[operator] || operator;
    }

    /**
     * Парсит атрибуты счетчика
     */
    parseAttributes(attributesStr, lineNumber) {
        if (!attributesStr || attributesStr.trim() === '') {
            return { cnt: { "$sum": 1 } }; // По умолчанию frequency
        }
        
        const attributes = {};
        
        // Ищем атрибуты в скобках
        const attributeMatches = attributesStr.match(/\(([^)]+)\)/g);
        
        if (attributeMatches) {
            for (const match of attributeMatches) {
                const attrStr = match.slice(1, -1); // Убираем скобки
                const attrInfo = this.parseAttribute(attrStr);
                if (attrInfo) {
                    attributes[attrInfo.name] = attrInfo.accumulator;
                }
            }
        }
        
        // Если атрибуты не найдены, добавляем frequency по умолчанию
        if (Object.keys(attributes).length === 0) {
            attributes.cnt = { "$sum": 1 };
        }
        
        return attributes;
    }

    /**
     * Парсит один атрибут
     */
    parseAttribute(attrStr) {
        // Ищем соответствие в маппинге
        for (const [key, value] of Object.entries(this.attributeMapping)) {
            if (attrStr.includes(key)) {
                return {
                    name: value.name,
                    accumulator: this.buildAccumulator(value.accumulator, attrStr)
                };
            }
        }
        
        return null;
    }

    /**
     * Строит аккумулятор MongoDB
     */
    buildAccumulator(accumulatorType, attrStr) {
        switch (accumulatorType) {
            case '$sum':
                if (attrStr.includes('amount')) {
                    // Для amount полей используем сумму поля
                    return { "$sum": "$d.amount" };
                } else {
                    // Для frequency используем сумму 1
                    return { "$sum": 1 };
                }
                
            case '$avg':
                return { "$avg": "$d.amount" };
                
            case '$max':
                return { "$max": "$d.amount" };
                
            case '$min':
                return { "$min": "$d.amount" };
                
            case '$addToSet':
                // Для distinct values обычно используется PAN или другое поле
                return { "$addToSet": "$d.PAN" };
                
            default:
                return { "$sum": 1 };
        }
    }
}

// Экспорт для использования
module.exports = CsvToCountersCfgParser;

// Если файл запускается напрямую
if (require.main === module) {
    const parser = new CsvToCountersCfgParser();
    
    const csvFile = path.join(__dirname, '../../docs/counters.csv');
    const outputFile = path.join(__dirname, '../../countersCfg.json');
    
    parser.parseCsvToJson(csvFile, outputFile)
        .then(result => {
            console.log('Парсинг завершен успешно');
        })
        .catch(error => {
            console.error('Ошибка парсинга:', error);
            process.exit(1);
        });
}
