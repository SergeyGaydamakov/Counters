const fs = require('fs');
const path = require('path');

class CountersCsvParser {
    constructor() {
        this.errors = [];
        this.warnings = [];
    }

    /**
     * Парсит CSV файл и преобразует его в JSON структуру
     * @param {string} csvFilePath - путь к CSV файлу
     * @param {string} outputFilePath - путь для сохранения JSON файла
     */
    parseCsvToJson(csvFilePath, outputFilePath) {
        try {
            console.log('Начинаем парсинг CSV файла...');
            
            // Читаем CSV файл
            const csvContent = fs.readFileSync(csvFilePath, 'utf8');
            const lines = csvContent.split('\n');
            
            if (lines.length < 2) {
                throw new Error('CSV файл должен содержать заголовок и хотя бы одну строку данных');
            }

            // Парсим заголовки
            const headers = this.parseCsvLine(lines[0]);
            console.log('Заголовки:', headers);

            const counters = [];
            
            // Парсим каждую строку данных
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (!line) continue;

                try {
                    const counter = this.parseCounterLine(line, headers, i + 1);
                    if (counter) {
                        counters.push(counter);
                    }
                } catch (error) {
                    this.addError(`Ошибка в строке ${i + 1}: ${error.message}`);
                    console.error(`Ошибка в строке ${i + 1}:`, error.message);
                }
            }

            // Создаем результирующий JSON
            const result = {
                counters: counters,
                metadata: {
                    totalCounters: counters.length,
                    errors: this.errors.length,
                    warnings: this.warnings.length,
                    generatedAt: new Date().toISOString()
                }
            };

            // Сохраняем результат
            fs.writeFileSync(outputFilePath, JSON.stringify(result, null, 2), 'utf8');
            
            console.log(`Парсинг завершен. Создано ${counters.length} счетчиков.`);
            console.log(`Ошибок: ${this.errors.length}, Предупреждений: ${this.warnings.length}`);
            
            if (this.errors.length > 0) {
                console.log('Ошибки:', this.errors);
            }
            if (this.warnings.length > 0) {
                console.log('Предупреждения:', this.warnings);
            }

            return result;

        } catch (error) {
            console.error('Критическая ошибка при парсинге:', error.message);
            throw error;
        }
    }

    /**
     * Парсит строку CSV с учетом экранирования кавычек
     */
    parseCsvLine(line) {
        const result = [];
        let current = '';
        let inQuotes = false;
        let i = 0;

        while (i < line.length) {
            const char = line[i];
            const nextChar = line[i + 1];

            if (char === '"') {
                if (inQuotes && nextChar === '"') {
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
                result.push(current.trim());
                current = '';
                i++;
            } else {
                current += char;
                i++;
            }
        }

        // Добавляем последнее поле
        result.push(current.trim());
        return result;
    }

    /**
     * Парсит одну строку счетчика
     */
    parseCounterLine(line, headers, lineNumber) {
        const fields = this.parseCsvLine(line);
        
        if (fields.length < headers.length) {
            this.addWarning(`Строка ${lineNumber}: недостаточно полей (${fields.length} из ${headers.length})`);
        }

        // Создаем объект с полями
        const counterData = {};
        headers.forEach((header, index) => {
            counterData[header] = fields[index] || '';
        });

        // Проверяем обязательные поля
        if (!counterData.Name) {
            this.addError(`Строка ${lineNumber}: отсутствует обязательное поле Name`);
            return null;
        }

        // Парсим условия
        const evaluationConditions = this.parseConditions(counterData['Evaluation Conditions'], 'evaluation', lineNumber);
        const computationConditions = this.parseConditions(counterData['Computation Conditions'], 'computation', lineNumber);
        
        // Парсим атрибуты
        const attributes = this.parseAttributes(counterData.Attributes, lineNumber);

        // Создаем объект счетчика
        const counter = {
            name: counterData.Name.replace(/\./g, '_'), // Заменяем точки на подчеркивания
            comment: counterData.Comment || `${counterData.Name} - счетчик`,
            indexTypeName: counterData.Index || 'idx_default',
            computationConditions: computationConditions,
            evaluationConditions: evaluationConditions,
            attributes: attributes
        };

        // Добавляем информацию об ошибках в комментарий, если есть
        if (this.errors.length > 0) {
            const recentErrors = this.errors.slice(-3); // Последние 3 ошибки
            counter.comment += ` [Ошибки парсинга: ${recentErrors.join('; ')}]`;
        }

        return counter;
    }

    /**
     * Парсит условия (Evaluation Conditions или Computation Conditions)
     */
    parseConditions(conditionsStr, type, lineNumber) {
        if (!conditionsStr || conditionsStr.trim() === '') {
            return null;
        }

        try {
            // Убираем внешние кавычки если есть
            let cleanConditions = conditionsStr.trim();
            if (cleanConditions.startsWith('"') && cleanConditions.endsWith('"')) {
                cleanConditions = cleanConditions.slice(1, -1);
            }

            const result = {};
            const expressions = this.splitExpressions(cleanConditions);

            for (const expr of expressions) {
                if (!expr.trim()) continue;

                const parsed = this.parseExpression(expr.trim(), type, lineNumber);
                if (parsed) {
                    Object.assign(result, parsed);
                }
            }

            return Object.keys(result).length > 0 ? result : null;

        } catch (error) {
            this.addError(`Строка ${lineNumber}: ошибка парсинга условий ${type}: ${error.message}`);
            return null;
        }
    }

    /**
     * Разделяет строку условий на отдельные выражения
     */
    splitExpressions(conditionsStr) {
        const expressions = [];
        let current = '';
        let parenCount = 0;
        let i = 0;

        while (i < conditionsStr.length) {
            const char = conditionsStr[i];
            
            if (char === '(') {
                parenCount++;
                current += char;
            } else if (char === ')') {
                parenCount--;
                current += char;
                
                if (parenCount === 0) {
                    expressions.push(current.trim());
                    current = '';
                }
            } else if (char === ';' && parenCount === 0) {
                if (current.trim()) {
                    expressions.push(current.trim());
                }
                current = '';
            } else {
                current += char;
            }
            i++;
        }

        if (current.trim()) {
            expressions.push(current.trim());
        }

        return expressions;
    }

    /**
     * Парсит одно выражение условия
     */
    parseExpression(expr, type, lineNumber) {
        // Убираем скобки
        if (expr.startsWith('(') && expr.endsWith(')')) {
            expr = expr.slice(1, -1);
        }

        // Сначала проверяем на арифметику с датами (приоритетная проверка)
        const dateArithmeticMatch = expr.match(/^(.+?)\s*([<>≥≤])\s*\(\[(.+?)\]\s*([+-])\s*(\d+)([dhm])\)$/);
        if (dateArithmeticMatch) {
            return this.parseDateArithmeticOperator(dateArithmeticMatch, type, lineNumber);
        }

        // Проверяем на простую арифметику с датами (без скобок)
        const simpleDateArithmeticMatch = expr.match(/^(.+?)\s*([<>≥≤])\s*\[(.+?)\]\s*([+-])\s*(\d+)([dhm])$/);
        if (simpleDateArithmeticMatch) {
            return this.parseDateArithmeticOperator(simpleDateArithmeticMatch, type, lineNumber);
        }

        // Ищем оператор (порядок важен - более специфичные операторы должны быть первыми)
        const operators = [
            { pattern: /^(.+?)\s*is\s+(true|false)\s*$/, handler: this.parseBooleanOperator },
            { pattern: /^(.+?)\s*¬=\*=\s*(.+)$/, handler: this.parseNotContainsOperator },
            { pattern: /^(.+?)\s*=\*=\s*(.+)$/, handler: this.parseContainsOperator },
            { pattern: /^(.+?)\s*¬\*=\s*(.+)$/, handler: this.parseNotStartsWithOperator },
            { pattern: /^(.+?)\s*\*=\s*(.+)$/, handler: this.parseStartsWithOperator },
            { pattern: /^(.+?)\s*¬≈\s*(.+)$/, handler: this.parseNotApproximatelyEqualsOperator },
            { pattern: /^(.+?)\s*≈\s*(.+)$/, handler: this.parseApproximatelyEqualsOperator },
            { pattern: /^(.+?)\s*≠\s*(.+)$/, handler: this.parseNotEqualsOperator },
            { pattern: /^(.+?)\s*=\s*(.+)$/, handler: this.parseEqualsOperator },
            { pattern: /^(.+?)\s*>\s*(.+)$/, handler: this.parseGreaterThanOperator },
            { pattern: /^(.+?)\s*≥\s*(.+)$/, handler: this.parseGreaterOrEqualOperator },
            { pattern: /^(.+?)\s*<\s*(.+)$/, handler: this.parseLessThanOperator },
            { pattern: /^(.+?)\s*≤\s*(.+)$/, handler: this.parseLessOrEqualOperator },
            { pattern: /^(.+?)\s*=====\*\s*(.+)$/, handler: this.parseDateEqualsOperator },
            { pattern: /^(.+?)\s*=\s*=\s*=\s*=\s*\*\s*(.+)$/, handler: this.parseDateEqualsOperator }
        ];

        for (const op of operators) {
            const match = expr.match(op.pattern);
            if (match) {
                return op.handler.call(this, match[1].trim(), match[2].trim(), type, lineNumber);
            }
        }

        this.addWarning(`Строка ${lineNumber}: неизвестный оператор в выражении: ${expr}`);
        return null;
    }

    /**
     * Парсит оператор is true/false
     */
    parseBooleanOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const boolValue = value.toLowerCase() === 'true';
        
        if (type === 'computation') {
            return { [`d.${cleanFieldName}`]: boolValue };
        } else {
            return { [`d.${cleanFieldName}`]: boolValue };
        }
    }

    /**
     * Парсит оператор =*= (содержит подстроку)
     */
    parseContainsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$regex": values[0], "$options": "i" } };
        } else {
            // Объединяем множественные значения в один regex с OR
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `(${escapedValues.join('|')})`;
            return { [`d.${cleanFieldName}`]: { "$regex": combinedRegex, "$options": "i" } };
        }
    }

    /**
     * Парсит оператор ¬=*= (не содержит подстроку)
     */
    parseNotContainsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": values[0], "$options": "i" } } };
        } else {
            // Объединяем множественные значения в один regex с OR и применяем $not
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `(${escapedValues.join('|')})`;
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": combinedRegex, "$options": "i" } } };
        }
    }

    /**
     * Парсит оператор *= (начинается с)
     */
    parseStartsWithOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$regex": `^${this.escapeRegex(values[0])}`, "$options": "i" } };
        } else {
            // Объединяем множественные значения в один regex с OR и якорем начала
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `^(${escapedValues.join('|')})`;
            return { [`d.${cleanFieldName}`]: { "$regex": combinedRegex, "$options": "i" } };
        }
    }

    /**
     * Парсит оператор ¬*= (не начинается с)
     */
    parseNotStartsWithOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": `^${this.escapeRegex(values[0])}`, "$options": "i" } } };
        } else {
            // Объединяем множественные значения в один regex с OR и якорем начала, применяем $not
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `^(${escapedValues.join('|')})`;
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": combinedRegex, "$options": "i" } } };
        }
    }

    /**
     * Парсит оператор ≈ (приблизительно равно)
     */
    parseApproximatelyEqualsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        // Проверяем на ссылки на другие поля
        if (value.startsWith('[') && value.endsWith(']')) {
            const refField = value.slice(1, -1);
            if (type === 'evaluation') {
                return { "$expr": { "$eq": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            } else {
                return { "$expr": { "$eq": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            }
        }

        // Для обычных значений используем regex с игнорированием регистра
        const values = this.parseValueList(value);
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$regex": `^${this.escapeRegex(values[0])}$`, "$options": "i" } };
        } else {
            // Объединяем множественные значения в один regex с OR и якорями начала/конца
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `^(${escapedValues.join('|')})$`;
            return { [`d.${cleanFieldName}`]: { "$regex": combinedRegex, "$options": "i" } };
        }
    }

    /**
     * Парсит оператор ¬≈ (не приблизительно равно)
     */
    parseNotApproximatelyEqualsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        // Проверяем на ссылки на другие поля
        if (value.startsWith('[') && value.endsWith(']')) {
            const refField = value.slice(1, -1);
            if (type === 'evaluation') {
                return { "$expr": { "$ne": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            } else {
                return { "$expr": { "$ne": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            }
        }

        // Для обычных значений используем $not с regex
        const values = this.parseValueList(value);
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": `^${this.escapeRegex(values[0])}$`, "$options": "i" } } };
        } else {
            // Объединяем множественные значения в один regex с OR и якорями начала/конца, применяем $not
            const escapedValues = values.map(v => this.escapeRegex(v));
            const combinedRegex = `^(${escapedValues.join('|')})$`;
            return { [`d.${cleanFieldName}`]: { "$not": { "$regex": combinedRegex, "$options": "i" } } };
        }
    }

    /**
     * Парсит оператор = (равно)
     */
    parseEqualsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        // Проверяем на ссылки на другие поля
        if (value.startsWith('{') && value.endsWith('}')) {
            const refField = value.slice(1, -1);
            if (type === 'computation') {
                return { [`d.${cleanFieldName}`]: `$d.${refField}` };
            } else {
                return { [`d.${cleanFieldName}`]: `$$d.${refField}` };
            }
        }
        
        if (value.startsWith('[') && value.endsWith(']')) {
            const refField = value.slice(1, -1);
            if (type === 'evaluation') {
                return { "$expr": { "$eq": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            } else {
                return { "$expr": { "$eq": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            }
        }

        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);

        if (type === 'computation') {
            if (values.length === 1) {
                return { [`d.${cleanFieldName}`]: typedValues[0] };
            } else {
                return { [`d.${cleanFieldName}`]: typedValues };
            }
        } else {
            if (values.length === 1) {
                return { [`d.${cleanFieldName}`]: typedValues[0] };
            } else {
                return { [`d.${cleanFieldName}`]: { "$in": typedValues } };
            }
        }
    }

    /**
     * Парсит оператор ≠ (не равно)
     */
    parseNotEqualsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        // Проверяем на ссылки на другие поля
        if (value.startsWith('{') && value.endsWith('}')) {
            const refField = value.slice(1, -1);
            if (type === 'computation') {
                return { [`d.${cleanFieldName}`]: { "$ne": `$d.${refField}` } };
            } else {
                return { [`d.${cleanFieldName}`]: { "$ne": `$$d.${refField}` } };
            }
        }
        
        if (value.startsWith('[') && value.endsWith(']')) {
            const refField = value.slice(1, -1);
            if (type === 'evaluation') {
                return { "$expr": { "$ne": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            } else {
                return { "$expr": { "$ne": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
            }
        }

        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);

        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$ne": typedValues[0] } };
        } else {
            return { [`d.${cleanFieldName}`]: { "$nin": typedValues } };
        }
    }

    /**
     * Парсит оператор > (больше)
     */
    parseGreaterThanOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$gt": typedValues[0] } };
        } else {
            // Для множественных значений используем $or
            const conditions = typedValues.map(v => ({ [`d.${cleanFieldName}`]: { "$gt": v } }));
            return { "$or": conditions };
        }
    }

    /**
     * Парсит оператор ≥ (больше или равно)
     */
    parseGreaterOrEqualOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$gte": typedValues[0] } };
        } else {
            const conditions = typedValues.map(v => ({ [`d.${cleanFieldName}`]: { "$gte": v } }));
            return { "$or": conditions };
        }
    }

    /**
     * Парсит оператор < (меньше)
     */
    parseLessThanOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$lt": typedValues[0] } };
        } else {
            const conditions = typedValues.map(v => ({ [`d.${cleanFieldName}`]: { "$lt": v } }));
            return { "$or": conditions };
        }
    }

    /**
     * Парсит оператор ≤ (меньше или равно)
     */
    parseLessOrEqualOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        const values = this.parseValueList(value);
        const typedValues = this.convertValuesToType(values, cleanFieldName);
        
        if (values.length === 1) {
            return { [`d.${cleanFieldName}`]: { "$lte": typedValues[0] } };
        } else {
            const conditions = typedValues.map(v => ({ [`d.${cleanFieldName}`]: { "$lte": v } }));
            return { "$or": conditions };
        }
    }

    /**
     * Парсит оператор ====* (равенство дат)
     */
    parseDateEqualsOperator(fieldName, value, type, lineNumber) {
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        if (value.startsWith('{') && value.endsWith('}')) {
            const refField = value.slice(1, -1);
            return { "$expr": { "$eq": [`$d.${cleanFieldName}`, `$d.${refField}`] } };
        }
        
        return { [`d.${cleanFieldName}`]: value };
    }

    /**
     * Парсит арифметические операции с датами
     */
    parseDateArithmeticOperator(match, type, lineNumber) {
        const [, fieldName, operator, refField, operation, amount, unit] = match;
        const cleanFieldName = this.cleanFieldName(fieldName);
        
        const mongoOperator = this.getMongoOperator(operator);
        const dateOperation = operation === '+' ? '$dateAdd' : '$dateSubtract';
        
        return {
            "$expr": {
                [mongoOperator]: [
                    `$d.${cleanFieldName}`,
                    {
                        [dateOperation]: {
                            "startDate": `$d.${refField}`,
                            "unit": this.convertTimeUnit(unit),
                            "amount": parseInt(amount)
                        }
                    }
                ]
            }
        };
    }

    /**
     * Парсит список значений, разделенных точкой с запятой
     */
    parseValueList(valueStr) {
        return valueStr.split(';').map(v => v.trim()).filter(v => v !== '');
    }

    /**
     * Очищает имя поля от лишних символов
     */
    cleanFieldName(fieldName) {
        // Убираем символы отрицания из названия поля
        return fieldName.replace(/¬\s*/g, '').trim();
    }

    /**
     * Конвертирует значения в соответствующие типы
     */
    convertValuesToType(values, fieldName) {
        return values.map(value => {
            // Специальная обработка для пустого значения
            if (value === '∅') {
                return null;
            }
            
            // MessageTypeID всегда числа
            if (fieldName === 'MessageTypeID') {
                return parseInt(value);
            }
            
            // Поля с суффиксом _flag, _indicator - boolean
            if (fieldName.endsWith('_flag') || fieldName.endsWith('_indicator')) {
                return value.toLowerCase() === 'true' || value === '1';
            }
            
            // Поля с датами
            if (this.isDateField(fieldName)) {
                return value;
            }
            
            // Проверяем, является ли значение числом с разделителями разрядов
            const parsedNumber = this.parseNumberWithSeparators(value);
            if (parsedNumber !== value && !isNaN(parsedNumber)) {
                return parsedNumber;
            }
            
            // Попытка конвертировать в число для числовых полей
            if (this.isNumericField(fieldName)) {
                const numValue = parseFloat(value.replace(',', '.'));
                if (!isNaN(numValue)) {
                    return numValue;
                }
            }
            
            return value;
        });
    }

    /**
     * Преобразует число с разделителями разрядов в числовой тип
     */
    parseNumberWithSeparators(value) {
        // Исключаем IP адреса
        if (/^\d+\.\d+\.\d+\.\d+$/.test(value)) {
            return value; // Возвращаем как строку
        }
        
        // Простые числа с запятой как десятичным разделителем: 100000,00 (2 знака после запятой)
        if (/^\d+,\d{2}$/.test(value)) {
            return parseFloat(value.replace(',', '.'));
        }
        
        // Числа с точкой как разделителем миллионов и запятой как разделителем тысяч: 6.000,000 (3 знака после запятой)
        if (/^\d{1,3}(\.\d{3})*,\d{3}$/.test(value)) {
            // Убираем точки (разделители миллионов) и запятые (разделители тысяч)
            return parseFloat(value.replace(/[.,]/g, ''));
        }
        
        // Числа с запятой как разделителем тысяч и точкой как десятичным разделителем: 1,000.50
        if (/^\d{1,3}(,\d{3})*\.\d+$/.test(value)) {
            // Убираем запятые (разделители тысяч)
            return parseFloat(value.replace(/,/g, ''));
        }
        
        return value; // Если не распознано как число, возвращаем как есть
    }

    /**
     * Проверяет, является ли поле числовым
     */
    isNumericField(fieldName) {
        const numericFields = ['Amount', 'Resolution', 'authRC', 'payCheckResult', 'terminalId'];
        return numericFields.some(field => fieldName.includes(field)) || 
               /^\d+$/.test(fieldName) ||
               fieldName.includes('amount') ||
               fieldName.includes('count') ||
               fieldName.includes('sum');
    }

    /**
     * Проверяет, является ли поле датой
     */
    isDateField(fieldName) {
        const dateFields = ['Timestamp', 'reg_dt', 'created_at', 'updated_at', 'trxnDate'];
        return dateFields.some(field => fieldName.includes(field)) || 
               fieldName.toLowerCase().includes('date') ||
               fieldName.toLowerCase().includes('time');
    }

    /**
     * Конвертирует единицы времени
     */
    convertTimeUnit(unit) {
        const unitMap = {
            'd': 'day',
            'h': 'hour', 
            'm': 'minute',
            's': 'second'
        };
        return unitMap[unit] || 'day';
    }

    /**
     * Получает MongoDB оператор
     */
    getMongoOperator(operator) {
        const operatorMap = {
            '>': '$gt',
            '<': '$lt',
            '≥': '$gte',
            '≤': '$lte'
        };
        return operatorMap[operator] || '$gt';
    }

    /**
     * Экранирует специальные символы для regex
     */
    escapeRegex(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    /**
     * Парсит атрибуты счетчика
     */
    parseAttributes(attributesStr, lineNumber) {
        if (!attributesStr || attributesStr.trim() === '') {
            return { "cnt": { "$sum": 1 } }; // По умолчанию только счетчик
        }

        const attributes = {};
        const attributeList = attributesStr.split(',').map(a => a.trim());

        for (const attr of attributeList) {
            const parsed = this.parseSingleAttribute(attr, lineNumber);
            if (parsed) {
                Object.assign(attributes, parsed);
            }
        }

        return Object.keys(attributes).length > 0 ? attributes : { "cnt": { "$sum": 1 } };
    }

    /**
     * Парсит один атрибут
     */
    parseSingleAttribute(attrStr, lineNumber) {
        // Убираем скобки если есть
        const cleanAttr = attrStr.replace(/[()]/g, '').trim();
        
        const attributeMap = {
            'frequency': { 'cnt': { '$sum': 1 } },
            'total amount': { 'sum': { '$sum': '$d.Amount' } },
            'average amount': { 'avg': { '$avg': '$d.Amount' } },
            'maximum amount': { 'max': { '$max': '$d.Amount' } },
            'minimum amount': { 'min': { '$min': '$d.Amount' } },
            'distinct values number': { 'dst': { '$addToSet': '$d.PAN' } },
            'most occurrence ratio': { 'cnt': { '$sum': 1 }, 'dst': { '$addToSet': '$d.Amount' } }
        };

        // Ищем точное совпадение
        for (const [key, value] of Object.entries(attributeMap)) {
            if (cleanAttr.toLowerCase().includes(key.toLowerCase())) {
                return value;
            }
        }

        // Если не найдено точное совпадение, пытаемся извлечь из названия
        if (cleanAttr.includes('frequency') || cleanAttr.includes('cnt')) {
            return { 'cnt': { '$sum': 1 } };
        }
        if (cleanAttr.includes('sum') || cleanAttr.includes('total')) {
            return { 'sum': { '$sum': '$d.Amount' } };
        }
        if (cleanAttr.includes('avg') || cleanAttr.includes('average')) {
            return { 'avg': { '$avg': '$d.Amount' } };
        }
        if (cleanAttr.includes('max') || cleanAttr.includes('maximum')) {
            return { 'max': { '$max': '$d.Amount' } };
        }
        if (cleanAttr.includes('min') || cleanAttr.includes('minimum')) {
            return { 'min': { '$min': '$d.Amount' } };
        }
        if (cleanAttr.includes('distinct') || cleanAttr.includes('dst')) {
            return { 'dst': { '$addToSet': '$d.PAN' } };
        }

        this.addWarning(`Строка ${lineNumber}: неизвестный тип атрибута: ${cleanAttr}`);
        return { 'cnt': { '$sum': 1 } };
    }

    /**
     * Добавляет ошибку
     */
    addError(message) {
        this.errors.push(message);
    }

    /**
     * Добавляет предупреждение
     */
    addWarning(message) {
        this.warnings.push(message);
    }
}

// Экспорт для использования в других модулях
module.exports = CountersCsvParser;

// Если файл запускается напрямую
if (require.main === module) {
    const parser = new CountersCsvParser();
    
    const csvFilePath = path.join(__dirname, '../../docs/counters.csv');
    const outputFilePath = path.join(__dirname, '../../countersCfg.json');
    
    try {
        parser.parseCsvToJson(csvFilePath, outputFilePath);
        console.log('Парсинг завершен успешно!');
    } catch (error) {
        console.error('Ошибка при парсинге:', error.message);
        process.exit(1);
    }
}
