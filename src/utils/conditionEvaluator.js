const Logger = require('../common/logger');

class ConditionEvaluator {
    constructor(logger = null, debugMode = false) {
        this.logger = logger || Logger.fromEnv('LOG_LEVEL', 'INFO');
        this._debugMode = debugMode;
    }

    /**
     * Функция возвращает значение поля по пути с точками
     * 
     * @param {Object} obj - объект для обработки
     * @param {string} path - путь к полю c точками
     * @returns {any} значение поля
     */
    _getValueByPath(obj, path) {
        const fields = String(path).split('.');
        return fields.reduce((acc, field) => (acc ? acc[field] : undefined), obj);
    }

    /**
     * Проверяет, подходит ли факт под условие счетчика
     * @param {Object} fact - Факт для проверки
     * @param {Object} condition - Условие счетчика
     * @param {boolean} undefinedFieldIsTrue - true, если поле undefined
     * @returns {boolean} true, если факт подходит под условие
     */
    matchesCondition(fact, condition, undefinedFieldIsTrue = false) {
        if (!fact || !fact.d) {
            return false;
        }
        if (!condition) {
            return true;
        }
        for (const [field, expectedValue] of Object.entries(condition)) {
            if (field === '$expr') {
                if (!this._processExprExpression(fact, expectedValue)) {
                    return false;
                }
                continue;
            }
            const actualValue = this._getValueByPath(fact, field);
            if (Array.isArray(expectedValue)) {
                if (!expectedValue.includes(actualValue)) {
                    return false;
                }
            } else if (typeof expectedValue === 'object' && expectedValue !== null) {
                if (!this._matchesMongoOperator(actualValue, expectedValue, fact)) {
                    return false;
                }
            } else {
                // Делаем нечеткое сравнение, если напутали с типами данных
                if (actualValue != expectedValue && (!undefinedFieldIsTrue || actualValue !== undefined)) {
                    return false;
                }
            }
        }
        return true;
    }

    _matchesMongoOperator(actualValue, operator, fact = null) {
        for (const [op, opValue] of Object.entries(operator)) {
            switch (op) {
                case '$eq': {
                    const eqValue = opValue === '$$NOW' ? new Date() : opValue;
                    if (!this._compareValues(actualValue, eqValue, 'eq')) return false;
                    break;
                }
                case '$ne': {
                    if (typeof opValue === 'object' && opValue !== null && !Array.isArray(opValue)) {
                        const hasOperators = Object.keys(opValue).some(key => key.startsWith('$'));
                        if (hasOperators) {
                            return !this._matchesMongoOperator(actualValue, opValue, fact);
                        }
                    }
                    const neValue = opValue === '$$NOW' ? new Date() : opValue;
                    if (actualValue === neValue) return false;
                    break;
                }
                case '$gt': {
                    let gtValue = opValue;
                    if (opValue === '$$NOW') gtValue = new Date();
                    else if (typeof opValue === 'object' && opValue !== null) {
                        const dateResult = this._processDateOperator(opValue, fact);
                        if (dateResult !== null) gtValue = dateResult;
                    }
                    if (!this._compareValues(actualValue, gtValue, 'gt')) return false;
                    break;
                }
                case '$gte': {
                    let gteValue = opValue;
                    if (opValue === '$$NOW') gteValue = new Date();
                    else if (typeof opValue === 'object' && opValue !== null) {
                        const dateResult = this._processDateOperator(opValue, fact);
                        if (dateResult !== null) gteValue = dateResult;
                    }
                    if (!this._compareValues(actualValue, gteValue, 'gte')) return false;
                    break;
                }
                case '$lt': {
                    let ltValue = opValue;
                    if (opValue === '$$NOW') ltValue = new Date();
                    else if (typeof opValue === 'object' && opValue !== null) {
                        const dateResult = this._processDateOperator(opValue, fact);
                        if (dateResult !== null) ltValue = dateResult;
                    }
                    if (!this._compareValues(actualValue, ltValue, 'lt')) return false;
                    break;
                }
                case '$lte': {
                    let lteValue = opValue;
                    if (opValue === '$$NOW') lteValue = new Date();
                    else if (typeof opValue === 'object' && opValue !== null) {
                        const dateResult = this._processDateOperator(opValue, fact);
                        if (dateResult !== null) lteValue = dateResult;
                    }
                    if (!this._compareValues(actualValue, lteValue, 'lte')) return false;
                    break;
                }
                case '$in':
                    if (!Array.isArray(opValue) || !opValue.includes(actualValue)) return false;
                    break;
                case '$nin':
                    if (!Array.isArray(opValue) || opValue.includes(actualValue)) return false;
                    break;
                case '$all':
                    if (!Array.isArray(actualValue) || !Array.isArray(opValue)) return false;
                    if (!opValue.every(val => actualValue.includes(val))) return false;
                    break;
                case '$elemMatch':
                    if (!Array.isArray(actualValue)) return false;
                    if (!actualValue.some(elem => this._matchesMongoOperator(elem, opValue, fact))) return false;
                    break;
                case '$size':
                    if (!Array.isArray(actualValue) || actualValue.length !== opValue) return false;
                    break;
                case '$regex': {
                    if (typeof actualValue !== 'string') return false;
                    const options = operator.$options || '';
                    const regex = new RegExp(opValue, options);
                    if (!regex.test(actualValue)) return false;
                    break;
                }
                case '$options':
                    break;
                case '$not':
                    if (this._matchesMongoOperator(actualValue, opValue, fact)) return false;
                    break;
                case '$and':
                    if (!Array.isArray(opValue)) return false;
                    if (!opValue.every(condition => (typeof condition === 'object' && condition !== null)
                        ? this._matchesMongoOperator(actualValue, condition, fact)
                        : actualValue === condition)) return false;
                    break;
                case '$or':
                    if (!Array.isArray(opValue)) return false;
                    if (!opValue.some(condition => (typeof condition === 'object' && condition !== null)
                        ? this._matchesMongoOperator(actualValue, condition, fact)
                        : actualValue === condition)) return false;
                    break;
                case '$exists': {
                    const exists = actualValue !== undefined && actualValue !== null;
                    if (opValue && !exists) return false;
                    if (!opValue && exists) return false;
                    break;
                }
                case '$type': {
                    const actualType = this._getMongoDBType(actualValue);
                    if (actualType !== opValue) return false;
                    break;
                }
                case '$expr':
                    if (!fact) {
                        this.logger.warn('Оператор $expr требует факт для обработки');
                        return false;
                    }
                    return this._processExprExpression(fact, opValue);
                case '$mod': {
                    if (!Array.isArray(opValue) || opValue.length !== 2) return false;
                    const [divisor, remainder] = opValue;
                    if (typeof actualValue !== 'number' || actualValue % divisor !== remainder) return false;
                    break;
                }
                case '$where':
                    this.logger.warn('Оператор $where не поддерживается в computationConditions');
                    return false;
                case '$text':
                    this.logger.warn('Оператор $text не поддерживается в computationConditions');
                    return false;
                case '$geoWithin':
                case '$geoIntersects':
                case '$near':
                case '$nearSphere':
                    this.logger.warn(`Геолокационный оператор ${op} не поддерживается в computationConditions`);
                    return false;
                default:
                    this.logger.warn(`Неподдерживаемый MongoDB оператор: ${op}`);
                    return false;
            }
        }
        return true;
    }

    _processDateOperator(operator, fact = null) {
        if (!operator || typeof operator !== 'object') {
            return null;
        }
        for (const [op] of Object.entries(operator)) {
            switch (op) {
                case '$dateAdd':
                    return this._processDateAdd(operator.$dateAdd, fact);
                case '$dateSubtract':
                    return this._processDateSubtract(operator.$dateSubtract, fact);
                case '$dateDiff':
                    return this._processDateDiff(operator.$dateDiff, fact);
                default:
                    this.logger.warn(`Неподдерживаемый оператор даты: ${op}`);
                    return null;
            }
        }
        return null;
    }

    _processDateAdd(opValue, fact = null) {
        if (!opValue || typeof opValue !== 'object') return null;
        const { startDate, unit, amount } = opValue;
        if (!startDate || !unit || typeof amount !== 'number') return null;
        let baseDate;
        if (startDate === '$$NOW') baseDate = new Date();
        else if (typeof startDate === 'string' && startDate.startsWith('$')) {
            if (fact) {
                const fieldValue = this._extractFieldValue(fact, startDate);
                baseDate = fieldValue instanceof Date ? fieldValue : new Date(fieldValue);
            } else return null;
        } else baseDate = new Date(startDate);
        if (isNaN(baseDate.getTime())) return null;
        const result = new Date(baseDate);
        switch (unit) {
            case 'year': result.setFullYear(result.getFullYear() + amount); break;
            case 'month': result.setMonth(result.getMonth() + amount); break;
            case 'day': result.setDate(result.getDate() + amount); break;
            case 'hour': result.setHours(result.getHours() + amount); break;
            case 'minute': result.setMinutes(result.getMinutes() + amount); break;
            case 'second': result.setSeconds(result.getSeconds() + amount); break;
            case 'millisecond': result.setMilliseconds(result.getMilliseconds() + amount); break;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }
        return result;
    }

    _processDateSubtract(opValue, fact = null) {
        if (!opValue || typeof opValue !== 'object') return null;
        const { startDate, unit, amount } = opValue;
        if (!startDate || !unit || typeof amount !== 'number') return null;
        let baseDate;
        if (startDate === '$$NOW') baseDate = new Date();
        else if (typeof startDate === 'string' && startDate.startsWith('$')) {
            if (fact) {
                const fieldValue = this._extractFieldValue(fact, startDate);
                baseDate = fieldValue instanceof Date ? fieldValue : new Date(fieldValue);
            } else return null;
        } else baseDate = new Date(startDate);
        if (isNaN(baseDate.getTime())) return null;
        const result = new Date(baseDate);
        switch (unit) {
            case 'year': result.setFullYear(result.getFullYear() - amount); break;
            case 'month': result.setMonth(result.getMonth() - amount); break;
            case 'day': result.setDate(result.getDate() - amount); break;
            case 'hour': result.setHours(result.getHours() - amount); break;
            case 'minute': result.setMinutes(result.getMinutes() - amount); break;
            case 'second': result.setSeconds(result.getSeconds() - amount); break;
            case 'millisecond': result.setMilliseconds(result.getMilliseconds() - amount); break;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }
        return result;
    }

    _processDateDiff(opValue, fact = null) {
        if (!opValue || typeof opValue !== 'object') return null;
        const { startDate, endDate, unit } = opValue;
        if (!startDate || !endDate || !unit) return null;
        let start, end;
        if (startDate === '$$NOW') start = new Date();
        else if (typeof startDate === 'string' && startDate.startsWith('$')) {
            if (fact) {
                const fieldValue = this._extractFieldValue(fact, startDate);
                start = fieldValue instanceof Date ? fieldValue : new Date(fieldValue);
            } else return null;
        } else start = new Date(startDate);
        if (endDate === '$$NOW') end = new Date();
        else if (typeof endDate === 'string' && endDate.startsWith('$')) {
            if (fact) {
                const fieldValue = this._extractFieldValue(fact, endDate);
                end = fieldValue instanceof Date ? fieldValue : new Date(fieldValue);
            } else return null;
        } else end = new Date(endDate);
        if (isNaN(start.getTime()) || isNaN(end.getTime())) return null;
        const diffMs = end.getTime() - start.getTime();
        switch (unit) {
            case 'year': return Math.floor(diffMs / (365.25 * 24 * 60 * 60 * 1000));
            case 'month': return Math.floor(diffMs / (30.44 * 24 * 60 * 60 * 1000));
            case 'day': return Math.floor(diffMs / (24 * 60 * 60 * 1000));
            case 'hour': return Math.floor(diffMs / (60 * 60 * 1000));
            case 'minute': return Math.floor(diffMs / (60 * 1000));
            case 'second': return Math.floor(diffMs / 1000);
            case 'millisecond': return diffMs;
            default:
                this.logger.warn(`Неподдерживаемая единица времени: ${unit}`);
                return null;
        }
    }

    _compareValues(actualValue, expectedValue, operator) {
        const actual = this._parseValue(actualValue);
        const expected = this._parseValue(expectedValue);
        if (typeof actual === 'number' && typeof expected === 'number') {
            switch (operator) {
                case 'eq': return actual === expected;
                case 'gt': return actual > expected;
                case 'gte': return actual >= expected;
                case 'lt': return actual < expected;
                case 'lte': return actual <= expected;
                default: return false;
            }
        }
        if (typeof actual === 'string' && typeof expected === 'string') {
            switch (operator) {
                case 'eq': return actual === expected;
                case 'gt': return actual > expected;
                case 'gte': return actual >= expected;
                case 'lt': return actual < expected;
                case 'lte': return actual <= expected;
                default: return false;
            }
        }
        if (actual instanceof Date || expected instanceof Date) {
            const actualDate = actual instanceof Date ? actual : new Date(actual);
            const expectedDate = expected instanceof Date ? expected : new Date(expected);
            if (isNaN(actualDate.getTime()) || isNaN(expectedDate.getTime())) return false;
            switch (operator) {
                case 'eq': return actualDate.getTime() === expectedDate.getTime();
                case 'gt': return actualDate > expectedDate;
                case 'gte': return actualDate >= expectedDate;
                case 'lt': return actualDate < expectedDate;
                case 'lte': return actualDate <= expectedDate;
                default: return false;
            }
        }
        return false;
    }

    _parseValue(value) {
        if (typeof value === 'string') {
            const num = parseFloat(value.replace(',', '.'));
            if (!isNaN(num)) return num;
        }
        return value;
    }

    _getMongoDBType(value) {
        if (value === null) return 'null';
        if (value === undefined) return 'undefined';
        if (typeof value === 'boolean') return 'bool';
        if (typeof value === 'number') {
            if (Number.isInteger(value)) return 'int';
            return 'double';
        }
        if (typeof value === 'string') return 'string';
        if (Array.isArray(value)) return 'array';
        if (value instanceof Date) return 'date';
        if (typeof value === 'object') return 'object';
        return 'unknown';
    }

    _extractFieldValue(fact, fieldPath) {
        if (!fact || !fieldPath) return undefined;
        if (typeof fieldPath !== 'string') {
            this.logger.warn(`_extractFieldValue: fieldPath должен быть строкой, получен тип: ${typeof fieldPath}, значение: ${fieldPath}`);
            return undefined;
        }
        if (fieldPath === '$$NOW') return new Date();
        const cleanPath = fieldPath.startsWith('$') ? fieldPath.substring(1) : fieldPath;
        const pathParts = cleanPath.split('.');
        let current = fact;
        for (const part of pathParts) {
            if (current && typeof current === 'object' && part in current) current = current[part];
            else return undefined;
        }
        return current;
    }

    _processExprOperand(operand, fact) {
        if (typeof operand === 'string') {
            return this._extractFieldValue(fact, operand);
        } else if (typeof operand === 'number') {
            return operand;
        } else if (typeof operand === 'object' && operand !== null) {
            if (operand.$dateAdd) return this._processDateAdd(operand.$dateAdd, fact);
            else if (operand.$dateSubtract) return this._processDateSubtract(operand.$dateSubtract, fact);
            else if (operand.$dateDiff) return this._processDateDiff(operand.$dateDiff, fact);
            else {
                this.logger.warn(`_processExprOperand: неподдерживаемый объект: ${JSON.stringify(operand)}`);
                return null;
            }
        } else {
            this.logger.warn(`_processExprOperand: операнд должен быть строкой, числом или объектом, получен тип: ${typeof operand}`);
            return null;
        }
    }

    _processExprExpression(fact, exprValue) {
        if (!exprValue || typeof exprValue !== 'object' || Array.isArray(exprValue)) {
            this.logger.warn(`_processExprExpression: exprValue должен быть объектом, получен тип: ${typeof exprValue}, значение: ${exprValue}`);
            return false;
        }
        for (const [operator, operands] of Object.entries(exprValue)) {
            if (!Array.isArray(operands)) {
                this.logger.warn(`_processExprExpression: operands должен быть массивом, получен тип: ${typeof operands}, значение: ${operands}`);
                continue;
            }
            switch (operator) {
                case '$eq': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return value1 === value2;
                    }
                    break;
                }
                case '$ne': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return value1 !== value2;
                    }
                    break;
                }
                case '$gt': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return this._compareValues(value1, value2, 'gt');
                    }
                    break;
                }
                case '$gte': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return this._compareValues(value1, value2, 'gte');
                    }
                    break;
                }
                case '$lt': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return this._compareValues(value1, value2, 'lt');
                    }
                    break;
                }
                case '$lte': {
                    if (operands.length === 2) {
                        const [field1, field2] = operands;
                        const value1 = this._processExprOperand(field1, fact);
                        const value2 = this._processExprOperand(field2, fact);
                        return this._compareValues(value1, value2, 'lte');
                    }
                    break;
                }
                case '$and': {
                    return operands.every(condition => (typeof condition === 'object' && condition !== null)
                        ? this._processExprExpression(fact, condition)
                        : false);
                }
                case '$or': {
                    return operands.some(condition => (typeof condition === 'object' && condition !== null)
                        ? this._processExprExpression(fact, condition)
                        : false);
                }
                case '$dateAdd': {
                    if (typeof operands === 'object' && operands !== null) {
                        const result = this._processDateAdd(operands, fact);
                        return result !== null;
                    }
                    break;
                }
                case '$dateSubtract': {
                    if (typeof operands === 'object' && operands !== null) {
                        const result = this._processDateSubtract(operands, fact);
                        return result !== null;
                    }
                    break;
                }
                case '$dateDiff': {
                    if (typeof operands === 'object' && operands !== null) {
                        const result = this._processDateDiff(operands, fact);
                        return result !== null;
                    }
                    break;
                }
                default:
                    this.logger.warn(`Неподдерживаемый оператор в $expr: ${operator}`);
                    return false;
            }
        }
        return false;
    }
}

module.exports = ConditionEvaluator;


