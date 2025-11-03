const FieldNameMapper = require('../generators/fieldNameMapper');
const Logger = require('../utils/logger');

/**
 * Тесты для модуля FieldNameMapper
 */
class FieldNameMapperTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
    }

    /**
     * Вспомогательный метод для проверки условий
     */
    assert(condition, message) {
        if (condition) {
            this.logger.info(`✓ ${message}`);
            this.testResults.passed++;
        } else {
            this.logger.error(`✗ ${message}`);
            this.testResults.failed++;
            this.testResults.errors.push(message);
        }
    }

    /**
     * Запускает все тесты
     */
    runAllTests() {
        this.logger.info('=== Запуск тестов FieldNameMapper ===\n');

        this.testConstructor('1. Тест конструктора...');
        this.testGetFieldName('2. Тест getFieldName...');
        this.testTransformFieldPath('3. Тест transformFieldPath...');
        this.testTransformMongoPath('4. Тест transformMongoPath...');
        this.testTransformCondition('5. Тест transformCondition...');
        this.testTransformExprExpression('6. Тест transformExprExpression...');
        this.testTransformAttributes('7. Тест transformAttributes...');
        this.testValidation('8. Тест валидации shortDst...');

        this.printResults();
    }

    /**
     * Тест конструктора
     */
    testConstructor(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] },
                { src: 'field2', dst: 'long_field_two', shortDst: 'f2', message_types: [1] }
            ];
            
            const mapper = new FieldNameMapper(config, false);
            this.assert(mapper instanceof FieldNameMapper, 'Конструктор создает экземпляр FieldNameMapper');
            this.assert(!mapper.useShortNames, 'useShortNames установлен в false');
            
            const mapper2 = new FieldNameMapper(config, true);
            this.assert(mapper2 instanceof FieldNameMapper, 'Конструктор с useShortNames=true');
            this.assert(mapper2.useShortNames, 'useShortNames установлен в true');
        } catch (error) {
            this.assert(false, `Ошибка при создании FieldNameMapper: ${error.message}`);
        }
    }

    /**
     * Тест getFieldName
     */
    testGetFieldName(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] },
                { src: 'field2', dst: 'long_field_two', shortDst: 'f2', message_types: [1] }
            ];
            
            const mapperFalse = new FieldNameMapper(config, false);
            this.assert(mapperFalse.getFieldName('long_field_one') === 'long_field_one', 'getFieldName возвращает dst при useShortNames=false');
            
            const mapperTrue = new FieldNameMapper(config, true);
            this.assert(mapperTrue.getFieldName('long_field_one') === 'f1', 'getFieldName возвращает shortDst при useShortNames=true');
            this.assert(mapperTrue.getFieldName('long_field_two') === 'f2', 'getFieldName возвращает shortDst для второго поля');
            this.assert(mapperTrue.getFieldName('unknown_field') === 'unknown_field', 'getFieldName возвращает исходное имя для неизвестного поля');
        } catch (error) {
            this.assert(false, `Ошибка в testGetFieldName: ${error.message}`);
        }
    }

    /**
     * Тест transformFieldPath
     */
    testTransformFieldPath(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] }
            ];
            
            const mapperFalse = new FieldNameMapper(config, false);
            this.assert(mapperFalse.transformFieldPath('d.long_field_one') === 'd.long_field_one', 'transformFieldPath не изменяет путь при useShortNames=false');
            
            const mapperTrue = new FieldNameMapper(config, true);
            this.assert(mapperTrue.transformFieldPath('d.long_field_one') === 'd.f1', 'transformFieldPath преобразует путь при useShortNames=true');
            this.assert(mapperTrue.transformFieldPath('long_field_one') === 'long_field_one', 'transformFieldPath не изменяет путь без префикса d.');
            this.assert(mapperTrue.transformFieldPath('t') === 't', 'transformFieldPath не изменяет верхнеуровневые поля');
        } catch (error) {
            this.assert(false, `Ошибка в testTransformFieldPath: ${error.message}`);
        }
    }

    /**
     * Тест transformMongoPath
     */
    testTransformMongoPath(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] },
                { src: 'field2', dst: 'amount', shortDst: 'amt', message_types: [1] }
            ];
            
            const mapperTrue = new FieldNameMapper(config, true);
            this.assert(mapperTrue.transformMongoPath('$d.long_field_one') === '$d.f1', 'transformMongoPath преобразует $d.path');
            this.assert(mapperTrue.transformMongoPath('$d.amount') === '$d.amt', 'transformMongoPath преобразует $d.amount');
            this.assert(mapperTrue.transformMongoPath('$d.unknown') === '$d.unknown', 'transformMongoPath не изменяет неизвестные поля');
            
            const mapperFalse = new FieldNameMapper(config, false);
            this.assert(mapperFalse.transformMongoPath('$d.long_field_one') === '$d.long_field_one', 'transformMongoPath не изменяет при useShortNames=false');
        } catch (error) {
            this.assert(false, `Ошибка в testTransformMongoPath: ${error.message}`);
        }
    }

    /**
     * Тест transformCondition
     */
    testTransformCondition(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] },
                { src: 'amount', dst: 'transaction_amount', shortDst: 'amt', message_types: [1] }
            ];
            
            const mapperTrue = new FieldNameMapper(config, true);
            
            const condition1 = {
                'd.long_field_one': { '$in': ['value1', 'value2'] },
                't': [1, 2]
            };
            const transformed1 = mapperTrue.transformCondition(condition1);
            this.assert(transformed1['d.f1'] !== undefined, 'transformCondition преобразует d.long_field_one в d.f1');
            this.assert(transformed1['t'] !== undefined && transformed1['t'].length === 2, 'transformCondition сохраняет верхнеуровневые поля');
            
            const condition2 = {
                'd.transaction_amount': { '$gte': 100 },
                '$expr': {
                    '$gte': ['$d.transaction_amount', 100]
                }
            };
            const transformed2 = mapperTrue.transformCondition(condition2);
            this.assert(transformed2['d.amt'] !== undefined, 'transformCondition преобразует d.transaction_amount в d.amt');
        } catch (error) {
            this.assert(false, `Ошибка в testTransformCondition: ${error.message}`);
        }
    }

    /**
     * Тест transformExprExpression
     */
    testTransformExprExpression(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'amount', dst: 'transaction_amount', shortDst: 'amt', message_types: [1] },
                { src: 'date', dst: 'transaction_date', shortDst: 'dt', message_types: [1] }
            ];
            
            const mapperTrue = new FieldNameMapper(config, true);
            
            const expr1 = {
                '$gte': ['$d.transaction_amount', 100]
            };
            const transformed1 = mapperTrue.transformExprExpression(expr1);
            this.assert(Array.isArray(transformed1['$gte']), 'transformExprExpression возвращает массив для $gte');
            this.assert(transformed1['$gte'][0] === '$d.amt', 'transformExprExpression преобразует $d.transaction_amount в $d.amt');
            
            const expr2 = {
                '$gte': [
                    '$d.transaction_date',
                    {
                        '$dateAdd': {
                            'startDate': '$$NOW',
                            'unit': 'day',
                            'amount': -7
                        }
                    }
                ]
            };
            const transformed2 = mapperTrue.transformExprExpression(expr2);
            this.assert(transformed2['$gte'][0] === '$d.dt', 'transformExprExpression преобразует дату в $expr');
        } catch (error) {
            this.assert(false, `Ошибка в testTransformExprExpression: ${error.message}`);
        }
    }

    /**
     * Тест transformAttributes
     */
    testTransformAttributes(title) {
        this.logger.info(title);
        try {
            const config = [
                { src: 'amount', dst: 'transaction_amount', shortDst: 'amt', message_types: [1] }
            ];
            
            const mapperTrue = new FieldNameMapper(config, true);
            
            const attributes = {
                'sum_amount': { '$sum': '$d.transaction_amount' },
                'avg_amount': { '$avg': '$d.transaction_amount' },
                'max_amount': { '$max': '$d.transaction_amount' }
            };
            const transformed = mapperTrue.transformAttributes(attributes);
            this.assert(transformed['sum_amount']['$sum'] === '$d.amt', 'transformAttributes преобразует $sum');
            this.assert(transformed['avg_amount']['$avg'] === '$d.amt', 'transformAttributes преобразует $avg');
            this.assert(transformed['max_amount']['$max'] === '$d.amt', 'transformAttributes преобразует $max');
        } catch (error) {
            this.assert(false, `Ошибка в testTransformAttributes: ${error.message}`);
        }
    }

    /**
     * Тест валидации shortDst
     */
    testValidation(title) {
        this.logger.info(title);
        try {
            const configWithShortDst = [
                { src: 'field1', dst: 'long_field_one', shortDst: 'f1', message_types: [1] }
            ];
            
            const mapper1 = new FieldNameMapper(configWithShortDst, true);
            this.assert(mapper1 instanceof FieldNameMapper, 'Валидация проходит для конфигурации с shortDst');
            
            const configWithoutShortDst = [
                { src: 'field1', dst: 'long_field_one', message_types: [1] }
            ];
            
            let errorThrown = false;
            try {
                new FieldNameMapper(configWithoutShortDst, true);
            } catch (error) {
                errorThrown = true;
                this.assert(error.message.includes('shortDst'), 'Валидация выбрасывает ошибку при отсутствии shortDst');
            }
            this.assert(errorThrown, 'Валидация выбрасывает ошибку при useShortNames=true и отсутствии shortDst');
        } catch (error) {
            this.assert(false, `Ошибка в testValidation: ${error.message}`);
        }
    }

    /**
     * Выводит результаты тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования FieldNameMapper ===');
        this.logger.info(`Пройдено: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.failed === 0) {
            const successRate = ((this.testResults.passed / (this.testResults.passed + this.testResults.failed)) * 100).toFixed(2);
            this.logger.info(`\nПроцент успешности: ${successRate}%`);
            this.logger.info('🎉 Все тесты прошли успешно!');
        } else {
            this.logger.error(`\nОшибки:\n${this.testResults.errors.join('\n')}`);
        }
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new FieldNameMapperTest();
    test.runAllTests();
}

module.exports = FieldNameMapperTest;

