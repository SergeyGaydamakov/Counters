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
        this.testProductionIndexConfig('9. Тест на основе indexConfig.json...');
        this.testProductionCountersConfig('10. Тест на основе countersConfig.json...');
        this.testComplexConditions('11. Тест сложных условий...');

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
     * Тест на основе примеров из indexConfig.json
     */
    testProductionIndexConfig(title) {
        this.logger.info(title);
        try {
            // Конфигурация полей из production
            const config = [
                { src: 'field1', dst: 'some_long_field1', shortDst: 'a01', message_types: [1] },
                { src: 'field2', dst: 'MessageTypeID', shortDst: 'a42', message_types: [1] },
                { src: 'field3', dst: 'some_long_field2', shortDst: 'c67', message_types: [1] },
                { src: 'field4', dst: 'some_long_field3', shortDst: 'a5a', message_types: [1] },
                { src: 'field5', dst: 'deviceId_CRC32', shortDst: 'b2c', message_types: [1] },
                { src: 'field6', dst: 'IP', shortDst: 'b90', message_types: [1] },
                { src: 'field7', dst: 'some_long_field8', shortDst: 'd84', message_types: [1] }
            ];
            
            const mapper = new FieldNameMapper(config, true);
            
            // Тест 1: Простое условие с MessageTypeID (из indexConfig.json строка 10)
            const condition1 = {
                'd.MessageTypeID': {
                    '$lt': 100
                }
            };
            const transformed1 = mapper.transformCondition(condition1);
            this.assert(transformed1['d.a42'] !== undefined, 'Преобразование d.MessageTypeID -> d.a42 (indexConfig пример 1)');
            this.assert(transformed1['d.a42']['$lt'] === 100, 'Сохранение оператора $lt для MessageTypeID');
            
            // Тест 2: Условие с MessageTypeID и операторами $lt и $gt (из indexConfig.json строка 24)
            const condition2 = {
                'd.MessageTypeID': {
                    '$lt': 100,
                    '$gt': 9
                }
            };
            const transformed2 = mapper.transformCondition(condition2);
            this.assert(transformed2['d.a42'] !== undefined, 'Преобразование d.MessageTypeID с множественными операторами');
            this.assert(transformed2['d.a42']['$lt'] === 100 && transformed2['d.a42']['$gt'] === 9, 'Сохранение множественных операторов');
            
            // Тест 3: Условие с some_long_field2 и $in (из indexConfig.json строка 39)
            const condition3 = {
                'd.some_long_field2': {'$in': ['TT', 'T', 'C', 'TO', 'P', 'N']}
            };
            const transformed3 = mapper.transformCondition(condition3);
            this.assert(transformed3['d.c67'] !== undefined, 'Преобразование d.some_long_field2 -> d.c67');
            this.assert(Array.isArray(transformed3['d.c67']['$in']) && transformed3['d.c67']['$in'].length === 6, 'Сохранение массива $in для some_long_field2');
            
            // Тест 4: Условие с $nin (из indexConfig.json строка 60)
            const condition4 = {
                'd.deviceId_CRC32': {'$nin': ['bd75645f', '00000000']}
            };
            const transformed4 = mapper.transformCondition(condition4);
            this.assert(transformed4['d.b2c'] !== undefined, 'Преобразование d.deviceId_CRC32 -> d.b2c');
            this.assert(transformed4['d.b2c']['$nin'].includes('bd75645f'), 'Сохранение $nin оператора');
            
            // Тест 5: Комбинированное условие с MessageTypeID и IP (из indexConfig.json строка 96)
            const condition5 = {
                'd.MessageTypeID': {
                    '$nin': [1, 102]
                },
                'd.IP': {'$nin': ['0.0.0.0']}
            };
            const transformed5 = mapper.transformCondition(condition5);
            this.assert(transformed5['d.a42'] !== undefined && transformed5['d.b90'] !== undefined, 'Преобразование множественных полей в одном условии');
            this.assert(transformed5['d.a42']['$nin'].includes(1), 'Сохранение условий для MessageTypeID');
            this.assert(transformed5['d.b90']['$nin'].includes('0.0.0.0'), 'Сохранение условий для IP');
            
            // Тест 6: Условие с $exists (из indexConfig.json строка 192)
            const condition6 = {
                'd.some_long_field8': {'$exists': true}
            };
            const transformed6 = mapper.transformCondition(condition6);
            this.assert(transformed6['d.d84'] !== undefined, 'Преобразование d.some_long_field8 -> d.d84');
            this.assert(transformed6['d.d84']['$exists'] === true, 'Сохранение оператора $exists');
            
        } catch (error) {
            this.assert(false, `Ошибка в testProductionIndexConfig: ${error.message}`);
        }
    }

    /**
     * Тест на основе примеров из countersConfig.json
     */
    testProductionCountersConfig(title) {
        this.logger.info(title);
        try {
            // Конфигурация полей из production
            const config = [
                { src: 'field1', dst: 'MessageTypeID', shortDst: 'a42', message_types: [1] },
                { src: 'field2', dst: 'some_long_field2', shortDst: 'c67', message_types: [1] },
                { src: 'field3', dst: 'some_long_field4', shortDst: 'a4b', message_types: [1] },
                { src: 'field4', dst: 'some_long_field5', shortDst: 'c83', message_types: [1] },
                { src: 'field5', dst: 'some_long_field6', shortDst: 'd20', message_types: [1] },
                { src: 'field6', dst: 'some_long_field7', shortDst: 'c94', message_types: [1] },
                { src: 'field7', dst: 'some_long_field8', shortDst: 'd84', message_types: [1] },
                { src: 'field8', dst: 'some_long_field9', shortDst: 'b90', message_types: [1] },
                { src: 'field9', dst: 'some_long_field3', shortDst: 'a5a', message_types: [1] }
            ];
            
            const mapper = new FieldNameMapper(config, true);
            
            // Тест 1: Простое computationConditions (из countersConfig.json строка 6-8)
            const computation1 = {
                'd.MessageTypeID': 61,
                'd.some_long_field2': 'CI'
            };
            const transformed1 = mapper.transformCondition(computation1);
            this.assert(transformed1['d.a42'] === 61, 'Преобразование d.MessageTypeID -> d.a42 в computationConditions');
            this.assert(transformed1['d.c67'] === 'CI', 'Преобразование d.some_long_field2 -> d.c67 в computationConditions');
            
            // Тест 2: evaluationConditions с $in (из countersConfig.json строка 11-13)
            const evaluation1 = {
                'd.MessageTypeID': {
                    '$in': [61, 50]
                },
                'd.some_long_field2': 'CI',
                'd.some_long_field9': {
                    '$regex': 'atm88.1',
                    '$options': 'i'
                }
            };
            const transformed2 = mapper.transformCondition(evaluation1);
            this.assert(transformed2['d.a42']['$in'].includes(61), 'Преобразование $in для MessageTypeID в evaluationConditions');
            this.assert(transformed2['d.c67'] === 'CI', 'Преобразование some_long_field2 в evaluationConditions');
            this.assert(transformed2['d.b90']['$regex'] === 'atm88.1', 'Преобразование some_long_field9 с regex в evaluationConditions');
            
            // Тест 3: computationConditions с $ne (из countersConfig.json строка 37-42)
            const computation2 = {
                'd.MessageTypeID': 61,
                'd.some_long_field2': 'CI',
                'd.some_long_field6': {
                    '$ne': 'Card'
                },
                'd.some_long_field7': {
                    '$ne': '1'
                }
            };
            const transformed3 = mapper.transformCondition(computation2);
            this.assert(transformed3['d.d20']['$ne'] === 'Card', 'Преобразование some_long_field6 с $ne');
            this.assert(transformed3['d.c94']['$ne'] === '1', 'Преобразование some_long_field7 с $ne');
            
            // Тест 4: computationConditions с null и $expr (из countersConfig.json строка 72-75)
            const computation3 = {
                'd.MessageTypeID': 61,
                'd.some_long_field2': 'CI',
                'd.some_long_field8': null,
                '$expr': {
                    '$ne': ['$d.some_long_field4', '$d.some_long_field5']
                }
            };
            const transformed4 = mapper.transformCondition(computation3);
            this.assert(transformed4['d.d84'] === null, 'Сохранение null значения для some_long_field8');
            this.assert(transformed4['$expr'] !== undefined, 'Сохранение $expr выражения');
            
            // Тест 5: attributes с $sum (из countersConfig.json строка 21-23)
            const attributes1 = {
                'cnt': {
                    '$sum': 1
                }
            };
            const transformed5 = mapper.transformAttributes(attributes1);
            this.assert(transformed5['cnt']['$sum'] === 1, 'Сохранение константного значения в attributes');
            
        } catch (error) {
            this.assert(false, `Ошибка в testProductionCountersConfig: ${error.message}`);
        }
    }

    /**
     * Тест сложных условий с вложенными структурами
     */
    testComplexConditions(title) {
        this.logger.info(title);
        try {
            // Конфигурация полей из production
            const config = [
                { src: 'field1', dst: 'MessageTypeID', shortDst: 'a42', message_types: [1] },
                { src: 'field2', dst: 'some_long_field3', shortDst: 'a5a', message_types: [1] },
                { src: 'field3', dst: 'some_long_field4', shortDst: 'a4b', message_types: [1] },
                { src: 'field4', dst: 'some_long_field5', shortDst: 'c83', message_types: [1] }
            ];
            
            const mapper = new FieldNameMapper(config, true);
            
            // Тест 1: Сложное $expr выражение с преобразованием полей
            const expr1 = {
                '$ne': ['$d.some_long_field4', '$d.some_long_field5']
            };
            const transformed1 = mapper.transformExprExpression(expr1);
            this.assert(transformed1['$ne'][0] === '$d.a4b', 'Преобразование $d.some_long_field4 -> $d.a4b в $expr');
            this.assert(transformed1['$ne'][1] === '$d.c83', 'Преобразование $d.some_long_field5 -> $d.c83 в $expr');
            
            // Тест 2: $expr с $dateAdd и преобразованным полем
            const expr2 = {
                '$gte': [
                    '$d.some_long_field3',
                    {
                        '$dateAdd': {
                            'startDate': '$$NOW',
                            'unit': 'day',
                            'amount': -700
                        }
                    }
                ]
            };
            const transformed2 = mapper.transformExprExpression(expr2);
            this.assert(transformed2['$gte'][0] === '$d.a5a', 'Преобразование $d.some_long_field3 -> $d.a5a в $expr с $dateAdd');
            this.assert(transformed2['$gte'][1]['$dateAdd'] !== undefined, 'Сохранение структуры $dateAdd');
            
            // Тест 3: Комбинированное условие с $expr и обычными полями
            const condition1 = {
                'd.MessageTypeID': {
                    '$in': [61, 50]
                },
                '$expr': {
                    '$gte': [
                        '$d.some_long_field3',
                        {
                            '$dateAdd': {
                                'startDate': '$$NOW',
                                'unit': 'hour',
                                'amount': -1
                            }
                        }
                    ]
                }
            };
            const transformed3 = mapper.transformCondition(condition1);
            this.assert(transformed3['d.a42']['$in'].includes(61), 'Сохранение $in в комбинированном условии');
            this.assert(transformed3['$expr']['$gte'][0] === '$d.a5a', 'Преобразование в $expr внутри комбинированного условия');
            
            // Тест 4: Attributes с множественными операторами
            const attributes1 = {
                'cnt': { '$sum': 1 },
                'sum_amount': { '$sum': '$d.MessageTypeID' },
                'count_distinct': { '$addToSet': '$d.some_long_field4' },
                'avg_amount': { '$avg': '$d.MessageTypeID' }
            };
            const transformed4 = mapper.transformAttributes(attributes1);
            this.assert(transformed4['cnt']['$sum'] === 1, 'Сохранение константного значения');
            this.assert(transformed4['sum_amount']['$sum'] === '$d.a42', 'Преобразование в $sum атрибуте');
            this.assert(transformed4['count_distinct']['$addToSet'] === '$d.a4b', 'Преобразование в $addToSet атрибуте');
            this.assert(transformed4['avg_amount']['$avg'] === '$d.a42', 'Преобразование в $avg атрибуте');
            
            // Тест 5: Вложенные условия с $and
            const condition2 = {
                '$and': [
                    { 'd.MessageTypeID': { '$gte': 50 } },
                    { 'd.MessageTypeID': { '$lte': 70 } },
                    { 'd.some_long_field4': { '$exists': true } }
                ]
            };
            const transformed5 = mapper.transformCondition(condition2);
            this.assert(transformed5['$and'] !== undefined && Array.isArray(transformed5['$and']), 'Обработка $and оператора');
            this.assert(transformed5['$and'].length === 3, 'Сохранение количества условий в $and');
            const firstCondition = transformed5['$and'][0];
            this.assert(firstCondition['d.a42'] !== undefined, 'Преобразование полей внутри $and');
            
            // Тест 6: Условие с $or
            const condition3 = {
                '$or': [
                    { 'd.MessageTypeID': 61 },
                    { 'd.MessageTypeID': 50 }
                ]
            };
            const transformed6 = mapper.transformCondition(condition3);
            this.assert(transformed6['$or'] !== undefined && Array.isArray(transformed6['$or']), 'Обработка $or оператора');
            this.assert(transformed6['$or'][0]['d.a42'] === 61, 'Преобразование в первом условии $or');
            this.assert(transformed6['$or'][1]['d.a42'] === 50, 'Преобразование во втором условии $or');
            
            // Тест 7: Условие с $not и regex
            const condition4 = {
                'd.some_long_field4': {
                    '$not': {
                        '$regex': '^0+$',
                        '$options': 'i'
                    }
                }
            };
            const transformed7 = mapper.transformCondition(condition4);
            this.assert(transformed7['d.a4b']['$not'] !== undefined, 'Обработка $not оператора');
            this.assert(transformed7['d.a4b']['$not']['$regex'] === '^0+$', 'Сохранение regex в $not');
            
        } catch (error) {
            this.assert(false, `Ошибка в testComplexConditions: ${error.message}`);
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

