const fs = require('fs');
const Logger = require('../utils/logger');
const config = require('../common/config');
const ConditionEvaluator = require('../common/conditionEvaluator');
const FieldNameMapper = require('./fieldNameMapper');

/**
 * Класс для создания счетчиков на основе конфигурации и фактов
 * Типы счетчиков:
 * average amount - средняя сумма (аккумулятор $avg)
 * distinct values number - количество уникальных значений (аккумулятор $addToSet)
 * frequency - частота (аккумулятор $sum: 1)
 * maximum amount - максимальная сумма (аккумулятор $max)
 * total amount - общая сумма (аккумулятор $sum)
 * 
 * Пока не рассматриваем, следующие счетчики:
 * most occurrence ratio - коэффициент наиболее частого появления
 * most occurrence value - наиболее часто встречающееся значение
 * 
 * Параметры счетчиков:
 * Параметр счетчика - это строка, начинающаяся с "$$" и содержащая имя поля факта, например: "$$f2"
 * 
 * CounterProducer анализирует факты и определяет, какие счетчики должны быть применены
 * на основе условий в конфигурации. Результатом является структура для использования
 * в MongoDB aggregate запросе с оператором $facet.
 * 
 * Поддерживаемые MongoDB операторы в computationConditions:
 * - Операторы сравнения: $eq, $ne, $gt, $gte, $lt, $lte
 * - Операторы для массивов: $in, $nin, $all, $elemMatch, $size
 * - Операторы для строк: $regex, $options
 * - Логические операторы: $not, $and, $or
 * - Операторы существования: $exists
 * - Операторы типов: $type
 * - Операторы модуло: $mod
 * - Операторы выражений: $expr (с поддержкой полей факта)
 * - Неподдерживаемые операторы: $where, $text, геолокационные операторы
 * 
 * Структура конфигурации счетчика:
 * @property {string} name - Имя счетчика
 * @property {string} comment - Комментарий к счетчику
 * @property {string} indexTypeName - Название типа индекса
 * @property {Object} computationConditions - Условия применения счетчика к факту
 * @property {Object} evaluationConditions - Операции MongoDB aggregate для счетчика
 * @property {Object} attributes - Атрибуты счетчика (агрегированные значения)
 */
class CounterProducer {
    constructor(configPathOrConfigArray = null, useShortNames = false, messageConfig = null) {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        this._debugMode= config.logging.debugMode;
        this._undefinedFieldIsTrue = config.undefinedFieldIsTrue;
        this._counterConfig = [];
        this._counterConfigByType = {};
        this._EvaluationConditionsByType = {};
        this._conditionEvaluator = new ConditionEvaluator(this.logger, this._debugMode);
        this._useShortNames = useShortNames;
        
        // Инициализируем FieldNameMapper
        this.fieldNameMapper = new FieldNameMapper(messageConfig, useShortNames);
        
        if (!configPathOrConfigArray) {
            this.logger.warn('Конфигурация счетчиков не задана. Счетчики не будут создаваться.');
            return;
        }
        
        // Определяем способ инициализации
        if (Array.isArray(configPathOrConfigArray)) {
            // Инициализация через массив конфигурации
            const validatedConfig = this._validateConfig(configPathOrConfigArray);
            this._counterConfig = this._transformCounterConfig(validatedConfig);
            if (config.facts.splitIntervals) {
                this._counterConfig = this._splitIntervals(this._counterConfig, config.facts.splitIntervals);
            }
            this.logger.info(`Конфигурация счетчиков инициализирована объектом. Количество счетчиков: ${this._counterConfig.length}`);
        } else if (typeof configPathOrConfigArray === 'string') {
            // Инициализация через путь к файлу
            const loadedConfig = this._loadConfig(configPathOrConfigArray);
            this._counterConfig = this._transformCounterConfig(loadedConfig);
            if (config.facts.splitIntervals) {
                this._counterConfig = this._splitIntervals(this._counterConfig, config.facts.splitIntervals);
            }
        } else {
            this.logger.warn('Конфигурация счетчиков не задана. Счетчики будут создаваться по умолчанию.');
            return;
        }
        
        if (useShortNames) {
            this.logger.info('Используются короткие имена полей (shortDst) для счетчиков');
        }
    }

    /**
     * Преобразует конфигурацию счетчиков с учетом настройки useShortNames
     * @param {Array} counterConfig - Конфигурация счетчиков
     * @returns {Array} Преобразованная конфигурация
     * @private
     */
    _transformCounterConfig(counterConfig) {
        if (!this._useShortNames || !this.fieldNameMapper) {
            return counterConfig;
        }
        
        return counterConfig.map(counter => {
            const transformed = { ...counter };
            
            // Преобразуем computationConditions
            if (counter.computationConditions && typeof counter.computationConditions === 'object') {
                transformed.computationConditions = this.fieldNameMapper.transformCondition(counter.computationConditions);
            }
            
            // Преобразуем evaluationConditions
            if (counter.evaluationConditions && typeof counter.evaluationConditions === 'object') {
                transformed.evaluationConditions = this.fieldNameMapper.transformCondition(counter.evaluationConditions);
            }
            
            // Преобразуем attributes
            if (counter.attributes && typeof counter.attributes === 'object') {
                transformed.attributes = this.fieldNameMapper.transformAttributes(counter.attributes);
            }
            
            return transformed;
        });
    }
    
    /**
     * Загружает конфигурацию счетчиков из файла
     * @param {string} configPath - Путь к файлу конфигурации
     * @returns {Array} Массив конфигураций счетчиков
     * @throws {Error} если файл конфигурации не найден или содержит неверный формат
     */
    _loadConfig(configPath) {
        try {
            if (!fs.existsSync(configPath)) {
                throw new Error(`Файл конфигурации счетчиков не найден: ${configPath}`);
            }

            const configData = fs.readFileSync(configPath, 'utf8');
            const counterConfig = JSON.parse(configData);

            // Валидация структуры конфигурации
            this._validateConfig(counterConfig);

            this.logger.info(`Загружена конфигурация счетчиков из ${configPath}`);
            this.logger.info(`Количество счетчиков: ${counterConfig.length}`);
            return counterConfig;
        } catch (error) {
            this.logger.error(`Ошибка загрузки конфигурации счетчиков: ${error.message}`);
            throw error;
        }
    }

    /**
     * Валидирует структуру загруженной конфигурации счетчиков
     * @param {Array} counterConfig - Конфигурация счетчиков для валидации
     * @throws {Error} если конфигурация имеет неверный формат
     */
    _validateConfig(counterConfig) {
        if (!Array.isArray(counterConfig)) {
            throw new Error('Конфигурация счетчиков должна быть массивом объектов');
        }

        for (let i = 0; i < counterConfig.length; i++) {
            const counter = counterConfig[i];
            
            if (!counter || typeof counter !== 'object') {
                throw new Error(`Счетчик ${counter.name} должен быть объектом`);
            }

            if (!counter.name || typeof counter.name !== 'string') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'name' типа string`);
            }

            if (!counter.computationConditions || typeof counter.computationConditions !== 'object') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'computationConditions' типа object`);
            }
            // Проверяем, что computationConditions содержит валидные MongoDB операции
            for (let j = 0; j < counter.computationConditions.length; j++) {
                const stage = counter.computationConditions[j];
                if (!stage || typeof stage !== 'object') {
                    throw new Error(`Счетчик ${counter.name}, этап computationConditions ${j} должен быть объектом`);
                }
            }

            if (counter.evaluationConditions === undefined || typeof counter.evaluationConditions !== 'object') {
                throw new Error(`Счетчик ${counter.name} должен содержать поле 'evaluationConditions' типа object`);
            }

        }
        
        this.logger.info(`Конфигурация счетчиков валидна. Количество счетчиков: ${counterConfig.length}`);
        return counterConfig;
    }

    /**
     * Разбивает один счетчик по интервалам
     * @param {Array} counters - счетчики
     * @param {Array} intervals - массив чисел, отсортированный по возрастанию, на которые нужно разбить диапазон fromTimeMs, toTimeMs счетчика (относительное время в прошлом от текущего времени)
     * @returns {Array} Преобразованная конфигурация
     * @private
     */
    _splitIntervals(counterConfig, intervals) {
        const result = [];

        for (const counter of counterConfig) {
            // Если у счетчика нет fromTimeMs или toTimeMs, добавляем его как есть
            if (counter.fromTimeMs === undefined && counter.toTimeMs === undefined) {
                result.push(counter);
                continue;
            }

            const fromTimeMs = counter.fromTimeMs ?? 0;
            const toTimeMs = counter.toTimeMs ?? 0;

            // Если fromTimeMs <= toTimeMs, нет смысла разбивать (некорректный интервал или нулевой)
            if (fromTimeMs <= toTimeMs) {
                result.push(counter);
                continue;
            }

            // Находим все границы из intervals, которые попадают в диапазон [toTimeMs, fromTimeMs]
            const relevantBoundaries = intervals.filter(boundary => 
                boundary > toTimeMs && (fromTimeMs === 0 || boundary < fromTimeMs)
            );

            // Если нет подходящих границ, добавляем счетчик как есть
            if (relevantBoundaries.length === 0) {
                result.push(counter);
                continue;
            }

            // Сортируем границы по возрастанию (они уже должны быть отсортированы, но на всякий случай)
            relevantBoundaries.sort((a, b) => a - b);

            // Создаем список всех границ: [fromTimeMs, ...границы..., toTimeMs]
            const allBoundaries = [toTimeMs, ...relevantBoundaries, fromTimeMs];

            // Разбиваем интервал на подынтервалы
            for (let i = 0; i < allBoundaries.length - 1; i++) {
                const intervalTo = allBoundaries[i];
                const intervalFrom = allBoundaries[i + 1];

                // Создаем копию счетчика с новыми границами
                const splitCounter = {
                    ...counter,
                    toTimeMs: intervalTo,
                    fromTimeMs: intervalFrom
                };

                // Добавляем суффикс к имени счетчика для идентификации интервала
                if (allBoundaries.length > 2) {
                    splitCounter.name = `${counter.name}#${i}`;
                }

                result.push(splitCounter);
            }
        }

        return result;
    }

    /**
     * Старый метод. Создает структуру счетчиков для факта
     * @param {Object} fact - Факт для обработки
     * @returns {Object|null} Объект с полем facetStages, или null если нет подходящих счетчиков
     * @returns {Object} facetStages - Структура для использования в MongoDB $facet aggregate запросе
     */
    make(fact) {
        if (!fact || !fact.d) {
            this.logger.warn('Передан некорректный факт для создания счетчиков');
            return null;
        }

        const facetStages = {};
        let matchedCountersCount = 0;
        let matchedIndexTypeNames = new Set();

        // Проходим по всем счетчикам и проверяем условия
        for (const counter of this._counterConfig) {
            if (this._conditionEvaluator.matchesCondition(fact, counter.computationConditions)) {
                if (!counter.attributes) {
                    this.logger.warn(`Счетчик '${counter.name}' не имеет атрибутов (attributes). Счетчик не будет добавлен.`);
                    continue;
                }
                const matchStage = counter.evaluationConditions ? { "$match": counter.evaluationConditions } : null;
                const groupStage = { "$group": counter.attributes };
                groupStage["$group"]["_id"] = null;
                facetStages[counter.name] = [];
                if (matchStage) {
                    facetStages[counter.name].push(matchStage);
                }
                if (groupStage) {
                    facetStages[counter.name].push(groupStage);
                }
                matchedIndexTypeNames.add(counter.indexTypeName);
                matchedCountersCount++;
                this.logger.debug(`Счетчик '${counter.name}' подходит для факта ${fact._id}`);
            }
        }

        // this.logger.info(`Для факта ${fact._id} найдено подходящих счетчиков: ${matchedCountersCount} из ${this._counterConfig.length}`);
        // this.logger.info(`facetStages: ${JSON.stringify(facetStages)}`);

        if (matchedCountersCount > 0) {
            return {
                facetStages: facetStages,
                indexTypeNames: Array.from(matchedIndexTypeNames)
            };
        }
        
        return null;
    }

    /**
     * Получает конфигурацию счетчиков для указанного типа факта с кешированием ранее вычисленного результата
     * @param {integer} type - Тип факта
     * @returns {Array<Object>} Массив конфигураций счетчиков для указанного типа факта
     */
    getCounterConfigByType(type, allowedCountersNames) {
        if (!this._counterConfigByType) {
            this._counterConfigByType = {};
        }
        if (!this._counterConfigByType[type]) {
            // Находим счетчики, у которых в computationConditions есть d.MessageTypeId или t (для тестов) и его значение равно type
            this._counterConfigByType[type] = [];
            this._counterConfig.forEach(counter => {
                // Если задан список разрешенных счетчиков и счетчик не в списке, то пропускаем
                if (allowedCountersNames && !allowedCountersNames.includes(counter.name)) {
                    return;
                }
                // Проверяем только одно условие на MessageTypeId
                const messageTypeIdFieldName = this.fieldNameMapper.getFieldName("MessageTypeID");
                const messageTypeIdValue = counter.computationConditions ? (counter.computationConditions["d."+messageTypeIdFieldName] || counter.computationConditions["t"]) : null;
                const condition = {};
                condition["d."+messageTypeIdFieldName] = messageTypeIdValue;
                // Создаем временный факт с указанным типом для проверки условия
                const fact = {
                    "d": {
                        "t": type
                    }
                };
                fact.d[messageTypeIdFieldName] = type;
                if (!messageTypeIdValue || this._conditionEvaluator.matchesCondition(fact, condition)) {
                    this._counterConfigByType[type].push(counter);
                }
            });
            // Сортируем счетчики в порядке возрастания toTimeMs и затем по возрастанию fromTimeMs
            this._counterConfigByType[type].sort((a, b) => {
                if (a.toTimeMs !== b.toTimeMs) {
                    return a.toTimeMs - b.toTimeMs;
                }
                return a.fromTimeMs - b.fromTimeMs;
            });
        }
        return this._counterConfigByType[type];
    }

    /**
     * Получает счетчики по evaluationConditions для указанного типа факта с кешированием ранее вычисленного результата.
     * Используется только для метрики потенциально изменяемых счетчиков для указанного факта.
     * @param {integer} type - Тип факта
     * @returns {Array<Object>} Массив счетчиков для указанного типа факта
     */
    getEvaluationConditionsByType(type, allowedCountersNames) {
        if (!this._EvaluationConditionsByType) {
            this._EvaluationConditionsByType = {};
        }
        if (!this._EvaluationConditionsByType[type]) {
            // Находим счетчики, у которых в evaluationConditions есть d.MessageTypeId или t (для тестов) и его значение равно type
            this._EvaluationConditionsByType[type] = [];
            this._counterConfig.forEach(counter => {
                // Если задан список разрешенных счетчиков и счетчик не в списке, то пропускаем
                if (allowedCountersNames && !allowedCountersNames.includes(counter.name)) {
                    return;
                }
                // Проверяем только одно условие на MessageTypeId
                const messageTypeIdFieldName = this.fieldNameMapper.getFieldName("MessageTypeID");
                const messageTypeIdValue = counter.evaluationConditions ? (counter.evaluationConditions["d."+messageTypeIdFieldName] || counter.evaluationConditions["t"]) : null;
                const condition = {};
                condition["d."+messageTypeIdFieldName] = messageTypeIdValue;
                // Создаем временный факт с указанным типом для проверки условия
                const fact = {
                    "d": {
                        "t": type
                    }
                };
                fact.d[messageTypeIdFieldName] = type;
                if (!messageTypeIdValue || this._conditionEvaluator.matchesCondition(fact, condition)) {
                    this._EvaluationConditionsByType[type].push(counter);
                }
            });
            // Сортируем счетчики в порядке возрастания fromTimeMs и затем по возрастанию toTimeMs
            this._EvaluationConditionsByType[type].sort((a, b) => {
                if (a.fromTimeMs !== b.fromTimeMs) {
                    return a.fromTimeMs - b.fromTimeMs;
                }
                return a.toTimeMs - b.toTimeMs;
            });
        }
        return this._EvaluationConditionsByType[type];
    }

    /**
     * Новый метод. Получает счетчики для факта
     * @param {Object} fact - Факт для обработки
     * @returns {Object|null} Объект с полем factCounters, или null если нет подходящих счетчиков
     * @returns {Object} factCounters - Массив счетчиков для факта
     */
    getFactCounters(fact, allowedCountersNames) {
        if (!fact || !fact.d) {
            this.logger.warn('Передан некорректный факт для получения счетчиков');
            return null;
        }

        const factCounters = [];
        // Проходим по всем счетчикам и проверяем условия
        for (const counter of this.getCounterConfigByType(fact.t, allowedCountersNames)) {
            if (this._conditionEvaluator.matchesCondition(fact, counter.computationConditions)) {
                if (!counter.attributes) {
                    this.logger.warn(`Счетчик '${counter.name}' не имеет атрибутов (attributes). Счетчик не будет добавлен.`);
                    continue;
                }
                factCounters.push(counter);
                this.logger.debug(`Счетчик '${counter.name}' подходит для факта ${fact._id}`);
            } else {
                if (this._debugMode) {
                    this.logger.debug(`Счетчик '${counter.name}' не подходит для факта ${fact._id} по условиям ${JSON.stringify(counter.computationConditions)}`);
                }
            }
        }
        this.logger.debug(`Для факта ${fact._id} найдено подходящих счетчиков: ${factCounters.length} из ${this._counterConfig.length}`);
        // Собираем метрику для evaluationConditions
        let evaluationCountersCount = 0;
        for (const counter of this.getEvaluationConditionsByType(fact.t, allowedCountersNames)) {
            if (this._conditionEvaluator.matchesCondition(fact, counter.evaluationConditions, this._undefinedFieldIsTrue)) {
                if (!counter.attributes) {
                    this.logger.warn(`Счетчик '${counter.name}' не имеет атрибутов (attributes). Счетчик не будет добавлен.`);
                    continue;
                }
                evaluationCountersCount++;
                this.logger.debug(`Счетчик '${counter.name}' будет меняться фактом ${fact._id} по условиям оценки ${JSON.stringify(counter.evaluationConditions)}`);
            } else {
                if (this._debugMode) {
                    this.logger.debug(`Счетчик '${counter.name}' не будет меняться фактом ${fact._id} по условиям оценки ${JSON.stringify(counter.evaluationConditions)}`);
                }
            }
        }
        this.logger.debug(`Факт ${fact._id} влияет на ${evaluationCountersCount} счетчиков из ${this._counterConfig.length}`);

        // this.logger.debug(`facetStages: ${JSON.stringify(factCounters)}`);

        if (!factCounters.length) {
            return null;
        }
        
        return {
            computationConditionsCounters: factCounters,
            evaluationCountersCount: evaluationCountersCount
        };
    }

    /**
     * Получает список всех счетчиков в конфигурации
     * @returns {Array<string>} Массив имен счетчиков
     */
    getCounterNames() {
        return this._counterConfig.map(counter => counter.name);
    }

    /**
     * Получает конфигурацию счетчика по имени
     * @param {string} counterName - Имя счетчика
     * @returns {Object|null} Конфигурация счетчика или null, если не найден
     */
    getCounterDescription(counterName) {
        return this._counterConfig.find(counter => counter.name === counterName) || null;
    }

    /**
     * Получает общее количество счетчиков в конфигурации
     * @returns {number} Количество счетчиков
     */
    getCounterCount() {
        return this._counterConfig.length;
    }
}

module.exports = CounterProducer;
