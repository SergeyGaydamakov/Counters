const EventGenerator = require('../generators/eventGenerator');
const FactIndexer = require('../generators/factIndexer');
const FactMapper = require('../generators/factMapper');
const Logger = require('../utils/logger');

/**
 * Класс-контроллер для управления фактами и их индексными значениями
 * Обеспечивает создание фактов с автоматической генерацией и сохранением индексных значений
 * Работает с абстрактным dbProvider, который должен реализовывать интерфейс для работы с данными
 */
class FactController {
    MAX_DEPTH_LIMIT = 1000;
    MAX_DEPTH_FROM_DATE = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);

    constructor(dbProvider, fieldConfigPathOrMapArray, indexConfigPathOrMapArray, targetSize) {
        if (!dbProvider) {
            throw new Error('dbProvider обязателен для инициализации FactController');
        }
        
        // Создаем логгер для этого контроллера
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        
        // Проверяем, что dbProvider имеет необходимые методы
        const requiredMethods = ['saveFact', 'saveFactIndexList', 'getRelevantFacts', 'getRelevantFactCounters'];
        for (const method of requiredMethods) {
            if (typeof dbProvider[method] !== 'function') {
                throw new Error(`dbProvider должен иметь метод '${method}'`);
            }
        }
        
        this.dbProvider = dbProvider;
        this.eventGenerator = new EventGenerator(fieldConfigPathOrMapArray, targetSize);
        this.factIndexer = new FactIndexer(indexConfigPathOrMapArray);
        this.factMapper = new FactMapper(fieldConfigPathOrMapArray);
    }

    /**
     * Запускает обработку фактов:
     * 1. Генерация нового случайного факта
     * 2. Получение из базы данных релевантных фактов для вычисления счетчиков
     * 3. Сохранение нового факта и индексных значений в базу данных
     * 
     */
    async run() {
        // Генерация нового случайного события
        const event = this.eventGenerator.generateRandomTypeEvent();
        // Обработка события
        return this.processEvent(event);
    }

    /**
     * Запускает обработку фактов:
     * 1. Генерация нового случайного факта
     * 2. Получение из базы данных счетчиков релевантных фактов
     * 3. Сохранение нового факта и индексных значений в базу данных
     * 
     */
    async runWithCounters() {
        // Генерация нового случайного события
        const event = this.eventGenerator.generateRandomTypeEvent();
        // Обработка события
        return this.processEventWithCounters(event);
    }

    /**
     * Обрабатывает событие: получает релевантные факты, сохраняет факт и индексные значения в базу данных
     * @param {Object} event - событие
     * @returns {Promise<Object>} результат операции создания факта
     */
    async processEvent(event) {
        const fact = this.factMapper.mapEventToFact(event);
        this.logger.debug(`*** Для события ${event.t} будет создан новый факт ${fact.t}: ${fact._id}`);
        const factIndexes = this.factIndexer.index(fact);
        const factIndexHashValues = factIndexes.map(index => index.h);
        const [relevantFacts, factResult, indexResult] = await Promise.all([
            this.dbProvider.getRelevantFacts(factIndexHashValues, fact._id, this.MAX_DEPTH_LIMIT, this.MAX_DEPTH_FROM_DATE),
            this.dbProvider.saveFact(fact),
            this.dbProvider.saveFactIndexList(factIndexes)
        ]);
        return {
            fact,
            relevantFacts,
            factResult,
            indexResult
        };
    }
    
    /**
     * Обрабатывает событие: получает релевантные факты, сохраняет факт и индексные значения в базу данных
     * @param {Object} event - событие
     * @returns {Promise<Object>} результат операции создания факта
     */
    async processEventWithCounters(event) {
        const fact = this.factMapper.mapEventToFact(event);
        this.logger.debug(`*** Для события ${event.t} будет создан новый факт ${fact.t}: ${fact._id}`);
        const factIndexes = this.factIndexer.index(fact);
        const factIndexHashValues = factIndexes.map(index => index.h);
        const [factCounters, factResult, indexResult] = await Promise.all([
            this.dbProvider.getRelevantFactCounters(factIndexHashValues, fact._id, this.MAX_DEPTH_LIMIT, this.MAX_DEPTH_FROM_DATE),
            this.dbProvider.saveFact(fact),
            this.dbProvider.saveFactIndexList(factIndexes)
        ]);
        return {
            fact,
            factCounters,
            factResult,
            indexResult
        };
    }

}

module.exports = FactController;
