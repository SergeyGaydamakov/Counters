const MessageGenerator = require('../generators/messageGenerator');
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
    MAX_DEPTH_FROM_DATE = new Date(Date.now() - 300 * 24 * 60 * 60 * 1000);

    constructor(dbProvider, fieldConfigPathOrObject, indexConfigPathOrObject, targetSize) {
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
        this.messageGenerator = new MessageGenerator(fieldConfigPathOrObject, targetSize);
        this.factIndexer = new FactIndexer(indexConfigPathOrObject);
        this.factMapper = new FactMapper(fieldConfigPathOrObject);

        // Значения хеша
        this.factIndexer._indexConfig.forEach(config => {
            this.logger.info(`* Значение хеша для значения 1234567890 в индексе ${config.indexType} -> ${this.factIndexer._hashBase64(config.indexType, '1234567890')}`);
        });
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
        const message = this.messageGenerator.generateRandomTypeMessage();
        // Обработка события
        return this.processMessage(message);
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
        const message = this.messageGenerator.generateRandomTypeMessage();
        // Обработка события
        return this.processMessageWithCounters(message);
    }

    /**
     * Обрабатывает сообщение: получает релевантные факты, сохраняет факт и индексные значения в базу данных
     * @param {Object} message - сообщение
     * @returns {Promise<Object>} результат операции создания факта
     */
    async processMessage(message) {
        const fact = this.factMapper.mapMessageToFact(message);
        const factIndexes = this.factIndexer.index(fact);
        if (factIndexes.length === 0) {
            this.logger.warn(`✓ Нет индексных значений для факта с типом ${fact.t}, обработка факта будет пропущена.`);
            // Чтобы не портить статистику, для processingTime устанавливаем null
            return {
                fact,
                relevantFacts: [],
                saveFactResult: { success: true },
                saveIndexResult: { success: true },
                processingTime: null
            };
        }
        const hashValuesForSearch = this.factIndexer.getHashValuesForSearch(factIndexes);
        const startTime = Date.now();
        const [relevantFactsResult, factResult, indexResult] = await Promise.all([
            this.dbProvider.getRelevantFacts(hashValuesForSearch, fact, this.MAX_DEPTH_LIMIT, this.MAX_DEPTH_FROM_DATE),
            this.dbProvider.saveFact(fact),
            this.dbProvider.saveFactIndexList(factIndexes)
        ]);
        return {
            fact,
            relevantFacts: relevantFactsResult?.result,
            saveFactResult: factResult?.result,
            saveIndexResult: indexResult,
            processingTime: {
                total: Date.now() - startTime,
                relevantFacts: relevantFactsResult.processingTime,
                saveFact: factResult.processingTime,
                saveIndex: indexResult.processingTime,
            },
            debug: relevantFactsResult?.debug
        };
    }

    /**
     * Обрабатывает сообщение: получает релевантные факты, сохраняет факт и индексные значения в базу данных
     * @param {Object} message - сообщение
     * @returns {Promise<Object>} результат операции создания факта
     */
    async processMessageWithCounters(message, debugMode) {
        const fact = this.factMapper.mapMessageToFact(message);
        const factIndexes = this.factIndexer.index(fact);
        if (factIndexes.length === 0) {
            this.logger.warn(`✓ Нет индексных значений для факта с типом ${fact.t}, обработка факта будет пропущена.`);
            // Чтобы не портить статистику, для processingTime устанавливаем null
            return {
                fact,
                relevantFacts: [],
                saveFactResult: { success: true },
                saveIndexResult: { success: true },
                processingTime: null,
                debug: {
                }
            };
        }
        const hashValuesForSearch = this.factIndexer.getHashValuesForSearch(factIndexes);
        const startTime = Date.now();
        const [factCountersResult, factResult, indexResult] = await Promise.all([
            this.dbProvider.getRelevantFactCounters(hashValuesForSearch, fact, this.MAX_DEPTH_LIMIT, this.MAX_DEPTH_FROM_DATE, debugMode),
            this.dbProvider.saveFact(fact),
            this.dbProvider.saveFactIndexList(factIndexes)
        ]);
        return {
            fact,
            counters: factCountersResult.result,
            saveFactResult: factResult.result,
            saveIndexResult: indexResult,
            processingTime: {
                total: Date.now() - startTime,
                counters: factCountersResult.processingTime,
                saveFact: factResult.processingTime,
                saveIndex: indexResult.processingTime,
            },
            metrics: factCountersResult?.metrics,
            debug: factCountersResult?.debug
        };
    }
}

module.exports = FactController;
