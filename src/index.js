// Основные экспорты проекта
const MessageGenerator = require('./domain/messageGenerator');
const FactIndexer = require('./domain/factIndexer');
const FactMapper = require('./domain/factMapper');
const CounterProducer = require('./domain/counterProducer');
const MongoProvider = require('./database/mongoProvider');
const FactService = require('./services/factService');

module.exports = {
    MessageGenerator,
    FactIndexer,
    FactMapper,
    CounterProducer,
    MongoProvider,
    FactService,
    // Обратная совместимость
    FactController: FactService
};
