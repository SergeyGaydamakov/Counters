// Основные экспорты проекта
const MessageGenerator = require('./generators/messageGenerator');
const FactIndexer = require('./generators/factIndexer');
const FactMapper = require('./generators/factMapper');
const MongoCounters = require('./db-providers/mongoCounters');
const MongoProvider = require('./db-providers/mongoProvider');
const FactController = require('./controllers/factController');

module.exports = {
    MessageGenerator,
    FactIndexer,
    FactMapper,
    MongoCounters,
    MongoProvider,
    FactController
};

