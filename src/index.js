// Основные экспорты проекта
const EventGenerator = require('./generators/eventGenerator');
const FactIndexer = require('./generators/factIndexer');
const FactMapper = require('./generators/factMapper');
const MongoProvider = require('./db-providers/mongoProvider');
const FactController = require('./controllers/factController');

module.exports = {
    EventGenerator,
    FactIndexer,
    FactMapper,
    MongoProvider,
    FactController
};

