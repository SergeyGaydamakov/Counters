// Основные экспорты проекта
const FactGenerator = require('./generators/factGenerator');
const FactIndexer = require('./generators/factIndexer');
const MongoProvider = require('./db-providers/mongoProvider');
const FactController = require('./controllers/factController');

module.exports = {
    FactGenerator,
    FactIndexer,
    MongoProvider,
    FactController
};

