const cluster = require('cluster');
const os = require('os');
const Logger = require('../utils/logger');

// Загружаем переменные окружения
const dotenv = require('dotenv');
dotenv.config();

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Количество CPU ядер для создания воркеров
const numCPUs = process.env.CLUSTER_WORKERS || os.cpus().length;

if (cluster.isMaster) {
    logger.info(`🚀 Master процесс ${process.pid} запущен`);
    logger.info(`⚙️  Создаю ${numCPUs} воркеров для обработки запросов`);

    // Создаем воркеры
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        logger.info(`👷 Воркер ${worker.process.pid} создан`);
    }

    // Обработка завершения воркеров
    cluster.on('exit', (worker, code, signal) => {
        if (signal) {
            logger.warn(`⚠️  Воркер ${worker.process.pid} завершен сигналом ${signal}`);
        } else if (code !== 0) {
            logger.error(`❌ Воркер ${worker.process.pid} завершен с кодом ${code}`);
        } else {
            logger.info(`✅ Воркер ${worker.process.pid} завершен успешно`);
        }

        // Перезапускаем воркер если он завершился неожиданно
        if (!worker.exitedAfterDisconnect) {
            logger.info(`🔄 Перезапускаю воркер...`);
            const newWorker = cluster.fork();
            logger.info(`👷 Новый воркер ${newWorker.process.pid} создан`);
        }
    });

    // Обработка сигналов завершения
    process.on('SIGTERM', () => {
        logger.info('📡 Получен SIGTERM, завершаю все воркеры...');
        for (const id in cluster.workers) {
            cluster.workers[id].kill();
        }
    });

    process.on('SIGINT', () => {
        logger.info('📡 Получен SIGINT, завершаю все воркеры...');
        for (const id in cluster.workers) {
            cluster.workers[id].kill();
        }
    });

    // Мониторинг производительности
    setInterval(() => {
        const workers = Object.keys(cluster.workers).length;
        logger.info(`📊 Активных воркеров: ${workers}`);
    }, 30000); // Каждые 30 секунд

} else {
    // Запускаем воркер
    // Каждый Worker работает в отдельном процессе Node.js
    // и создает свои собственные экземпляры MongoProvider, FactController и CounterProducer
    require('./worker.js');
}
