/**
 * IPC Serializer - Утилита для сериализации/десериализации IPC сообщений
 * Поддерживает JSON (по умолчанию) и MessagePack (опционально)
 */

const config = require('./config');

// Ленивая загрузка msgpackr только если включен
let msgpackr = null;
let useMessagePack = false;

/**
 * Инициализация MessagePack (ленивая загрузка)
 */
function initMessagePack() {
    if (useMessagePack && !msgpackr) {
        try {
            const msgpackrModule = require('msgpackr');
            // Настройка msgpackr для оптимальной производительности
            // msgpackr автоматически обрабатывает Date объекты (сериализует как timestamp и десериализует обратно в Date)
            // Используем Packr для сериализации/десериализации с оптимизациями
            msgpackr = new msgpackrModule.Packr({
                useTimestamp32: true,  // Используем 32-битный timestamp для Date (экономия места)
                mapsAsObjects: true,    // Сохраняем объекты как объекты, не как Map
                variableMapSize: true   // Оптимизация размера для объектов
            });
        } catch (error) {
            console.warn('IPC Serializer: Не удалось загрузить msgpackr, используется JSON:', error.message);
            useMessagePack = false;
        }
    }
}

/**
 * Проверяет, включен ли MessagePack
 */
function isMessagePackEnabled() {
    return useMessagePack;
}

/**
 * Включает/выключает использование MessagePack
 * @param {boolean} enabled - true для включения MessagePack, false для JSON
 */
function setMessagePackEnabled(enabled) {
    useMessagePack = enabled === true;
    if (useMessagePack) {
        initMessagePack();
    }
}

// Инициализация из конфигурации
const IPC_USE_MSGPACK = process.env.IPC_USE_MSGPACK === 'true' || config.ipc?.useMessagePack === true;
setMessagePackEnabled(IPC_USE_MSGPACK);

/**
 * Сериализация сообщения для IPC
 * @param {*} message - Сообщение для сериализации
 * @returns {Buffer|string} - Сериализованное сообщение (Buffer для MessagePack, string для JSON)
 */
function serialize(message) {
    if (useMessagePack) {
        initMessagePack();
        if (msgpackr) {
            try {
                return msgpackr.pack(message);
            } catch (error) {
                console.warn('IPC Serializer: Ошибка сериализации MessagePack, fallback на JSON:', error.message);
                // Fallback на JSON при ошибке
                return JSON.stringify(message);
            }
        }
    }
    // JSON по умолчанию
    return JSON.stringify(message);
}

/**
 * Десериализация сообщения из IPC
 * @param {Buffer|string} data - Сериализованное сообщение
 * @returns {*} - Десериализованное сообщение
 */
function deserialize(data) {
    if (useMessagePack) {
        initMessagePack();
        if (msgpackr && Buffer.isBuffer(data)) {
            try {
                return msgpackr.unpack(data);
            } catch (error) {
                console.warn('IPC Serializer: Ошибка десериализации MessagePack, fallback на JSON:', error.message);
                // Fallback на JSON при ошибке
                return JSON.parse(data.toString());
            }
        }
    }
    // JSON по умолчанию
    if (Buffer.isBuffer(data)) {
        return JSON.parse(data.toString());
    }
    return JSON.parse(data);
}

/**
 * Отправка сообщения через IPC с автоматической сериализацией
 * @param {Object} process - Child process или parent process
 * @param {*} message - Сообщение для отправки
 */
function send(process, message) {
    if (!process || !process.send) {
        throw new Error('IPC Serializer: process.send не доступен');
    }
    
    const serialized = serialize(message);
    
    // process.send() в Node.js использует JSON.stringify() по умолчанию
    // Для MessagePack нужно обернуть Buffer в объект с маркером
    if (useMessagePack && Buffer.isBuffer(serialized)) {
        // Отправляем объект с маркером MessagePack и данными в base64
        process.send({
            __msgpack: true,
            __data: serialized.toString('base64')
        });
    } else {
        // Для JSON отправляем объект напрямую (process.send() автоматически сериализует через JSON.stringify)
        // Это обеспечивает обратную совместимость
        const jsonData = typeof serialized === 'string' ? JSON.parse(serialized) : JSON.parse(serialized.toString());
        process.send(jsonData);
    }
}

/**
 * Обработка входящего сообщения с автоматической десериализацией
 * @param {Buffer|string|Object} data - Входящие данные
 * @returns {*} - Десериализованное сообщение
 */
function receive(data) {
    // Если это объект с маркером MessagePack
    if (data && typeof data === 'object' && data.__msgpack === true && typeof data.__data === 'string') {
        const buffer = Buffer.from(data.__data, 'base64');
        return deserialize(buffer);
    }
    
    // Обратная совместимость: если это обычный объект (JSON формат)
    // process.send() в Node.js автоматически десериализует JSON объекты
    if (data && typeof data === 'object' && !Buffer.isBuffer(data) && !data.__msgpack) {
        // Это уже десериализованный объект (JSON формат от process.send())
        return data;
    }
    
    // Обычная десериализация для строк или Buffer (fallback)
    return deserialize(data);
}

module.exports = {
    serialize,
    deserialize,
    send,
    receive,
    isMessagePackEnabled,
    setMessagePackEnabled
};

