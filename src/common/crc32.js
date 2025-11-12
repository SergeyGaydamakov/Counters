/**
 * Вычисляет CRC32 хеш для строки
 * @param {string} str - строка для хеширования
 * @returns {number} CRC32 хеш
 */
function crc32(str) {
    const buffer = Buffer.from(str, 'utf8');
    let crc = 0xFFFFFFFF;
    
    for (let i = 0; i < buffer.length; i++) {
        crc ^= buffer[i];
        for (let j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >>> 1) ^ 0xEDB88320;
            } else {
                crc = crc >>> 1;
            }
        }
    }
    
    return (crc ^ 0xFFFFFFFF) >>> 0;
}

module.exports = { crc32 };

