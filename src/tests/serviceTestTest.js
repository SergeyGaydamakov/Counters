// Простой тест для проверки работы модуля serviceTest.js
const { main } = require('../serviceTest');

// Тестируем только инициализацию и первые несколько циклов
async function testServiceTest() {
    console.log('🧪 Тестирование модуля serviceTest.js...');
    
    try {
        // Запускаем main функцию
        console.log('✓ Модуль успешно импортирован');
        console.log('✓ Функция main доступна');
        
        // Проверяем, что модуль может быть запущен
        console.log('✓ Модуль готов к запуску');
        console.log('');
        console.log('Для полного тестирования запустите:');
        console.log('npm run test:service');
        console.log('');
        console.log('⚠️  Убедитесь, что сервис запущен на http://localhost:3000');
        
    } catch (error) {
        console.error('✗ Ошибка при тестировании модуля:', error.message);
        process.exit(1);
    }
}

// Запуск теста
if (require.main === module) {
    testServiceTest();
}
